package lib

import (
	"errors"
	"strconv"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
)

// In cutovercfg.go I have defined this, we don't need ActiveCOInfo
/*
type CutoverCfg struct {
	ActiveTwoTask     string            // FOO or FOO_CUTOVER is the active
	Phase             string            // current cutover phase
	DbUnameBy2task    map[string]string // DB_UNAME by two_task and two_task_cutover
	RWstatusByDbUname map[string]int    //dbuname --> rw status, 1 R, 2 W, 3 RW, 0 NRNW
}
*/

// Active shard means the shard either take R, W, or RW sql.
// If no shard is active, the ActiveCOInfo should container empty strings.
// Each coordinator will pull the CutoverCfg to check if there is any update.
// The information is stored in a structure ActiveCOInfo as for which shard and RW, R, or W
//
// Or, should we let the cutoverCfg to "push" the change to the coordinator? is it possible and better? where does the coordinator is tracked?
type ActiveCOInfo struct {
	TwoTask  string // FOO or FOO_CUTOVER is the active
	Phase    string // current cutover phase
	DbUname  string // Active DB_UNAME
	RWstatus int    //dbuname --> rw status, 1 R, 2 W, 3 RW, 0 NRNW
}

func copyActCOInfo(destInfo *ActiveCOInfo, srcInfo ActiveCOInfo) {
	destInfo.TwoTask = srcInfo.TwoTask
	destInfo.Phase = srcInfo.Phase
	destInfo.RWstatus = srcInfo.RWstatus
	destInfo.DbUname = srcInfo.DbUname
}

// Compare existing ActiveCOInfo with new cfg.
// (00000) identical
// (00001) 1 if twotask changes (shard id changes)
// (00010) 2 if dbuname changes
// (00100) 4 if phase changes (may force shard id )
// (01000) 8 if RWStatus changes
func compActCOInfo(cur ActiveCOInfo, new ActiveCOInfo) int {
	flag := 0
	if cur.TwoTask != new.TwoTask {
		flag |= 0x0001
	}
	if cur.DbUname != new.DbUname {
		flag |= 0x0002
	}
	if cur.Phase != new.Phase {
		flag |= 0x0004
	}
	if cur.RWstatus != new.RWstatus {
		flag |= 0x0008
	}
	return flag
}

// Build the ActiveCOInfo from CutoverCfg.
func newCutoverInfo(cocfg *CutoverCfg) ActiveCOInfo {
	newcoinfo := ActiveCOInfo{
		TwoTask:  cocfg.ActiveTwoTask,
		DbUname:  cocfg.DbBy2task[cocfg.ActiveTwoTask],
		Phase:    cocfg.Phase,
		RWstatus: cocfg.RWstatusByDb[cocfg.ActiveTwoTask],
	}
	return newcoinfo
}

/*
PreprocessCutover is to detect the change that will require breaking existing transaction
// (00000) identical
// (00001) 1 if (active) twotask changes (shard id changes), terminate ongoing txn
// (00010) 2 if dbuname changes (doesn't affect shard id)
// (00100) 4 if phase changes (may force shard id )
// (01000) 8 if RWStatus changes. any type (of R or W) is stopped, terminate ongoing txn

coordinator cares about
a. which workerpool to dispatch such request
b. some phase has default behavior and different policy
*/
func (crd *Coordinator) PreprocessCutover(requests []*netstring.Netstring) (bool, error) {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, crd.id, "PreprocessCutover:", crd.shard)
	}

	curCOCfg := GetCutoverCfg()
	newCOInfo := newCutoverInfo(curCOCfg)
	diff := compActCOInfo(*crd.curCOInfo, newCOInfo)

	if crd.inTransaction {
		if diff != 0 {

			if (diff & 0x0004) > 0 { // phase change
				if newCOInfo.Phase == EnabledPh || newCOInfo.Phase == PrePh {
					// we will always use TwoTask shard for dispatch
					crd.curCOInfo.TwoTask = twoTaskName
				} else if newCOInfo.Phase == BroomPh {
					crd.curCOInfo.TwoTask = twoTaskCutoverName
				} else { // other case we follow general rules
					crd.curCOInfo.TwoTask = newCOInfo.TwoTask
				}
			}

			// following is single changes
			switch diff {
			case 0x0001:
				//  need to terminate the intransaction if any
			case 0x0002:
				// diff dbuname, integrity is handled by workerpool in a separate way, is there anything we can do here?
			case 0x0004:
				// diff phase
				// preprocess upon phase change, what does this mean?
			case 0x0008:
				// diff rwstatus
				// if this has a stop to read or write, we will take action by stopping the intransaction
				// rw status, 1 R, 2 W, 3 RW, 0 NRNW
				if crd.curCOInfo.RWstatus > newCOInfo.RWstatus {
					if newCOInfo.RWstatus&0x0001 == 0 {
						// stop READ
						if crd.isRead {
							return true, errors.New("Stop in txn read")
						}
					}
					if newCOInfo.RWstatus&0x0002 == 0 {
						// stop WRTIE
						if !crd.isRead {
							return true, errors.New("Stop in txn write")
						}
					}
				}
			}
		}
	} else {

		// not in transactions.
		// we will just load the new cfg and update the crd flags as needed.
		if diff != 0 {
			copyActCOInfo(crd.curCOInfo, newCOInfo)
		}
		// TODO: why is this needed
		crd.prevShard.sessionShardID = crd.shard.sessionShardID
		return false, nil // allow to proceed
	}
	return false, nil
}

// This is only for internal queries. read cfg always use two_task, write uses both
func (crd *Coordinator) processSetCoShardID(val []byte) error {
	sh, err := strconv.ParseInt(string(val), 10, 32)
	if err != nil {
		return nil
	}
	if !GetConfig().EnableCutover { // no need to pass
		crd.coInternalPool = ShIdUnset
		return nil
	}

	// cutover enabled. we expect sh to be 0 (two_task) or 1 (two_task_cutover)
	if sh != 0 && sh != 1 {
		return ErrBadShardID
	}

	//crd.shard.sessionShardID = int(sh)
	if !crd.isInternal {
		return ErrNotInternal
	}

	crd.coInternalPool = ShardByTwoTask(sh)
	if crd.inTransaction && (crd.worker != nil) {
		// in transaction, piggy back on the shard variable
		if crd.coInternalPool != crd.workerpool.p2task {
			evt := cal.NewCalEvent(EvtTypeCutover, "internal query change pool", cal.TransOK, "")
			evt.AddDataInt("cur_shard_id", int64(crd.worker.shardID))
			evt.AddDataStr("requested_shard_id", string(val))
			evt.Completed()
			return ErrChangeShardIDInTxn
		}
	}
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, crd.id, "Shard ID forced to", crd.shard.shardID)
	}
	return nil
}
