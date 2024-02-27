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
// If no shard is active, the ActiveInfo should container empty strings.
// Each coordinator will pull the CutoverCfg to check if there is any update.
// The information is stored in a structure ActiveInfo as for which shard and RW, R, or W
//
// Or, should we let the cutoverCfg to "push" the change to the coordinator? is it possible and better? where does the coordinator is tracked?
type ActiveInfo struct {
	ActShId    ShardByTwoTask // active pool shard id
	Aphase     string         // current cutover phase
	AdbUname   string         // Active DB_UNAME
	Arwstatus  int            // dbuname --> rw status, 1 R, 2 W, 3 RW, 0 NRNW
	SessionCfg CutoverCfg     // tracking the cfg used in the session
}

func copyActCOInfo(destInfo *ActiveInfo, srcInfo ActiveInfo) {
	destInfo.ActShId = srcInfo.ActShId
	destInfo.Aphase = srcInfo.Aphase
	destInfo.Arwstatus = srcInfo.Arwstatus
	destInfo.AdbUname = srcInfo.AdbUname
}

// Compare existing ActiveCOInfo with new cfg.
// (00000) identical
// (00001) 1 if twotask changes (shard id changes)
// (00010) 2 if dbuname changes
// (00100) 4 if phase changes (may force shard id )
// (01000) 8 if RWStatus changes
func compActiveInfo(cur ActiveInfo, new ActiveInfo) int {
	flag := 0
	if cur.ActShId != new.ActShId {
		flag |= 0x0001
	}
	if cur.AdbUname != new.AdbUname {
		flag |= 0x0002
	}
	if cur.Aphase != new.Aphase {
		flag |= 0x0004
	}
	if cur.Arwstatus != new.Arwstatus {
		flag |= 0x0008
	}
	return flag
}

// Build the ActiveCOInfo from CutoverCfg.
func cvtActiveInfo(cocfg *CutoverCfg) *ActiveInfo {
	if cocfg == nil {
		return nil
	}

	newActInfo := ActiveInfo{
		ActShId:   cocfg.ActiveShardId,
		AdbUname:  cocfg.DbBy2task[cocfg.ActiveTwoTask],
		Aphase:    cocfg.Phase,
		Arwstatus: cocfg.RWstatusByDb[cocfg.DbBy2task[cocfg.ActiveTwoTask]],
	}
	logger.GetLogger().Log(logger.Alert, "shtien newActInfo (ActShId, AdbUname, Aphase, Arwstatus)=(", newActInfo.ActShId, newActInfo.AdbUname, newActInfo.Aphase, newActInfo.Arwstatus,")")
	return &newActInfo
}

/*
Every sqlrequest goes through PreprocessCutover. The function loads the latest cfg and detect which pool shard it should go
and disconnect the client if needed.
// (00000) identical
// (00001) 1 if (active) twotask changes (shard id changes), terminate ongoing txn
// (00010) 2 if dbuname changes (doesn't affect shard id)
// (00100) 4 if phase changes (may force shard id )
// (01000) 8 if RWStatus changes. any type (of R or W) is stopped, terminate ongoing txn

info required.
a. which workerpool (sh + type) to dispatch such request
b. some phase has default behavior and different policy
c. update the ActiveInfo struct
d. return bool -> hang up or not, int -> shard id

hang up conditions
1. active two_task has changed from last tracked active info in this coordinator
2. active two_task is unchanged but RWstatus disabled from enabled.
*/
func (crd *Coordinator) PreprocessCutover(requests []*netstring.Netstring) (bool, error) {

	//curActInfo     *ActiveInfo    // maybe we don't need this, just use the CutoverInfo (atomic) directly
	//curCOCfg
	if crd.curActInfo == nil && GetCutoverCfg() == nil {
		logger.GetLogger().Log(logger.Alert, crd.id, "shtien PreprocessCutover at init")
	}
	interrupt := false // if txn should be disrupted
	newActInfo := cvtActiveInfo(GetCutoverCfg())
	if newActInfo == nil {
		logger.GetLogger().Log(logger.Alert, crd.id, "shtien PreprocessCutover no new activeInfo, treat as no diff")
		return interrupt, nil
	}

	if crd.curActInfo == nil {
		// TODO. are we doing the right thing? need to check if we have missed some init case.
		logger.GetLogger().Log(logger.Alert, "crd.curActInfo is nil")
		crd.curActInfo = newActInfo
	}
	diff := compActiveInfo(*crd.curActInfo, *newActInfo)
	if diff == 0 {
		logger.GetLogger().Log(logger.Alert, "crd.curActInfo and newActInfo is the same")
		return interrupt, nil // same cutover config
	}
	var err error
	if crd.inTransaction {
		// worker could be in a long txn, break it if needed.
		if (diff & 0x0001) == 0x0001 {
			if newActInfo.Aphase == PrePhStr || newActInfo.Aphase == EnablePhStr {
				if newActInfo.ActShId != ShId2Task {
					logger.GetLogger().Log(logger.Alert, crd.id, "logging only. prior to cutover phase config using ShId2TaskCutover!")
				}
			} else if newActInfo.Aphase == CompletePhStr || newActInfo.Aphase == BroomPhStr {
				if newActInfo.ActShId != ShId2TaskCutover {
					logger.GetLogger().Log(logger.Alert, crd.id, "logging only. post cutover phase config using ShId2Task!")
				}
			} else if newActInfo.Aphase == CutoverPhStr {
				//cutover phase, swithc worker pool
				logger.GetLogger().Log(logger.Alert, crd.id, "cutover phase worker pool switch")
				interrupt = true
				err = errors.New("cutover database switch")
			} else {
				// shouldn't reach here
				logger.GetLogger().Log(logger.Alert, crd.id, "logging only. Unrecognized new cutover phase with occ_two_task change")
			}
		}
		if (diff & 0x0002) == 0x0002 {
			// integrity is handled by workerpool in a separate way, is there anything we should do here as dispatching?
			logger.GetLogger().Log(logger.Alert, crd.id, "logging only. active dbuname change")
		}
		if (diff & 0x0004) == 0x0004 {
			// preprocess upon phase change, what does this mean?
			logger.GetLogger().Log(logger.Alert, crd.id, "logging only. cutover phase change")
		}
		if (diff & 0x0008) == 0x0008 {
			// if this has a stop to read or write, we will take action by stopping the intransaction
			// rw status, 1 R, 2 W, 3 RW, 0 NRNW
			if newActInfo.Aphase == CutoverPhStr {
				if crd.curActInfo.Arwstatus > newActInfo.Arwstatus { // either W or R or both RW are newly disabled.
					if newActInfo.Arwstatus == 0 {
						interrupt = true //we will check this later
						err = errors.New("cutover stop in-txn")
					}
					if newActInfo.Arwstatus&0x0001 == 0 { // read is disabled now
						// stop READ
						if crd.isRead {
							interrupt = true // we will check this later
							err = errors.New("cutover stop in-txn read")
						}
					}
					if newActInfo.Arwstatus&0x0002 == 0 { // write is disabled now
						// stop WRTIE
						if !crd.isRead {
							interrupt = true // we will check this later
							err = errors.New("cutover top in-txn write")
						}
					}
				}
			}
		}
		copyActCOInfo(crd.curActInfo, *newActInfo)      // now coordinator has updated with latest info
		crd.shard.shardID = int(crd.curActInfo.ActShId) // we will need shardID in dispatchRequest(). hm..
		return interrupt, err
	} else {
		// worker is not in transactions
		// we will just load the new cfg and update the crd flags as needed.
		copyActCOInfo(crd.curActInfo, *newActInfo)
		crd.shard.shardID = int(crd.curActInfo.ActShId)
		return false, nil // allow to proceed
	}
}

// only for internal write queries. When read cfg always use two_task shard, write uses two_task shard and cutover shard
func (crd *Coordinator) processSetCoShardID(val []byte) error {
	if !GetConfig().EnableCutover { // no need to pass
		crd.coInternalShId = ShIdUnset
		return nil
	}
	if !crd.isInternal { // not allow external connections
		return ErrNotInternal
	}

	sh, err := strconv.ParseInt(string(val), 10, 32)
	if err != nil {
		return nil
	}
	// cutover enabled. we expect sh to be 0 (two_task) or 1 (two_task_cutover)
	if sh != 0 && sh != 1 {
		return ErrBadShardID
	}

	crd.coInternalShId = ShardByTwoTask(sh)
	if crd.inTransaction && (crd.worker != nil) {
		// in transaction, piggy back on the shard variable
		if int(crd.coInternalShId) != crd.worker.shardID {
			evt := cal.NewCalEvent(EvtTypeCutover, "internal query change pool", cal.TransOK, "")
			evt.AddDataInt("cur_shard_id", int64(crd.worker.shardID))
			evt.AddDataStr("requested_shard_id", string(val))
			evt.Completed()
			// processSetCoShardID has higher priority, switch worker.
		}
	}
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, crd.id, "Shard ID forced to", crd.shard.shardID)
	}
	return nil
}
