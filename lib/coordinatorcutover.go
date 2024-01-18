package lib

import (
	"errors"

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

// compare the two struct. 0 if same, 1 if activetwotask differs, 2 if dbuname differs, 4 if phase differs, 8 if RWStatus differs.
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

// construct a new cutoverInfo from CutoverCfg.
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
PreprocessCutover is to detect change that coordinator needs to change for the next dispatch
1. active db change
2. rw status change
3. cutover phase change
*/
func (crd *Coordinator) PreprocessCutover(requests []*netstring.Netstring) (bool, error) {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, crd.id, "PreprocessCutover:", crd.shard)
	}

	curCOCfg := GetCutoverCfg()
	newCOInfo := newCutoverInfo(curCOCfg)
	diff := compActCOInfo(*crd.curCoInfo, newCOInfo)

	if crd.inTransaction {
		// if crd is in transaction we should hang up if read/write stops or active db has changed.
		//		0 if same, 1 if activetwotask differs, 2 if dbuname differs, 4 if phase differs, 8 if RWStatus differs.

		if diff != 0 {

			// check if it's single change
			switch diff {
			case 0x0001:
				// diff two_task changes where to dispatch next. We need to terminate the intransaction.
			case 0x0002:
				// diff dbuname, integrity is handled by workerpool
			case 0x0004:
				// diff phase
				// preprocess upon phase change, what does this mean?
			case 0x0008:
				// diff rwstatus
				// if this has a stop to read or write, we will take action by stopping the intransaction
				// rw status, 1 R, 2 W, 3 RW, 0 NRNW
				if crd.curCoInfo.RWstatus > newCOInfo.RWstatus {
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
			copyActCOInfo(crd.curCoInfo, newCOInfo)
		}
		// TODO: why is this needed
		crd.prevShard.sessionShardID = crd.shard.sessionShardID
		return false, nil // allow to proceed
	}
	return false, nil
}
