package lib

import (
	"errors"

	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
)

// Active shard means the shard either take R, W, or RW sql.
// If no shard is active, the ActiveDbInfo should container empty strings.
// Each coordinator will pull the CutoverCfg to check if there is any update.
// The information is stored in a structure ActiveDbInfo as for which shard and RW, R, or W
//
// Or, should we let the cutoverCfg to "push" the change to the coordinator? is it possible and better? where does the coordinator is tracked?
type ActiveDbInfo struct {
	ShId       ShardByTwoTask // active pool shard id
	Phase      string         // current cutover phase
	DbUname    string         // Active DB_UNAME
	RwStatus   int            // dbuname --> rw status, 1 R, 2 W, 3 RW, 0 NRNW
	SessionCfg CutoverCfg     // tracking the cfg used in the session
}

func copyActCOInfo(destInfo *ActiveDbInfo, srcInfo ActiveDbInfo) {
	destInfo.ShId = srcInfo.ShId
	destInfo.Phase = srcInfo.Phase
	destInfo.RwStatus = srcInfo.RwStatus
	destInfo.DbUname = srcInfo.DbUname
}

// Compare existing ActiveCOInfo with new cfg.
// (00000) identical
// (00001) 1 if twotask changes (shard id changes)
// (00010) 2 if dbuname changes
// (00100) 4 if phase changes (may force shard id )
// (01000) 8 if RWStatus changes
func compActiveInfo(cur ActiveDbInfo, new ActiveDbInfo) int {
	flag := 0
	if cur.ShId != new.ShId {
		flag |= 0x0001
	}
	if cur.DbUname != new.DbUname {
		flag |= 0x0002
	}
	if cur.Phase != new.Phase {
		flag |= 0x0004
	}
	if cur.RwStatus != new.RwStatus {
		flag |= 0x0008
	}
	return flag
}

// Build the ActiveCOInfo from CutoverCfg.
func cvtActiveInfo(cocfg *CutoverCfg) *ActiveDbInfo {
	if cocfg == nil {
		return nil
	}
	var newActInfo ActiveDbInfo
	if cocfg.Phase == EnablePhStr || cocfg.Phase == PrePhStr {
		newActInfo.ShId = ShId2Task
		newActInfo.DbUname = cocfg.DbBy2task[g2TaskName]
		newActInfo.Phase = cocfg.Phase
		newActInfo.RwStatus = (ReadOk | WriteOk)
	} else if cocfg.Phase == CompletePhStr {
		newActInfo.ShId = ShId2TaskCutover
		newActInfo.DbUname = cocfg.DbBy2task[g2TaskCutoverName]
		newActInfo.Phase = cocfg.Phase
		newActInfo.RwStatus = (ReadOk | WriteOk)
	} else {
		//cutover or unknown phase
		if cocfg.ActiveTwoTask == UnsetStr {
			logger.GetLogger().Log(logger.Alert, "CP 5 convert to ActiveInfo: no active DB in cutover phase", cocfg)
			newActInfo.ShId = ShIdUnset
			newActInfo.DbUname = UnsetStr
			newActInfo.Phase = cocfg.Phase
		} else {
			actDb := cocfg.DbBy2task[cocfg.ActiveTwoTask]
			newActInfo.DbUname = actDb
			newActInfo.Phase = cocfg.Phase
			newActInfo.ShId = cocfg.ActiveShardId
			newActInfo.RwStatus = cocfg.RWstatusByDb[actDb]
		}
	}
	logger.GetLogger().Log(logger.Alert, "CP 5 convert cfg to newActInfo (ShId, dbUname, phase, rwstatus)=(", newActInfo.ShId, newActInfo.DbUname, newActInfo.Phase, newActInfo.RwStatus, ")")
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
3. if no active config can be constructed, return hangup true
*/
func (crd *Coordinator) PreprocessCutover(requests []*netstring.Netstring) (bool, error) {

	if GetCutoverCfg() == nil {
		logger.GetLogger().Log(logger.Alert, crd.id, "PreprocessCutover at init")
		return true, nil
	}

	if crd.curActDb == nil {
		logger.GetLogger().Log(logger.Alert, crd.id, "PreprocessCutover crd.curActInfo is nil, expected when coordinator is just created")
	}

	interrupt := false // if txn should be disrupted
	newActInfo := cvtActiveInfo(GetCutoverCfg())
	if newActInfo == nil {
		logger.GetLogger().Log(logger.Alert, crd.id, "shtien PreprocessCutover no new activeInfo, treat as no diff")
		return interrupt, nil
	}

	if crd.curActDb == nil {
		// TODO. are we doing the right thing? need to check if we have missed some init case.
		logger.GetLogger().Log(logger.Alert, "crd.curActInfo is nil")
		crd.curActDb = newActInfo
	}
	diff := compActiveInfo(*crd.curActDb, *newActInfo)
	if diff == 0 {
		logger.GetLogger().Log(logger.Alert, "crd.curActInfo and newActInfo is the same")
		return interrupt, nil // same cutover config
	} else {
		logger.GetLogger().Log(logger.Alert, "crd.curActInfo and newActInfo is different", diff)
	}
	var err error
	if crd.inTransaction {
		// worker could be in a long txn, break it if needed.
		if (diff & 0x0001) == 0x0001 {
			if newActInfo.Phase == PrePhStr || newActInfo.Phase == EnablePhStr {
				if newActInfo.ShId != ShId2Task {
					logger.GetLogger().Log(logger.Alert, crd.id, "logging only. prior to cutover phase config using ShId2TaskCutover!")
				}
			} else if newActInfo.Phase == CompletePhStr {
				if newActInfo.ShId != ShId2TaskCutover {
					logger.GetLogger().Log(logger.Alert, crd.id, "logging only. post cutover phase config using ShId2Task!")
				}
			} else if newActInfo.Phase == CutoverPhStr {
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
			if newActInfo.Phase == CutoverPhStr {
				if crd.curActDb.RwStatus > newActInfo.RwStatus { // either W or R or both RW are newly disabled.
					if newActInfo.RwStatus == 0 {
						interrupt = true //we will check this later
						err = errors.New("cutover stop in-txn")
					}
					if newActInfo.RwStatus&0x0001 == 0 { // read is disabled now
						// stop READ
						if crd.isRead {
							interrupt = true // we will check this later
							err = errors.New("cutover stop in-txn read")
						}
					}
					if newActInfo.RwStatus&0x0002 == 0 { // write is disabled now
						// stop WRTIE
						if !crd.isRead {
							interrupt = true // we will check this later
							err = errors.New("cutover top in-txn write")
						}
					}
				}
			}
		}
		copyActCOInfo(crd.curActDb, *newActInfo)   // now coordinator has updated with latest info
		crd.shard.shardID = int(crd.curActDb.ShId) // we will need shardID in dispatchRequest(). hm..
		return interrupt, err
	} else {
		// worker is not in transactions
		// we will just load the new cfg and update the crd flags as needed.
		copyActCOInfo(crd.curActDb, *newActInfo)
		crd.shard.shardID = int(crd.curActDb.ShId)
		return false, nil // allow to proceed
	}
}

func (crd *Coordinator) getShardByCutoverCfg () (ShardByTwoTask, error) {
	shardToUse := ShIdUnset
	logger.GetLogger().Log(logger.Verbose, crd.id, "CP 6.2 cutover - run external query", crd.curActDb.Phase)
	if crd.curActDb.Phase == CutoverPhStr {
		logger.GetLogger().Log(logger.Verbose, crd.id, "CP 6.2 CUTOVER phase isRead [", crd.isRead, "] crd.curActInfo.Arwstatus [", crd.curActDb.RwStatus, "]")
		if crd.isRead {
			if (crd.curActDb.RwStatus & ReadOk) != ReadOk {
				logger.GetLogger().Log(logger.Alert, crd.id, "OCC-500: active db cutover no read allowed")
				return shardToUse, ErrCutoverReadNotAllowed
			}
		} else {
			if (crd.curActDb.RwStatus & WriteOk) != WriteOk {
				logger.GetLogger().Log(logger.Alert, crd.id, "OCC-501: active db cutover no write allowed")
				return shardToUse, ErrCutoverWriteNotAllowed
			}
		}

		shardToUse = crd.curActDb.ShId
		logger.GetLogger().Log(logger.Verbose, crd.id, "CP 6.2 CUTOVER phase, dispatch to", int(shardToUse), "workers")
	} else if crd.curActDb.Phase == EnablePhStr || crd.curActDb.Phase == PrePhStr {
		shardToUse = ShId2Task
		logger.GetLogger().Log(logger.Verbose, crd.id, "CP 6.2 ENABLE or PRE phase, dispatch to two_task workers", int(shardToUse))
	} else if crd.curActDb.Phase == CompletePhStr {
		shardToUse = ShId2TaskCutover
		logger.GetLogger().Log(logger.Verbose, crd.id, "CP 6.2 COMPLETE phase, dispatch to two_task_cutover", int(shardToUse))
	} else {
		logger.GetLogger().Log(logger.Verbose, crd.id, "CP 6.2 dispatchRequest error invalid cutover phase")
		return shardToUse, errors.New("Invalid cutover phase")
	}
	return shardToUse, nil
}



// only for internal write queries. When read cfg always use two_task shard, write uses two_task shard and cutover shard
//func (crd *Coordinator) processSetCoShardID(val []byte) error {
//	if !GetConfig().EnableCutover { // no need to pass
//		crd.coInternalShId = ShIdUnset
//		return nil
//	}
//	if !crd.isInternal { // not allow external connections
//		return ErrNotInternal
//	}
//
//	sh, err := strconv.ParseInt(string(val), 10, 32)
//	if err != nil {
//		return nil
//	}
//	// cutover enabled. we expect sh to be 0 (two_task) or 1 (two_task_cutover)
//	if sh != 0 && sh != 1 {
//		return ErrBadShardID
//	}
//
//	crd.coInternalShId = ShardByTwoTask(sh)
//	if crd.inTransaction && (crd.worker != nil) {
//		// in transaction, piggy back on the shard variable
//		if int(crd.coInternalShId) != crd.worker.shardID {
//			evt := cal.NewCalEvent(EvtTypeCutover, "internal query change pool", cal.TransOK, "")
//			evt.AddDataInt("cur_shard_id", int64(crd.worker.shardID))
//			evt.AddDataStr("requested_shard_id", string(val))
//			evt.Completed()
//			// processSetCoShardID has higher priority, switch worker.
//		}
//	}
//	if logger.GetLogger().V(logger.Debug) {
//		logger.GetLogger().Log(logger.Debug, crd.id, "Shard ID forced to", crd.shard.shardID)
//	}
//	return nil
//}
