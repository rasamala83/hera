package lib

import (
	"errors"
	"strconv"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
)

// Active shard means the shard either take R, W, or RW sql.
// If no shard is active, the ActiveDbInfo should container empty strings.
// Each coordinator will pull the CutoverCfg to check if there is any update.
// The information is stored in a structure ActiveDbInfo as for which shard and RW, R, or W
type ActiveDbInfo struct {
	SrcTns     string         // source Tns key
	ShId       ShardByTwoTask // active pool shard id
	Phase      string         // current cutover phase
	DbUname    string         // Active DB_UNAME
	RwStatus   int            // dbuname --> rw status, 1 R, 2 W, 3 RW, 0 NRNW
}

func (crd *Coordinator) copyActCOInfo(destInfo *ActiveDbInfo, srcInfo ActiveDbInfo) {
	if destInfo == nil {
		destInfo = &ActiveDbInfo{}
	}
	destInfo.SrcTns = srcInfo.SrcTns
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
func compActiveInfo(cur *ActiveDbInfo, newcfg *ActiveDbInfo) int {
	if cur == nil || newcfg == nil {
		return -1
	}
	flag := 0
	if cur.ShId != newcfg.ShId {
		flag |= 0x0001
	}
	if cur.DbUname != newcfg.DbUname {
		flag |= 0x0002
	}
	if cur.Phase != newcfg.Phase {
		flag |= 0x0004
	}
	if cur.RwStatus != newcfg.RwStatus {
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
	newActInfo.SrcTns = cocfg.TnsByRole[Source]
	if cocfg.Phase == EnablePhStr || cocfg.Phase == FlexupPhStr {
		newActInfo.ShId = ShIdTns
		newActInfo.DbUname = cocfg.DbByTns[gTnsAlias]
		newActInfo.Phase = cocfg.Phase
		newActInfo.RwStatus = (ReadOk | WriteOk)
	} else {
		//cutover or unknown phase
		if cocfg.ActiveTns == UnsetStr {
			evt := cal.NewCalEvent(EvtTypeCutover, "cutover_no_active_db", cal.TransOK, "")
			evt.Completed()
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "No active DB in cutover phase", cocfg)
			}
			newActInfo.ShId = ShIdUnset
			newActInfo.DbUname = UnsetStr
			newActInfo.Phase = cocfg.Phase
		} else {
			actDb := cocfg.DbByTns[cocfg.ActiveTns]
			newActInfo.DbUname = actDb
			newActInfo.Phase = cocfg.Phase
			newActInfo.ShId = cocfg.ActiveShardId
			newActInfo.RwStatus = cocfg.RWstatusByDb[actDb]
		}
	}
	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, "ActiveDBInfo (ShId, dbUname, phase, rwstatus)=(", newActInfo.ShId, newActInfo.DbUname, newActInfo.Phase, newActInfo.RwStatus, ")")
	}
	return &newActInfo
}

/*
PreprocessCutover returns bool: hang up client connection or not regardless if error is nil. error: if there is an error in process.
Every sqlrequest goes through PreprocessCutover. The function loads the latest cfg and detect which pool shard it should go
and disconnect the client if needed.

	(00000) identical
	(00001) 1 if (active) twotask changes (shard id changes), terminate ongoing txn
	(00010) 2 if dbuname changes (doesn't affect shard id)
	(00100) 4 if phase changes (may force shard id )
	(01000) 8 if RWStatus changes. any type (of R or W) is stopped, terminate ongoing txn

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

	tmpcfg := GetCutoverCfg()
	if tmpcfg.Phase == "" {
		if !crd.isInternal {
			evt := cal.NewCalEvent(EvtTypeCutover, "preproc_cfg_nil_startup", cal.TransOK, "")
			evt.Completed()
			if logger.GetLogger().V(logger.Info) {
				logger.GetLogger().Log(logger.Info, crd.id, "cutovercfg is nil, likely at server start up")
			}
			return true, nil
		}
		evt := cal.NewCalEvent(EvtTypeCutover, "preproc_internal_startup", cal.TransOK, "")
		evt.Completed()
		return false, nil
	}

	interrupt := false // if txn should be disrupted
	newActInfo := cvtActiveInfo(&tmpcfg)
	if newActInfo == nil {
		evt := cal.NewCalEvent(EvtTypeCutover, "preproc_empty_newactive", cal.TransOK, "")
		evt.Completed()
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "something is wrong. no new activeInfo in PreprocessCutover")
		}
		if crd.curActDb != nil {
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, crd.id, "cur  Active [ShId, dbUname, phase, rwstatus]=[",
					crd.curActDb.ShId, crd.curActDb.DbUname, crd.curActDb.Phase, crd.curActDb.RwStatus, "]")
			}
		}
		return interrupt, nil
	}

	if crd.curActDb == nil {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, crd.id, "PreprocessCutover crd.curActInfo is nil, expected when coordinator is just created")
		}
		crd.curActDb = &ActiveDbInfo{}
		crd.copyActCOInfo(crd.curActDb, *newActInfo) // now coordinator has updated with latest info
	}
	diff := compActiveInfo(crd.curActDb, newActInfo)

	if diff < 0 {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, crd.id, "PreprocessCutover unexpected nil at comparing ActiveInfo")
		}
		return interrupt, nil
	}

	if diff == 0 {
		return interrupt, nil // same cutover config
	}

	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "crd.curActInfo and newActInfo is different", diff)
	}
	var err error
	if crd.inTransaction {
		// worker could be in a long txn, break it if needed.
		if (diff & 0x0001) == 0x0001 {
			if newActInfo.Phase == FlexupPhStr || newActInfo.Phase == EnablePhStr {
				if newActInfo.ShId != ShIdTns {
					if logger.GetLogger().V(logger.Info) {
						logger.GetLogger().Log(logger.Info, crd.id, "prior to cutover phase config using")
					}
				}
			} else if newActInfo.Phase == CutoverPhStr {
				//cutover phase, swithc worker pool
				logger.GetLogger().Log(logger.Alert, crd.id, "cutover phase worker pool switch")
				evt := cal.NewCalEvent(EvtTypeCutover, "crd_act_db_change", cal.TransOK, "")
				evt.Completed()
				interrupt = true
				err = errors.New("cutover database switch")
			} else {
				// shouldn't reach here
				logger.GetLogger().Log(logger.Alert, crd.id, "logging only. Unrecognized new cutover phase with occ_tns_alias change")
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
						evt := cal.NewCalEvent(EvtTypeCutover, "crd_stop_in_txn_rw", cal.TransOK, "")
						evt.Completed()
						interrupt = true //we will check this later
						err = errors.New("cutover stop in-txn")
					}
					if newActInfo.RwStatus&0x0001 == 0 { // read is disabled now
						// stop READ
						if crd.isRead {
							evt := cal.NewCalEvent(EvtTypeCutover, "crd_stop_in_txn_r", cal.TransOK, "")
							evt.Completed()
							interrupt = true // we will check this later
							err = errors.New("cutover stop in-txn read")
						}
					}
					if newActInfo.RwStatus&0x0002 == 0 { // write is disabled now
						// stop WRTIE
						if !crd.isRead {
							evt := cal.NewCalEvent(EvtTypeCutover, "crd_stop_in_txn_w", cal.TransOK, "")
							evt.Completed()
							interrupt = true // we will check this later
							err = errors.New("cutover top in-txn write")
						}
					}
				}
			}
		}
		crd.copyActCOInfo(crd.curActDb, *newActInfo) // now coordinator has updated with latest info
		crd.shard.shardID = int(crd.curActDb.ShId)   // we will need shardID in dispatchRequest(). hm..
		return interrupt, err
	} else {
		// worker is not in transactions
		// we will just load the new cfg and update the crd flags as needed.
		crd.copyActCOInfo(crd.curActDb, *newActInfo)
		crd.shard.shardID = int(crd.curActDb.ShId)
		return false, nil // allow to proceed
	}
}

func (crd *Coordinator) ProceedReadInCutover() error {
	if (crd.curActDb.Phase == CutoverPhStr) && ((crd.curActDb.RwStatus & ReadOk) != ReadOk) {
		logger.GetLogger().Log(logger.Alert, crd.id, "OCC-500: active db cutover no read allowed")
		return ErrCutoverReadNotAllowed
	}
	return nil
}

func (crd *Coordinator) ProceedWriteInCutover() error {
	if (crd.curActDb.Phase == CutoverPhStr) && ((crd.curActDb.RwStatus & WriteOk) != WriteOk) {
		logger.GetLogger().Log(logger.Alert, crd.id, "OCC-501: active db cutover no write allowed")
		return ErrCutoverWriteNotAllowed
	}
	return nil
}
func (crd *Coordinator) getSrcShardByCutoverCfg() ShardByTwoTask {
	if crd.curActDb == nil {
		logger.GetLogger().Log(logger.Warning, crd.id, "unknown source, ignore during server init")
		// we don't know yet
		return ShIdUnset
	}

	logger.GetLogger().Log(logger.Debug, crd.id, "get ActiveDb source tns", crd.curActDb.SrcTns)
	srcShId := ShIdTns
	if crd.curActDb.SrcTns == GetTnsCutoverName() {
		srcShId = ShIdTnsCutover
	}
	logger.GetLogger().Log(logger.Debug, crd.id, "ActiveDb source tns", srcShId)
	// reset the internal
	crd.shId4Internal = ShIdUnset
	return srcShId
}
func (crd *Coordinator) getShardByCutoverCfg() (ShardByTwoTask, error) {
	shardToUse := ShIdUnset
	if crd.curActDb.Phase == CutoverPhStr {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, crd.id, "phase cutover crd.isRead [", crd.isRead, "] crd.curActInfo.Arwstatus [", crd.curActDb.RwStatus, "]")
		}
		if crd.isRead {
			if (crd.curActDb.RwStatus & ReadOk) != ReadOk {
				if logger.GetLogger().V(logger.Info) {
					logger.GetLogger().Log(logger.Info, crd.id, "OCC-500: active db cutover no read allowed")
				}
				return shardToUse, ErrCutoverReadNotAllowed
			}
		} else {
			if (crd.curActDb.RwStatus & WriteOk) != WriteOk {
				if logger.GetLogger().V(logger.Info) {
					logger.GetLogger().Log(logger.Info, crd.id, "OCC-501: active db cutover no write allowed")
				}
				return shardToUse, ErrCutoverWriteNotAllowed
			}
		}

		shardToUse = crd.curActDb.ShId
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, crd.id, "phase cutover, dispatch to", int(shardToUse), "workers")
		}
	} else if crd.curActDb.Phase == EnablePhStr || crd.curActDb.Phase == FlexupPhStr {
		// now we need to know which connection pool is the source (in the opposite of target)
		shardToUse = crd.getSrcShardByCutoverCfg()
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, crd.id, "ENABLE or FLEXUP phase, get source shard = ", int(shardToUse))
		}
		if shardToUse >= MaxDbInCutover {
			// we can't default sql routing by unknown source
			shardToUse = ShIdUnset
			return shardToUse, ErrSrcUnknown
		}
	} else {
		logger.GetLogger().Log(logger.Verbose, crd.id, "dispatchRequest error invalid cutover phase")
		return shardToUse, errors.New("invalid cutover phase")
	}
	return shardToUse, nil
}

// only for internal write queries. When read cfg always use two_task shard, write uses two_task shard and cutover shard
func (crd *Coordinator) processSetCoShardID(val []byte) error {
	if !GetConfig().EnableCutover { // no need to pass
		crd.shId4Internal = ShIdUnset
		return nil
	}
	if !crd.isInternal { // not allow external connections
		return ErrNotInternal
	}

	sh, err := strconv.ParseInt(string(val), 10, 32)
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, crd.id, "processSetCoShardID", sh)
	}
	if err != nil {
		return nil
	}
	// cutover enabled. we expect sh to be 0 (two_task) or 1 (two_task_cutover)
	if sh != 0 && sh != 1 {
		return ErrBadShardID
	}

	crd.shId4Internal = ShardByTwoTask(sh)
	if crd.inTransaction && (crd.worker != nil) {
		// crd.worker.shardID is used by cutover feature so we check if we need switch.
		// this is unlikely since internal sql don't use persistent connection.
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, crd.id, "processSetCoShardID crd.shId4Internal", crd.shId4Internal, "crd.worker.shardID", crd.worker.shardID)
		}
		if int(crd.shId4Internal) != crd.worker.shardID {
			evt := cal.NewCalEvent(EvtTypeCutover, "internal query change pool", cal.TransOK, "")
			evt.AddDataInt("cur_shard_id", int64(crd.worker.shardID))
			evt.AddDataStr("requested_shard_id", string(val))
			evt.Completed()
			return ErrChangeShardIDInTxn
		}
	}
	return nil
}
