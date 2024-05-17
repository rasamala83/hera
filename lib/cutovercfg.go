package lib

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/paypal/hera/cal"
	//	"github.com/paypal/hera/client/gosqldriver"
	"github.com/paypal/hera/utility/logger"
)

// rapid overloaded the sharding setting
// two_task pool as shard 0
// two_task_cutover as shard 1
// max support 2 db at this time.
// > 2 is as undefined

const (
	EnablePhStr   = "ENABLE"
	PrePhStr      = "PRE"
	CutoverPhStr  = "CUTOVER"
	CompletePhStr = "COMPLETE"
	BroomPhStr    = "BROOM"
	UnsetStr      = "NONE"
)

const (
	EnablePhId   = 1
	PrePhId      = 2
	CutoverPhId  = 3
	CompletePhId = 4
//	BroomPhId    = 5
)

const (
	ReadOk  int = 0x0001
	WriteOk int = 0x0002
)

// each record represents a table's row of record
type CutoverRecord struct {
	//poolName string
	//dbUname  string
	//occ2task string
	poolName sql.NullString
	dbUname  sql.NullString
	occ2task sql.NullString
	wstatus  sql.NullString
	rstatus  sql.NullString
	phase    sql.NullString
	wisbRoles sql.NullString
	//expiration  int
}

var gModuleName string

// 2task names once initialized will never change
var g2TaskName string         // e.g.  MONEY
var g2TaskCutoverName string  // e.g.  MONEY_CUTOVER
var g2TaskRName string        //e.g. MONEY_OCC
var g2TaskRCutoverName string // e.g. MONEY_OCC_CUTOVER

// a comphrehensive version of the state
// maybe we should look up on RWstatus by two_task + DBuname so it allows both two_task and two_task_cutover point to the same DB like in ENABLE and BROOM state
type CutoverCfg struct {
	Phase         string            // current cutover phase
	ActiveTwoTask string            // FOO or FOO_CUTOVER is the active. If no active, set to UnsetStr ("NONE")
	ActiveShardId ShardByTwoTask    // active shard id mapped to FOO or FOO_CUTOVER. Set to -1 if no active.
	DbBy2task     map[string]string // DB_UNAME by two_task and two_task_cutover
	RWstatusByDb  map[string]int    // unique db name --> rw status, 1 R, 2 W, 3 RW, 0 NRNW
	UserRoleByDb  map[string]int    // unique db name --> user enabled role.
	UpdateTime int
}

// we will have to view the records atomically.
var gCutoverCfg atomic.Value

func Get2TaskName() string {
	return g2TaskName
}

func Get2TaskCutoverName() string {
	return g2TaskCutoverName
}

func Get2TaskRName() string {
	return g2TaskRName
}

func Get2TaskRCutoverName() string {
	return g2TaskRCutoverName
}

// Get the cfg atomically
func GetCutoverCfg() *CutoverCfg {
	cfg := gCutoverCfg.Load()
	if cfg == nil {
		return nil
	}
	return cfg.(*CutoverCfg)
}

// validate if the phase is unregonized.

// main.go calls this InitCutoverCfg.
// Assumption:
// DB will not suspend the session process. Is the two database will have the same data in this table.
func InitCutoverCfg(modulename string) error {
	if !GetConfig().EnableCutover {
		return nil
	}

	loadEnvErr := setPermTwoTaskName()

	if loadEnvErr != nil {
		return loadEnvErr
	}

	gModuleName = modulename

	i := 0
	//ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
        //defer cancel()
	var db *sql.DB
	var err error
	maxRetry := 10
	for ; i < maxRetry; i++ {

		if db != nil {
			db.Close()
		}
		// always send cfg query to two_task connections
		db, err = cutoverOpenDb()
		evtname := "init_cfg_"
		if err != nil {
			logger.GetLogger().Log(logger.Alert, "error: cutoverOpenDb", err.Error())
			evt := cal.NewCalEvent(EvtTypeCutover, evtname+"opendb_error_"+strconv.Itoa(i), cal.TransOK, err.Error())
			evt.Completed()
		} else {
			err = loadCutoverCfg(db)
			if err != nil {
				logger.GetLogger().Log(logger.Alert, "error: init loadCutoverCfg()", err.Error())
				evt := cal.NewCalEvent(EvtTypeCutover, evtname+strconv.Itoa(i), cal.TransOK, err.Error())
				evt.Completed()
			} else {
				logger.GetLogger().Log(logger.Info, "init loadCutoverCfg successful")
				evt := cal.NewCalEvent(EvtTypeCutover, evtname+"successful_"+strconv.Itoa(i), cal.TransOK, strconv.Itoa(i))
				evt.Completed()
				break
			}
		}

		time.Sleep(time.Second)
	}

	if i == maxRetry {
		return errors.New("failed to load cutovercfg from two_task pool, no more retry")
	}

	// spawn the routine to load config
	go func() {
		for {
			time.Sleep(time.Second * time.Duration(GetConfig().CutoverCfgReloadInterval))
			if db != nil {
				db.Close()
			}

			// always two_task connections
			db, err = cutoverOpenDb()
			if err != nil {
				evt := cal.NewCalEvent(EvtTypeCutover, "load opendb error", cal.TransOK, err.Error())
				evt.Completed()
			} else {
				err = loadCutoverCfg(db)
				if err != nil {
					logger.GetLogger().Log(logger.Warning, "error: loadCutoverCfg DB id", err.Error())
					evt := cal.NewCalEvent(EvtTypeCutover, "load cfgerror", cal.TransOK, err.Error())
					evt.Completed()
				} else {
					evt := cal.NewCalEvent(EvtTypeCutover, "loadcfg", cal.TransOK, "success")
					evt.Completed()
				}
			}
		}
	}()
	return nil
}

// Get the SQL used to read the cutover configuration.
// poolnad: occ name
// db_uname: db unique name
// two_task: expected two_task connections to this db (db_uname)
// write_status: allow write or not
// read_status: allow read or not
// cutover_phase: enable, pre, cutover, complete, broom
// expiration: time stamp or status
func getCutoverSQL() string {
	sqltxt := fmt.Sprintf("select occ_name, dbuname, occ_two_task, cutover_phase, write_status, read_status, wisb_roles from %s_cutover where occ_name = '%s' and occ_two_task IN ('%s', '%s')",
		GetConfig().ManagementTablePrefix,
		//GetConfig().CutoverPostfix, // why do we need postfix for table name ?
		gModuleName,
		g2TaskName,
		g2TaskCutoverName)
	return sqltxt
}

func loadCutoverCfg(db *sql.DB) error {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "CP 7 Begin loading cutover cfg")
		defer func() {
			cancel()
			logger.GetLogger().Log(logger.Verbose, "Done loading cutover cfg")

		}()

	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("CP 7 error (conn) loading cutover cfg: %s", err.Error())
	}
	defer conn.Close()
	stmt, err := conn.PrepareContext(ctx, getCutoverSQL())
	if err != nil {
		return fmt.Errorf("CP 7 error (stmt) loading cutover cfg: %s", err.Error())
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return fmt.Errorf("CP 7error (query) loading cutover cfg: %s", err.Error())
	}
	defer rows.Close()

	var records [2]CutoverRecord
	nrow := 0
	for rows.Next() {
		if nrow > 2 {
			return fmt.Errorf("CP 7 error more than 2 rows from cfg table")
		}
		//sqltxt := fmt.Sprintf("select occ_name, dbuname, occ_two_task, write_status, read_status, cutover_phase from %s_cutover where occ_name = '%s' and occ_two_task IN ('%s', '%s')",
		rec := &records[nrow]
		err = rows.Scan(&(rec.poolName), &(rec.dbUname), &(rec.occ2task), &(rec.phase), &(rec.wstatus), &(rec.rstatus), &(rec.wisbRoles))
		if err != nil {
			return err
		}
		nrow++
	}

	if nrow != 2 {
		return fmt.Errorf("CP 7 error expected 2 rows but get %d from cfg table", nrow)
	}

/*
Fundamental requirement for occ to process cutover config data
1. config sql above returns exact two rows of data
2. both the returned rows have identical value of cutover_phase
3. the value of cutover_phase in the two rows is not among ‘Enable’, ‘Pre’, ‘Cutover’, ‘Complete’ (case insensitive)
4. read_status, write_status is not ‘Y' or 'N’. (case insensitive)
(Excluding cutover phase “Enable”), both the returned rows have 'Y' in read_status and/or write_status
*/
	// standardize string case and error out with empty string 
	for i := 0; i < 2; i++ {
		if (!records[i].phase.Valid) {
			return fmt.Errorf("error cutover cfg has NULL string in Phase")
		}
		if (!records[i].dbUname.Valid) {
			return fmt.Errorf("error cutover cfg has NULL string in Db_name")
		}
		if (!records[i].rstatus.Valid) {
			return fmt.Errorf("error cutover cfg has NULL string in read_status")
		}
		if (!records[i].wstatus.Valid) {
			return fmt.Errorf("error cutover cfg has NULL string in write_status")
		}
		if (!records[i].wisbRoles.Valid) {
			return fmt.Errorf("error cutover cfg has NULL string in wisb_roles")
		}
		records[i].phase.String = strings.ToUpper(strings.TrimSpace(records[i].phase.String))
		records[i].dbUname.String = strings.ToUpper(strings.TrimSpace(records[i].dbUname.String))
		records[i].occ2task.String = strings.ToUpper(strings.TrimSpace(records[i].occ2task.String))
		records[i].rstatus.String = strings.ToUpper(strings.TrimSpace(records[i].rstatus.String))
		records[i].wstatus.String = strings.ToUpper(strings.TrimSpace(records[i].wstatus.String))
	}
	// The two rows'occ_two_task must not be the same
	if (isCfgSame(records[0].occ2task.String, records[1].occ2task.String)) {
		return fmt.Errorf("CP 7 error cutover cfg can't have same two_task [%s, %s] [%s, %s]",
			records[0].occ2task.String, records[0].dbUname.String,
			records[1].occ2task.String , records[1].dbUname.String)
	}

	// The two rows' phase must be identical
	if (!isCfgSame(records[0].phase.String, records[1].phase.String)) {
		logger.GetLogger().Log(logger.Alert, "CP 7 error load inconsistent phase", records[0].phase, records[1].phase)
		return fmt.Errorf("error cutovercfg query result has inconsistent phase %s, %s", records[0].phase, records[1].phase)
	}
	ph := validatePhase(records[0].phase.String) // phase is valid
	if ph == 0 {
		return fmt.Errorf("CP 7 error cutover cfg query result has invalid phase")
	} else if (isCfgSame(records[0].dbUname.String, records[1].dbUname.String)){ 
		// The two rows'dbUname can't be the same in Pre and Cutover phases.
		// maybe we just prohibit identical dbUname since we will no longer enforce integrity in Complete as well
		if (ph == CutoverPhId) { 
			return fmt.Errorf("CP 7 error cutovercfg can't have same dbuname [%s, %s] [%s, %s] during cutover",
				records[0].occ2task.String, records[0].dbUname.String,
				records[1].occ2task.String, records[1].dbUname.String)
		}
		// log only
		logger.GetLogger().Log(logger.Info, "CP 7 OK, just logging. cutover two_task and two_task_cutover dbuname identical") 
	}


	// count how many active db 
	var newcfg CutoverCfg
	newcfg.DbBy2task = make(map[string]string, 10)
	newcfg.RWstatusByDb = make(map[string]int, 10)
	newcfg.Phase = records[0].phase.String
	active := 0
	for i := 0; i < 2; i++ {
		newcfg.DbBy2task[records[i].occ2task.String] = records[i].dbUname.String
		newcfg.RWstatusByDb[records[i].dbUname.String] = 0
		if records[i].rstatus.String[0] == 'Y' {
			newcfg.RWstatusByDb[records[i].dbUname.String] |= 0x0001
		}
		if records[i].wstatus.String[0] == 'Y' {
			newcfg.RWstatusByDb[records[i].dbUname.String] |= 0x0002
		}

		logger.GetLogger().Log(logger.Debug, "CP 7", "rec id", i, ", newcfg.RWStatusByDb[", newcfg.DbBy2task[records[i].occ2task.String], 
					"], value =", newcfg.RWstatusByDb[newcfg.DbBy2task[records[i].occ2task.String]])
		if newcfg.RWstatusByDb[newcfg.DbBy2task[records[i].occ2task.String]] > 0 {
			active++
			newcfg.ActiveTwoTask = records[i].occ2task.String // set it to active
			logger.GetLogger().Log(logger.Debug, "CP 7 Active db checking, rec id", i, "newcfg two_task",
				newcfg.ActiveTwoTask, ", RWStatusByDb[", newcfg.DbBy2task[newcfg.ActiveTwoTask],
				"], value =", newcfg.RWstatusByDb[newcfg.DbBy2task[newcfg.ActiveTwoTask]])

			if records[i].occ2task.String == g2TaskName { // set shardid based on ActiveTwoTask.
				newcfg.ActiveTwoTask = g2TaskName
				newcfg.ActiveShardId = ShId2Task
			} else if records[i].occ2task.String == g2TaskCutoverName {
				newcfg.ActiveTwoTask = g2TaskCutoverName
				newcfg.ActiveShardId = ShId2TaskCutover
			} else {
				logger.GetLogger().Log(logger.Alert, "CP 7 error unrecognized occ2task")
				newcfg.ActiveTwoTask = UnsetStr
				newcfg.ActiveShardId = ShIdUnset
				// unrecognized
			}
			logger.GetLogger().Log(logger.Alert, "CP 7 Setting Active db ShId to", newcfg.ActiveShardId)

			if active >= 2 {
				logger.GetLogger().Log(logger.Alert, "CP 7 error: cutovercfg both db active")
				// ignore this update
				if newcfg.Phase == CutoverPhStr {
					newcfg.ActiveTwoTask = UnsetStr // just to be safe.
					evt := cal.NewCalEvent(EvtTypeCutover, "daul_active_skip_loading", cal.TransOK, "dual active db cfg")
					evt.Completed()
					return fmt.Errorf("error dual active db")
				} else {
					//outside Cutover Phase, read/write config is not applied. log warning and move on
					evt := cal.NewCalEvent(EvtTypeCutover, "CP 7 cfgerror_dual_active", cal.TransOK, "dual active db cfg")
					evt.Completed()
					newcfg.ActiveTwoTask = UnsetStr // just to be safe.
					newcfg.ActiveShardId = ShIdUnset
				}
			}
		}
		logger.GetLogger().Log(logger.Verbose, "CP 7 finished load record", i, "](two_task, phase, dbuname, wstatus, rstatus)(", records[i].occ2task, records[i].phase, records[i].dbUname, records[i].wstatus, records[i].rstatus, ")")

	}

	// handle if no active db
	if active == 0 {
		if newcfg.Phase == EnablePhStr || newcfg.Phase == PrePhStr {
			newcfg.ActiveTwoTask = g2TaskName
			newcfg.ActiveShardId = ShId2Task
		}

		if newcfg.Phase == CompletePhStr || newcfg.Phase == BroomPhStr {
			newcfg.ActiveTwoTask = g2TaskCutoverName // reset to two_task_cutover
			newcfg.ActiveShardId = ShId2TaskCutover
		}
		if newcfg.Phase == CutoverPhStr {
			// set it to None, this can be valid
			newcfg.ActiveTwoTask = UnsetStr // reset to two_task
			newcfg.ActiveShardId = ShIdUnset
		}
		logger.GetLogger().Log(logger.Alert, "CP 7 no active DB reset newcfg.ActiveTwoTask based on cutover phase ", newcfg.ActiveTwoTask)
	}

	logger.GetLogger().Log(logger.Verbose, "CP 7 dump newcfg active db info (TwoTask, ShardId, Phase, rwstatus)=(",
		newcfg.ActiveTwoTask, newcfg.ActiveShardId, newcfg.Phase, newcfg.RWstatusByDb[newcfg.DbBy2task[newcfg.ActiveTwoTask]], ")")
	precfg := GetCutoverCfg()
	if precfg == nil {
		logger.GetLogger().Log(logger.Verbose, "CP 7 INIT after run cutovercfg sql")
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Verbose, "CP 14 INIT cutovercfg init dump activecfg", newcfg)
		}
		// TODO we need to notify workersize change based on the Phase we are in
		cfgwkrchange := GetConfig().NumWorkersChW()
		cfgwkrchange <- validatePhase(newcfg.Phase)
		gCutoverCfg.Store(&newcfg)
		// ensure the change-triggered action are done as well
		//

		// here we also need to update workerpool
		maxtype := int(wtypeRW)
		if GetConfig().ReadonlyPct > 0 {
			maxtype += 1
		}
		for shid := 0; shid < int(MaxDbInCutover); shid++ {
			for t := 0; t <= maxtype; t++ {
				wpool, initerr := GetWorkerBrokerInstance().GetWorkerPool(HeraWorkerType(t), 0, shid)
				if initerr != nil {
					logger.GetLogger().Log(logger.Alert, "loadCutoverCfg() [shid, wtype] [", shid, ",", t, "]", initerr.Error())
				} else {
					if wpool != nil {
						if shid == int(ShId2Task) {
							wpool.ChangeCutoverInfo(newcfg.Phase, newcfg.DbBy2task[g2TaskName])
						}
						if shid == int(ShId2TaskCutover) {
							wpool.ChangeCutoverInfo(newcfg.Phase, newcfg.DbBy2task[g2TaskCutoverName])
						}
					} else {
						logger.GetLogger().Log(logger.Alert, "loadCutoverCfg() can't get workerpool [shid, type] [", shid, ",", t, "]")
					}
					wpool = nil
				}
			}
		}

		setUserRole(nil, &newcfg);

	} else {
		changed, changedAttr := CheckCfgChange(*precfg, newcfg)
		if !changed {
			logger.GetLogger().Log(logger.Alert, "CP 14 cutovercfg has no change")
		} else {
			logger.GetLogger().Log(logger.Alert, "CP 14 detected cutovercfg change", changed, changedAttr)
			doAbortWorker(*precfg, newcfg)
			setUserRole(precfg, &newcfg);
			// 1. Coordinator could retrieve (pull per sql) the new cfg after gCutoverCfg.Store(&newcfg)
			// 2. we notify workerpool to update (push once) when the two_task to dbname mapping is changed.
			// Comments: Every sql invokes the check and load latest global copy of cutovercfg but workerpool takes time to process and recycle workers on (dbuname) mismatch.
			// During the period, the dispatch can detect mismatch (between coordinator and workerpool) and fail that request.

			//
			// only works for wtypeRW and wtypeRO in cutover
			maxtype := int(wtypeRW)
			if GetConfig().ReadonlyPct > 0 {
				maxtype += 1
			}

			var wpool *WorkerPool
			for shid := 0; shid < int(MaxDbInCutover); shid++ {
				for t := 0; t <= maxtype; t++ {
					wpool = nil
					wpool, err = GetWorkerBrokerInstance().GetWorkerPool(HeraWorkerType(t), 0, shid)
					if err != nil {
						logger.GetLogger().Log(logger.Alert, "CP 14 error cutovercfg failed to udpate workerpool ", shid, t)
					} else {
						// workerpool tracks phase, dbuname and enforce integrity at Pre, Cutover
						if wpool != nil {
							logger.GetLogger().Log(logger.Alert, "CP 14 got the workerpool [shid, type] [", shid, ",", t, "]")
							// integrity is mainly for the workerpool phase + dbuname
							// if phase changed only, apply to all pools
							// if phase didn't change, we can add protection?
							if shid == int(ShId2Task) {
								wpool.ChangeCutoverInfo(newcfg.Phase, newcfg.DbBy2task[g2TaskName])
							}
							if shid == int(ShId2TaskCutover) {
								wpool.ChangeCutoverInfo(newcfg.Phase, newcfg.DbBy2task[g2TaskCutoverName])
							}
						} else {
							logger.GetLogger().Log(logger.Alert, "CP 14 can't get workerpool [shid, type] [", shid, ",", t, "]")
						}
					}
				}
			}

			gCutoverCfg.Store(&newcfg)
			cfgwkrchange := GetConfig().NumWorkersChW()
			cfgwkrchange <- validatePhase(newcfg.Phase)
			gCutoverCfg.Store(&newcfg)
			// ensure the change-triggered action are done as well
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Verbose, "CP 14 cutovercfg change is processed and updated.")
			}
		}
	}

	return nil
}

/*
	Return true if changed, false if the same. return changed attritubes value as:

0x0000  0 identical
0x0001  1 phase
0x0002  2 tashShard's dbuname
0x0004  4 2taskCutoverShard's dbuname
0x0008  8 2taskShard's RW
0x0010 16 2taskCutoverShard's RW
0x0020 32 active_two_task
*/
func CheckCfgChange(curcfg CutoverCfg, nextcfg CutoverCfg) (bool, int) {
	changed := false
	whatchanged := 0

	if curcfg.ActiveTwoTask != nextcfg.ActiveTwoTask {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "CP 18 ActiveTwoTask", curcfg.ActiveTwoTask, "->", nextcfg.ActiveTwoTask)
		}
		whatchanged |= 0x0020
	}

	if curcfg.Phase != nextcfg.Phase {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "CP 18 cutover phase change", curcfg.Phase, "->", nextcfg.Phase)
		}
		whatchanged |= 0x0001
	}

	if curcfg.DbBy2task[g2TaskName] != nextcfg.DbBy2task[g2TaskName] {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "CP 18 TWO_TASK", g2TaskName, "Conn DBUname is changed:", curcfg.DbBy2task[g2TaskName], nextcfg.DbBy2task[g2TaskName])
		}
		whatchanged |= 0x0002
	}

	if curcfg.DbBy2task[g2TaskCutoverName] != nextcfg.DbBy2task[g2TaskCutoverName] {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "CP 18 TWO_TASK_CUTOVER", g2TaskCutoverName, "Conn DBUname is changed:",
				curcfg.DbBy2task[g2TaskCutoverName], nextcfg.DbBy2task[g2TaskCutoverName])
		}
		whatchanged |= 0x0004
	}

	cutoverDb := curcfg.DbBy2task[g2TaskName]
	if curcfg.RWstatusByDb[cutoverDb] != nextcfg.RWstatusByDb[cutoverDb] {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "CP 18 TWO_TASK", g2TaskName, "RW status changed from %s to %s",
				curcfg.RWstatusByDb[curcfg.DbBy2task[g2TaskName]], nextcfg.RWstatusByDb[nextcfg.DbBy2task[g2TaskName]])
		}
		whatchanged |= 0x0008
	}

	cutoverDb = curcfg.DbBy2task[g2TaskCutoverName]
	if curcfg.RWstatusByDb[cutoverDb] != nextcfg.RWstatusByDb[cutoverDb] {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "CP 18 TWO_TASK_CUTOVER DBUname %s RW status changed from %s to %s", curcfg.RWstatusByDb[g2TaskCutoverName], nextcfg.RWstatusByDb[g2TaskCutoverName])
		}
		whatchanged |= 0x0010
	}

	logger.GetLogger().Log(logger.Info, "CP 18 whatchanged", whatchanged)
	return changed, whatchanged
}

/*
call workerpool of the shard that prev read_status or write _status or both, are changed from allowed to not allowed.
*/
func doAbortWorker(curcfg CutoverCfg, nextcfg CutoverCfg) {
	// Read, Write can be actually stopped only in CUTOVER phase
	if nextcfg.Phase != CutoverPhStr {
		return
	}

	curActDb := curcfg.DbBy2task[curcfg.ActiveTwoTask]
	nextActDb := nextcfg.DbBy2task[nextcfg.ActiveTwoTask]
	stopR := false
	stopW := false
	if curcfg.Phase != CutoverPhStr {
		// Current phase is not CUTOVER but next phase is. We will abort requests accordingly.
		if nextcfg.ActiveTwoTask == UnsetStr {
			stopR = true
			stopW = true
		} else {
			if nextcfg.RWstatusByDb[nextActDb]&ReadOk != ReadOk {
				stopR = true
			}
			if nextcfg.RWstatusByDb[nextActDb]&WriteOk != WriteOk {
				stopW = true
			}
		}
	} else {
		// both current and next cfg are in CUTOVER, need to check if it changes from Y to N

		if (curcfg.ActiveTwoTask != UnsetStr) && (nextcfg.ActiveTwoTask == UnsetStr) {
			/*
				| DB1 |* | Y | Y |  -> | DB1 |  | N | N |
				| DB2 |  | N | N |     | DB2 |  | N | N |
			*/
			if (curcfg.RWstatusByDb[curActDb] & ReadOk) == ReadOk {
				stopR = true
			}
			if (curcfg.RWstatusByDb[curActDb] & WriteOk) == WriteOk {
				stopW = true
			}
		}

		if (curcfg.ActiveTwoTask != UnsetStr) && (nextcfg.ActiveTwoTask != UnsetStr) {
			if curcfg.ActiveTwoTask != nextcfg.ActiveTwoTask {
				// active DB changes, stopRW only happens to current active DB
				/*
					| DB1 |* | Y | Y | -> | DB1 |  | N | N |
					| DB2 |  | N | N |    | DB2 |* | N | Y |
				*/
				if (curcfg.RWstatusByDb[curActDb] & ReadOk) == ReadOk {
					stopR = true
				}
				if (curcfg.RWstatusByDb[curActDb] & WriteOk) == WriteOk {
					stopW = true
				}
			} else {
				// active DB remains the same.
				if (curcfg.RWstatusByDb[curActDb]&ReadOk == ReadOk) && (nextcfg.RWstatusByDb[nextActDb]&ReadOk != ReadOk) {
					/*
						| DB1 |* | Y | N | -> | DB1 |* | N | N |
						| DB2 |  | N | N |    | DB2 |  | N | N |
					*/
					stopR = true
				}
				if (curcfg.RWstatusByDb[curActDb]&WriteOk == WriteOk) && (nextcfg.RWstatusByDb[nextActDb]&WriteOk != WriteOk) {
					stopR = true
				}
			}
		}
	}

	// shortcut
	if (!stopR) && (!stopW) {
		logger.GetLogger().Log(logger.Alert, "CP 27 Nothing is required to stop. Done doAbortWorker()")
		return
	}

	// we will abort the "current" active db
	shid := int(curcfg.ActiveShardId)
	maxtype := int(wtypeRW)
	if GetConfig().ReadonlyPct > 0 {
		maxtype += 1
	}
	for t := 0; t <= maxtype; t++ {
		logger.GetLogger().Log(logger.Alert, "CP 27 Require to stop in-progress request. Stop read =", stopR, ", Stop writ =", stopW)
		wpool, err := GetWorkerBrokerInstance().GetWorkerPool(HeraWorkerType(t), 0, shid)
		if err != nil {
			logger.GetLogger().Log(logger.Alert, "CP 27 ", t, "error:", err.Error())
		} else {
			if wpool != nil {
				if shid == int(ShId2Task) {
					wpool.StopWorker(stopR, stopW)
				}
				if shid == int(ShId2TaskCutover) {
					wpool.StopWorker(stopR, stopW)
				}
			} else {
				logger.GetLogger().Log(logger.Alert, "CP 27 workerpool nil. [shid, type] [", shid, ",", t, "]")
			}
			wpool = nil
		}
	}

}

func cutoverOpenDb() (*sql.DB, error) {
	db, err := sql.Open("heraloop", fmt.Sprintf("0:0:0"))
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(0)
	return db, nil
}

/*  validatePhase(phase string) int - func takes string input and return cooresponding non-zero phase ID. return 0 if input phase is defined
 */
func validatePhase(phase string) int {
	switch phase {
	case EnablePhStr:
		return EnablePhId
	case PrePhStr:
		return PrePhId
	case CutoverPhStr:
		return CutoverPhId
	case CompletePhStr:
		return CompletePhId
//	case BroomPhStr:
//		return BroomPhId
	default:
		logger.GetLogger().Log(logger.Warning, "config phase is invalid", phase)
		return 0
	}

}

// return true if two phases are the same otherwise false.
func isCfgSame(s1 string, s2 string) bool {
	same := (s1 == s2)
	return same
}

func setPermTwoTaskName() error {
	g2TaskName = strings.ToUpper(os.Getenv("TWO_TASK"))
	if g2TaskName == "" {
		g2TaskName = strings.ToUpper(os.Getenv("TWO_TASK"))
	}
	g2TaskCutoverName = strings.ToUpper(os.Getenv("TWO_TASK_CUTOVER"))
	if g2TaskCutoverName == "" {
		g2TaskCutoverName = strings.ToUpper(os.Getenv("TWO_TASK_CUTOVER"))
	}

	if g2TaskName == "" || g2TaskCutoverName == "" {
		return fmt.Errorf("can't proceed due to env not completed [%s] [%s]", g2TaskName, g2TaskRCutoverName)
	}

	if GetConfig().ReadonlyPct > 0 {
		g2TaskRName = strings.ToUpper(os.Getenv("TWO_TASK_READ_0"))
		if g2TaskRName == "" {
			g2TaskRName = strings.ToUpper(os.Getenv("TWO_TASK_READ"))
		}
		g2TaskRCutoverName = strings.ToUpper(os.Getenv("TWO_TASK_READ_CUTOVER_0"))
		if g2TaskRCutoverName == "" {
			g2TaskRCutoverName = strings.ToUpper(os.Getenv("TWO_TASK_READ_CUTOVER"))
		}
		if g2TaskRCutoverName == "" || g2TaskCutoverName == "" {
			// can't proceed
			return fmt.Errorf("error can't proceed due to env not completed [%s] [%s]", g2TaskRName, g2TaskRCutoverName)
		}
	}
	return nil
}

/* At cutover phase, we will also honor the user_role.
1. non-cutover to cutover phase, we will notify workerpools to tell the workers to start check and set user role
2. cutover to non-cutover phase, we will notify workerpools to tell the workers to stop check and set user role
3. should we pass the role_enabled value to the workers? no, but let's send versioning counter. Everytime the value of role_enable changes, increment the counter
*/
func setUserRole(curcfg *CutoverCfg, nextcfg *CutoverCfg) {
	var execSetUserRole uint = 0
	if (curcfg == nil) {
		// nil cutcfg indicates server startup before listener is enabled.
		execSetUserRole = 0
	} else if (nextcfg.Phase == CutoverPhStr) && (curcfg == nil) {
		execSetUserRole = 1
	} else if (nextcfg.Phase == CutoverPhStr) && (curcfg.Phase != CutoverPhStr) {
		execSetUserRole = 1
	}

	if (execSetUserRole > 0) {
		// during cutover phase, it is unrealistic scenario that db unique name be changed.
		maxtype := int(wtypeRW)
		if GetConfig().ReadonlyPct > 0 {
			maxtype += 1
		}

		for shid := 0; shid < int(MaxDbInCutover); shid++ {
			for t := 0; t <= maxtype; t++ {
				wpool, err := GetWorkerBrokerInstance().GetWorkerPool(HeraWorkerType(t), 0, shid)
				if err != nil {
					logger.GetLogger().Log(logger.Alert, "loadCutoverCfg() [shid, wtype] [", shid, ",", t, "]", err.Error())
				} else {
					if wpool != nil {
						logger.GetLogger().Log(logger.Alert, "cutover setUserRole", shid, ",", t, "]")
						// we will pass the user_role flag to workerpool, subsequently 
						// workerpool notifies all existing workers, and pass it to all future workers.
						wpool.CheckSetUserRole(execSetUserRole)
					}
				}
			}
		}
	}
}
