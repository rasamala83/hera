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
	//BroomPhStr    = "BROOM"
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
	moduleName sql.NullString
	dbUname  sql.NullString
	occ2task sql.NullString
	wstatus  sql.NullString
	rstatus  sql.NullString
	phase    sql.NullString
	wisbRoles sql.NullString
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

// main.go calls this function. DB can't suspend user sessions. 
func InitCutoverCfg(modulename string) error {
	if !GetConfig().EnableCutover {
		return nil
	}

	loadEnvErr := setPermTwoTaskName()

	if loadEnvErr != nil {
		evt := cal.NewCalEvent(EvtTypeCutover, "error_init_env", cal.TransOK, loadEnvErr.Error())
		evt.Completed()
		return loadEnvErr
	}

	gModuleName = modulename

	i := 0
	var db *sql.DB
	var err error
	maxRetry := 10
	for ; i < maxRetry; i++ {

		if db != nil {
			db.Close()
		}
		// always send cfg query to two_task connections
		db, err = cutoverOpenDb()
		if err != nil {
			evt := cal.NewCalEvent(EvtTypeCutover, "err_init_opendb_"+strconv.Itoa(i), cal.TransOK, err.Error())
			evt.Completed()
			logger.GetLogger().Log(logger.Alert, "error: cutoverOpenDb()", strconv.Itoa(i), err.Error())
		} else {
			err = loadCutoverCfg(db)
			if err != nil {
				evt := cal.NewCalEvent(EvtTypeCutover, "err_init_loadcfg_"+strconv.Itoa(i), cal.TransOK, err.Error())
				evt.Completed()
				logger.GetLogger().Log(logger.Alert, "error: loadCutoverCfg()", strconv.Itoa(i), err.Error())
			} else {
				evt := cal.NewCalEvent(EvtTypeCutover, "init_cutovercfg_"+strconv.Itoa(i), cal.TransOK,"")
				evt.Completed()
				logger.GetLogger().Log(logger.Info, "successful init cutovercfg")
				break
			}
		}
		time.Sleep(time.Second)
	}

	if i == maxRetry {
		evt := cal.NewCalEvent(EvtTypeCutover, "fail_init_cfg", cal.TransOK,strconv.Itoa(i))
		evt.Completed()
		return errors.New("failed init cutovercfg after max retry")
	}

	go func() {
		for {
			time.Sleep(time.Second * time.Duration(GetConfig().CutoverCfgReloadInterval))
			if db != nil {
				db.Close()
			}

			// always two_task connections
			db, err = cutoverOpenDb()
			if err != nil {
				evt := cal.NewCalEvent(EvtTypeCutover, "err_reload_opendb", cal.TransOK, err.Error())
				evt.Completed()
				logger.GetLogger().Log(logger.Warning, "err_reload_opendb", err.Error())
			} else {
				err = loadCutoverCfg(db)
				if err != nil {
					evt := cal.NewCalEvent(EvtTypeCutover, "err_reload_cfg", cal.TransOK, err.Error())
					evt.Completed()
					logger.GetLogger().Log(logger.Warning, "error: reload loadCutoverCfg()", err.Error())
				} else {
					logger.GetLogger().Log(logger.Info, "successful reload loadCutoverCfg()","") 
				}
			}
		}
	}()
	return nil
}

// Get the SQL used to read the cutover configuration.
func getCutoverSQL() string {
	sqltxt := fmt.Sprintf("select occ_name, dbuname, occ_two_task, cutover_phase, write_status, read_status, wisb_roles from %s_cutover where occ_name = '%s' and occ_two_task IN ('%s', '%s')",
		GetConfig().ManagementTablePrefix,
		//GetConfig().CutoverPostfix, // why do we need postfix for table name ?
		gModuleName,
		g2TaskName,
		g2TaskCutoverName)
	return sqltxt
}

// this function create a deep copy from src to dst
func copyCutoverCfg(dst *CutoverCfg, src *CutoverCfg) {
	dst.Phase = src.Phase
	dst.ActiveTwoTask = src.ActiveTwoTask
	dst.ActiveShardId = src.ActiveShardId
	dst.DbBy2task = src.DbBy2task
	dst.RWstatusByDb = src.RWstatusByDb
	dst.UserRoleByDb = src.UserRoleByDb
	dst.UpdateTime = src.UpdateTime
}

/* this function sends the sql to fetch the config records and validate the requirement before proceed further. 
	1. config sql above returns exact two rows of data
	2. can't have identical value of occ_two_task
	2. both the returned rows have identical value of cutover_phase
	3. the value of cutover_phase in the two rows is not among ‘Enable’, ‘Pre’, ‘Cutover’, ‘Complete’ (case insensitive)
	4. read_status, write_status is not ‘Y' or 'N’. (case insensitive)
	(Excluding cutover phase “Enable”), both the returned rows have 'Y' in read_status and/or write_status
	5. Not allow Null from phase, dbUname, rstatus, wstatus, wisbroles.
*/
func loadCutoverCfg(db *sql.DB) error {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer func() {
		cancel()
		logger.GetLogger().Log(logger.Verbose, "Completed. loadCutovercfg")

	}()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("error: (conn) loading cutover cfg: %s", err.Error())
	}
	defer conn.Close()
	stmt, err := conn.PrepareContext(ctx, getCutoverSQL())
	if err != nil {
		return fmt.Errorf("error: (stmt) loading cutover cfg: %s", err.Error())
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return fmt.Errorf("error: (query) loading cutover cfg: %s", err.Error())
	}
	defer rows.Close()

	var records [3]CutoverRecord
	nrow := 0
	for rows.Next() {
		if nrow > 2 {
			return fmt.Errorf("error: fetched more than 2 rows from cfg table")
		}
		rec := &records[nrow]
		err = rows.Scan(&(rec.moduleName), &(rec.dbUname), &(rec.occ2task), &(rec.phase), &(rec.wstatus), &(rec.rstatus), &(rec.wisbRoles))
		if err != nil {
			return err
		}
		nrow++
	}

	if nrow < 2 {
		return fmt.Errorf("error: expected 2 rows but get %d", nrow)
	}

	// Not allow Null from phase, dbUname, rstatus, wstatus, wisbroles.
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
	// can't have identical occ_two_task
	if (isCfgSame(records[0].occ2task.String, records[1].occ2task.String)) {
		return fmt.Errorf("CP 7 error cutover cfg can't have same two_task [%s, %s] [%s, %s]",
			records[0].occ2task.String, records[0].dbUname.String,
			records[1].occ2task.String , records[1].dbUname.String)
	}

	// both the returned rows have identical value of cutover_phase
	if (!isCfgSame(records[0].phase.String, records[1].phase.String)) {
		logger.GetLogger().Log(logger.Alert, "error: load cutovercfg has inconsistent cutover_phase", records[0].phase, records[1].phase)
		return fmt.Errorf("error: query result has inconsistent phase %s, %s", records[0].phase, records[1].phase)
	}

	// the value of cutover_phase in the two rows is not among ‘Enable’, ‘Pre’, ‘Cutover’, ‘Complete’ (case insensitive)
	ph := validatePhase(records[0].phase.String)
	if ph == 0 {
		logger.GetLogger().Log(logger.Alert, "error: load cutovercfg has invalid cutover_phase", records[0].phase)
		return fmt.Errorf("error: query result has invalid cutover_phase")

	} else if (isCfgSame(records[0].dbUname.String, records[1].dbUname.String)){ 
		// The two rows'dbUname can't be the same in Pre and Cutover phases.
		if (ph == CutoverPhId) {
			return fmt.Errorf("error:  query result can't have identical dbuname [%s:%s], [%s:%s] at cutover",
				records[0].occ2task.String, records[0].dbUname.String,
				records[1].occ2task.String, records[1].dbUname.String)
		}
		evt := cal.NewCalEvent(EvtTypeCutover, "same_dbuname_ok", cal.TransOK, "")
		evt.Completed()
		logger.GetLogger().Log(logger.Info, "ATTN: cutovercfg two_task and two_task_cutover dbuname identical") 
	}


	// count how many active db while populating newcfg 
	var newcfg CutoverCfg
	newcfg.DbBy2task = make(map[string]string, 10)
	newcfg.RWstatusByDb = make(map[string]int, 10)
	newcfg.Phase = records[0].phase.String
	active := 0
	for i := 0; i < 2; i++ {
		newcfg.DbBy2task[records[i].occ2task.String] = records[i].dbUname.String
		newcfg.RWstatusByDb[records[i].dbUname.String] = 0
		if records[i].rstatus.String[0] == 'Y' {
			newcfg.RWstatusByDb[records[i].dbUname.String] |= ReadOk
		}
		if records[i].wstatus.String[0] == 'Y' {
			newcfg.RWstatusByDb[records[i].dbUname.String] |= WriteOk
		}

		logger.GetLogger().Log(logger.Debug, "rec", i, " newcfg RWStatusByDb[", newcfg.DbBy2task[records[i].occ2task.String], 
					"] = ", newcfg.RWstatusByDb[newcfg.DbBy2task[records[i].occ2task.String]])

		// when both map to same db/dbuname with different rw status, the behavior becomes undeterministic
		if newcfg.RWstatusByDb[newcfg.DbBy2task[records[i].occ2task.String]] > 0 {
			active++
			newcfg.ActiveTwoTask = records[i].occ2task.String
			logger.GetLogger().Log(logger.Debug, "rec", i, "active db - two_task",
				newcfg.ActiveTwoTask, ", RWStatusByDb =", newcfg.RWstatusByDb[newcfg.DbBy2task[newcfg.ActiveTwoTask]])

			// based on two_task name, set active shard id 
			if records[i].occ2task.String == g2TaskName {
				newcfg.ActiveTwoTask = g2TaskName
				newcfg.ActiveShardId = ShId2Task
			} else if records[i].occ2task.String == g2TaskCutoverName {
				newcfg.ActiveTwoTask = g2TaskCutoverName
				newcfg.ActiveShardId = ShId2TaskCutover
			} else {
				// this shouldn't never happen w/ the defined sql
				evt := cal.NewCalEvent(EvtTypeCutover, "err_cfg_2task_not_mismatch", cal.TransOK, records[i].occ2task.String)
				evt.Completed()
				logger.GetLogger().Log(logger.Alert, "error: occ2task not match defined two_task or two_task_cutover", records[i].occ2task.String)
				newcfg.ActiveTwoTask = UnsetStr
				newcfg.ActiveShardId = ShIdUnset
			}

			if active >= 2 {
				logger.GetLogger().Log(logger.Alert, "error: cutovercfg has multi active db")
				// ignore this update
				if newcfg.Phase == CutoverPhStr {
					newcfg.ActiveTwoTask = UnsetStr // just to be safe.
					evt := cal.NewCalEvent(EvtTypeCutover, "err_multi_active_db", cal.TransOK, "skip this reload")
					evt.Completed()
					return fmt.Errorf("error dual active db")
				} else {

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

		if newcfg.Phase == CompletePhStr {
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

			/**
			During cutover dbuname is unlikely (shouldn't be)  changed, so the precedence of action is 
			1. Cutover reload (done by loadCutoverCfg())
			2. Validate config (done by loadCutoverCfg())
			3. Update global copy (so real-time flow can pick it up)
			4. Workerpool abort in-progress txn
			5. Update Workerpool ChangeCutoverInfo
			6. Update needed worker count change
			7. Set the user role flag
			**/
			var curCfg CutoverCfg
			copyCutoverCfg(&curCfg,precfg)
			gCutoverCfg.Store(&newcfg)
			evt := cal.NewCalEvent(EvtTypeCutover, "cutovercfg_updated", cal.TransOK, "")
			evt.Completed()

			logger.GetLogger().Log(logger.Alert, "CP 14 detected cutovercfg change", changed, changedAttr)
			doAbortWorker(&curCfg, &newcfg)
			setUserRole(&curCfg, &newcfg);
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
			//gCutoverCfg.Store(&newcfg)
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
func doAbortWorker(curcfg *CutoverCfg, nextcfg *CutoverCfg) {
	// Read, Write can be actually stopped only in CUTOVER phase
	if nextcfg.Phase != CutoverPhStr {
		return
	}

	curActDb := curcfg.DbBy2task[curcfg.ActiveTwoTask]
	nextActDb := nextcfg.DbBy2task[nextcfg.ActiveTwoTask]
	stopR := false
	stopW := false
	if curcfg.Phase != CutoverPhStr {
		// Enable -> Cutover, Pre -> Cutover, Complete -> Cutover
		// We will abort requests according to the RW status.
		if nextcfg.ActiveTwoTask == UnsetStr { // no active db. 
			stopR = true
			stopW = true
		} else {
			// we have the active db.
			if nextcfg.RWstatusByDb[nextActDb]&ReadOk != ReadOk {
				stopR = true
			}
			if nextcfg.RWstatusByDb[nextActDb]&WriteOk != WriteOk {
				stopW = true
			}
		}
	} else {
		maxtype := int(wtypeRW)
		if GetConfig().ReadonlyPct > 0 {
			maxtype += 1
		}
		// current and next are CUTOVER phase, check active db Y to N, and N to Y.
		if (curcfg.ActiveTwoTask == nextcfg.ActiveTwoTask) { // no active db change
			if curcfg.ActiveTwoTask == UnsetStr {
				return
			}

			// active db remains the same, see if any change from Y to N
			if ((curcfg.RWstatusByDb[curActDb]&ReadOk) == ReadOk) && ((nextcfg.RWstatusByDb[curActDb]&ReadOk) == 0) {
				stopR = true
			}

			if ((curcfg.RWstatusByDb[curActDb]&WriteOk) == WriteOk) && ((nextcfg.RWstatusByDb[curActDb]&WriteOk) == 0) {
				stopW = true
			}

			if !(stopR || stopW) {
				return
			}

			// next, stop sent to current active db.
			shid := int(curcfg.ActiveShardId)
			for t := 0; t <= maxtype; t++ {
				logger.GetLogger().Log(logger.Alert, "CP 27 Require to stop in-progress request. stopR =", stopR, ", stopW =", stopW)
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
		} else {
			// active db is changed between current and next cfg.

			// if curcfg is no active, nothing to stop
			if curcfg.ActiveTwoTask == UnsetStr {
				return
			}

			// active db is changed
			if nextcfg.ActiveTwoTask == UnsetStr {
				/*
					| DB1 |* | Y | Y |  -> | DB1 |  | N | N |
					| DB2 |  | N | N |     | DB2 |  | N | N |
				*/
				stopR = true
				stopW = true
				// stop sent to current active db
				shid := int(curcfg.ActiveShardId)
				for t := 0; t <= maxtype; t++ {
					logger.GetLogger().Log(logger.Alert, "CP 27 Require to stop in-progress request. stopR =", stopR, ", stopW =", stopW)
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
				return
			}
			// active db changes from one to another, we need to stop the current active db 
			stopR = true
			stopW = true
			// stop sent to current active db
			shid := int(curcfg.ActiveShardId)
			for t := 0; t <= maxtype; t++ {
				logger.GetLogger().Log(logger.Alert, "CP 27 Require to stop in-progress request. stopR =", stopR, ", stopW =", stopW)
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
		logger.GetLogger().Log(logger.Alert, "CP 27 Require to stop in-progress request. stopR =", stopR, ", stopW =", stopW)
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
