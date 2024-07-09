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

// each record represents a table's row of record
type CutoverRecord struct {
	moduleName sql.NullString
	dbUname    sql.NullString
	occ2task   sql.NullString
	wstatus    sql.NullString
	rstatus    sql.NullString
	phase      sql.NullString
	wisbRoles  sql.NullString
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
	Phase         string         // current cutover phase
	ActiveTwoTask string         // FOO or FOO_CUTOVER is the active. If no active, set to UnsetStr ("NONE")
	ActiveShardId ShardByTwoTask // active shard id mapped to FOO or FOO_CUTOVER. Set to -1 if no active.
	//DbRwBy2task map[string]map[string]int// db logical name -> map[dbuname] to RW status
	DbBy2task    map[string]string // DB_UNAME by two_task and two_task_cutover
	RWstatusByDb map[string]int    // unique db name --> rw status, 1 R, 2 W, 3 RW, 0 NRNW
	UserRoleByDb map[string]int    // unique db name --> user enabled role.
	UpdateTime   int
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
			if logger.GetLogger().V(logger.Warning) {
				logger.GetLogger().Log(logger.Warning, "error: cutoverOpenDb()", strconv.Itoa(i), err.Error())
			}
		} else {
			err = loadCutoverCfg(db)
			if err != nil {
				evt := cal.NewCalEvent(EvtTypeCutover, "err_init_loadcfg_"+strconv.Itoa(i), cal.TransOK, err.Error())
				evt.Completed()
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "error: loadCutoverCfg()", strconv.Itoa(i), err.Error())
				}
			} else {
				evt := cal.NewCalEvent(EvtTypeCutover, "init_success_"+strconv.Itoa(i), cal.TransOK, "")
				evt.Completed()
				if logger.GetLogger().V(logger.Info) {
					logger.GetLogger().Log(logger.Info, "successful init cutovercfg")
				}
				break
			}
		}
		time.Sleep(time.Second)
	}

	if i == maxRetry {
		evt := cal.NewCalEvent(EvtTypeCutover, "init_fail_max_retry", cal.TransOK, strconv.Itoa(i))
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
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "err_reload_opendb", err.Error())
				}
			} else {
				err = loadCutoverCfg(db)
				if err != nil {
					evt := cal.NewCalEvent(EvtTypeCutover, "err_reload", cal.TransOK, err.Error())
					evt.Completed()
					if logger.GetLogger().V(logger.Warning) {
						logger.GetLogger().Log(logger.Warning, "error: reload loadCutoverCfg()", err.Error())
					}
				} else {
					logger.GetLogger().Log(logger.Info, "successful reload loadCutoverCfg()", "")
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
	if dst.DbBy2task == nil {
		dst.DbBy2task = make(map[string]string, 3)
	}
	for t := range src.DbBy2task {
		dst.DbBy2task[t] = src.DbBy2task[t]
	}
	dst.RWstatusByDb = src.RWstatusByDb
	for rw := range src.RWstatusByDb {
		dst.RWstatusByDb[rw] = src.RWstatusByDb[rw]
	}
	dst.UserRoleByDb = src.UserRoleByDb
}

/*
	 this function sends the sql to fetch the config records and validate the requirement before proceed further.
		1. config sql above returns exact two rows of data
		7. Not allow Null from phase, dbUname, rstatus, wstatus, wisbroles.
		2. can't have identical value for occ_two_task
		3. two rows must have consistent cutover_phase
		4. cutover_phase must be among ‘Enable’, ‘Pre’, ‘Cutover’, ‘Complete’ (case insensitive)
		5. two rows' dbuname must be different
		6. cutover_phase must be among ‘Enable’, ‘Pre’, ‘Cutover’, ‘Complete’ (case insensitive)
		7. read_status, write_status must be among ‘Y' or 'N’. (case insensitive)
		(Excluding cutover phase “Enable”), both the returned rows have 'Y' in read_status and/or write_status
*/
func loadCutoverCfg(db *sql.DB) error {

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer func() {
		cancel()
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
		if !records[i].phase.Valid {
			return fmt.Errorf("error cutover cfg has NULL string in Phase")
		}
		if !records[i].dbUname.Valid {
			return fmt.Errorf("error cutover cfg has NULL string in Db_name")
		}
		if !records[i].rstatus.Valid {
			return fmt.Errorf("error cutover cfg has NULL string in read_status")
		}
		if !records[i].wstatus.Valid {
			return fmt.Errorf("error cutover cfg has NULL string in write_status")
		}
		if !records[i].wisbRoles.Valid {
			return fmt.Errorf("error cutover cfg has NULL string in wisb_roles")
		}
		records[i].phase.String = strings.ToUpper(strings.TrimSpace(records[i].phase.String))
		records[i].dbUname.String = strings.ToUpper(strings.TrimSpace(records[i].dbUname.String))
		records[i].occ2task.String = strings.ToUpper(strings.TrimSpace(records[i].occ2task.String))
		records[i].rstatus.String = strings.ToUpper(strings.TrimSpace(records[i].rstatus.String))
		records[i].wstatus.String = strings.ToUpper(strings.TrimSpace(records[i].wstatus.String))
	}
	// can't have identical occ_two_task
	if isCfgSame(records[0].occ2task.String, records[1].occ2task.String) {
		evt := cal.NewCalEvent(EvtTypeCutover, "err_same_occ2task", cal.TransOK, "")
		evt.Completed()
		return fmt.Errorf("error cutover cfg can't have same two_task [%s, %s] [%s, %s]",
			records[0].occ2task.String, records[0].dbUname.String,
			records[1].occ2task.String, records[1].dbUname.String)
	}

	// both the returned rows have identical value of cutover_phase
	if !isCfgSame(records[0].phase.String, records[1].phase.String) {
		evt := cal.NewCalEvent(EvtTypeCutover, "err_diff_phase", cal.TransOK, "")
		evt.Completed()
		return fmt.Errorf("error: query result has inconsistent phase %s, %s", records[0].phase.String, records[1].phase.String)
	}

	// the value of cutover_phase in the two rows is not among ‘Enable’, ‘Pre’, ‘Cutover’, ‘Complete’ (case insensitive)
	ph := validatePhase(records[0].phase.String)
	if ph == 0 {
		evt := cal.NewCalEvent(EvtTypeCutover, "err_invalid_phase", cal.TransOK, "")
		evt.Completed()
		return fmt.Errorf("error: query result has invalid cutover_phase %s", records[0].phase.String)

	}
	// can't have identical dbuname. we shouldn't need to allow this.
	if isCfgSame(records[0].dbUname.String, records[1].dbUname.String) {
		evt := cal.NewCalEvent(EvtTypeCutover, "err_same_dbuname", cal.TransOK, "")
		evt.Completed()
		return fmt.Errorf("error:  can't have identical dbuname [%s:%s], [%s:%s] at cutover",
			records[0].occ2task.String, records[0].dbUname.String,
			records[1].occ2task.String, records[1].dbUname.String)
	}

	for i := 0; i < 2; i++ {
		if !(isValidRw(records[i].rstatus.String, records[i].wstatus.String)) {
			return fmt.Errorf("error: read or write status invalid")
		}
	}
	// count how many active db while populating newcfg
	var newcfg CutoverCfg
	newcfg.DbBy2task = make(map[string]string, 3)
	newcfg.RWstatusByDb = make(map[string]int, 3)
	newcfg.Phase = records[0].phase.String
	active := 0
	for i := 0; i < 2; i++ {
		rec2task := records[i].occ2task.String
		recDbUname := records[i].dbUname.String
		recRstatus := records[i].rstatus.String
		recWstatus := records[i].wstatus.String
		newcfg.DbBy2task[rec2task] = recDbUname
		newcfg.RWstatusByDb[recDbUname] = 0
		if recRstatus[0] == 'Y' {
			newcfg.RWstatusByDb[recDbUname] |= ReadOk
		}
		if recWstatus[0] == 'Y' {
			newcfg.RWstatusByDb[recDbUname] |= WriteOk
		}

		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, "rec", i, " newcfg RWStatusByDb[", recDbUname, "] = ", newcfg.RWstatusByDb[recDbUname])
		}

		if newcfg.RWstatusByDb[recDbUname] > 0 {
			active++
			newcfg.ActiveTwoTask = rec2task
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, "rec", i, "active db - two_task", newcfg.ActiveTwoTask, ", RWStatusByDb =", newcfg.RWstatusByDb[newcfg.DbBy2task[rec2task]])
			}

			// based on two_task name, set active shard id
			if rec2task == g2TaskName {
				newcfg.ActiveShardId = ShId2Task
			} else if rec2task == g2TaskCutoverName {
				newcfg.ActiveShardId = ShId2TaskCutover
			} else {
				// this should never happen w/ the defined sql
				evt := cal.NewCalEvent(EvtTypeCutover, "err_undefined_occ2task", cal.TransOK, rec2task)
				evt.Completed()
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "error: occ2task not match defined two_task or two_task_cutover", rec2task)
				}
				newcfg.ActiveTwoTask = UnsetStr
				newcfg.ActiveShardId = ShIdUnset
			}
			if active >= 2 {
				evt := cal.NewCalEvent(EvtTypeCutover, "err_multi_active_db", cal.TransOK, "")
				evt.Completed()
				newcfg.ActiveTwoTask = UnsetStr // just to be safe.
				return fmt.Errorf("error dual active db")
			}

		}
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, "finished load record", i, "](two_task, phase, dbuname, wstatus, rstatus)(", rec2task, records[i].phase, recDbUname, recWstatus, recRstatus, ")")
		}
	}
	// set default active db for Enable, Pre, and Complete phases.
	if newcfg.Phase == EnablePhStr || newcfg.Phase == PrePhStr {
		newcfg.ActiveTwoTask = g2TaskName
		newcfg.ActiveShardId = ShId2Task
	}

	if newcfg.Phase == CompletePhStr {
		newcfg.ActiveTwoTask = g2TaskCutoverName
		newcfg.ActiveShardId = ShId2TaskCutover
	}

	// handle no active db when phase is Cutover
	if active == 0 {
		evt := cal.NewCalEvent(EvtTypeCutover, "no_active_db", cal.TransOK, "")
		evt.Completed()
		newcfg.ActiveTwoTask = UnsetStr
		newcfg.ActiveShardId = ShIdUnset
		evt = cal.NewCalEvent(EvtTypeCutover, "no_active_db", cal.TransOK, "")
		evt.Completed()
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "no active DB at Cutover phase")
		}
	}
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "Dump newcfg active db info (TwoTask, ShardId, Phase, rwstatus)=(",
			newcfg.ActiveTwoTask, newcfg.ActiveShardId, newcfg.Phase, newcfg.RWstatusByDb[newcfg.DbBy2task[newcfg.ActiveTwoTask]], ")")
	}

	/**
	The order of action is
	1. Update global copy (coordinator can load the latest instruction)
	2. Notify workerpools to abort in-progress txn according to new RW status
		// if fail, we won't retry this attempt but depends on kill long running sql detected by DB (3~5s)
	3. Update Workerpools with (phase and dbuname) and enforce conn integraty
		// if fail, cutover workerpools is left wrong dbuname. maybe we should notify workerpools even during no change.
		// Reset global copy, exit?
	4. Update workerpools to enable or disable check-set-user-role flag
		// if fail, workerpool has wrong flag, we should notify workerpools even during no change.
		// workerpool internally sends msg to workers if flag changes.
	5. Resize workerpool size if needed
		// if fail, the pool size can continue to be wrong. maybe we should notify workerpools even during no change.
	**/

	precfg := GetCutoverCfg()
	if precfg == nil {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, "INIT cutover cfg", newcfg)
		}

		gCutoverCfg.Store(&newcfg) // now coordinator can pick new cfg.
		evt := cal.NewCalEvent(EvtTypeCutover, "init_cfg_updated", cal.TransOK, "")
		evt.Completed()
		// now ensure the change-triggered action are done as well
		// update workerpool
		maxtype := int(wtypeRW)
		if GetConfig().ReadonlyPct > 0 {
			maxtype += 1
		}
		for shid := 0; shid < int(MaxDbInCutover); shid++ {
			for t := 0; t <= maxtype; t++ {
				wpool, initerr := GetWorkerBrokerInstance().GetWorkerPool(HeraWorkerType(t), 0, shid)
				if initerr != nil {
					evtn := fmt.Sprint("init_wpool_err", shid, "_", t)
					evt = cal.NewCalEvent(EvtTypeCutover, evtn, cal.TransOK, initerr.Error())
					evt.Completed()
					if logger.GetLogger().V(logger.Warning) {
						logger.GetLogger().Log(logger.Warning, "loadCutoverCfg() [shid, wtype] [", shid, ",", t, "]", initerr.Error())
					}
				} else {
					if wpool != nil {
						tname := g2TaskName
						if shid == int(ShId2TaskCutover) {
							tname = g2TaskCutoverName
						}
						wpool.ChangeCutoverInfo(newcfg.Phase, newcfg.DbBy2task[tname])
					} else {
						evtn := fmt.Sprint("init_wpool_nil_", shid, "_", t)
						evt = cal.NewCalEvent(EvtTypeCutover, evtn, cal.TransOK, "")
						evt.Completed()
						if logger.GetLogger().V(logger.Warning) {
							logger.GetLogger().Log(logger.Warning, "loadCutoverCfg() can't get workerpool [shid, type] [", shid, ",", t, "]")
						}
					}
					wpool = nil
				}
			}
		}
		immediateStopReq(nil, &newcfg)
		setCheckUserRoleFlag(&newcfg)
		cfgwkrchange := GetConfig().NumWorkersChW()
		cfgwkrchange <- validatePhase(newcfg.Phase)

	} else {
		changed, changedAttr := CheckCfgChange(*precfg, newcfg)
		if !changed {
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Debug, "cutovercfg reload shows no change")
			}
		}
		var curCfg CutoverCfg
		copyCutoverCfg(&curCfg, precfg) // create a deep copy
		if changed {
			if logger.GetLogger().V(logger.Info) {
				logger.GetLogger().Log(logger.Info, "cutovercfg has new change", changed, changedAttr)
			}
			evt := cal.NewCalEvent(EvtTypeCutover, "detect_cfg_change", cal.TransOK, "")
			evt.Completed()
			gCutoverCfg.Store(&newcfg)
			immediateStopReq(&curCfg, &newcfg)
		}

		setCheckUserRoleFlag(&newcfg)
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
					if logger.GetLogger().V(logger.Warning) {
						logger.GetLogger().Log(logger.Warning, "CP 14 error cutovercfg failed to udpate workerpool ", shid, t)
					}
				} else {
					// workerpool tracks phase, dbuname and enforce integrity at Pre, Cutover
					if wpool != nil {
						evtn := fmt.Sprint("err_wpool_", shid, "_", t)
						evt := cal.NewCalEvent(EvtTypeCutover, evtn, cal.TransOK, "")
						evt.Completed()
						if logger.GetLogger().V(logger.Warning) {
							logger.GetLogger().Log(logger.Warning, "loadCutoverCfg() [shid, wtype] [", shid, ",", t, "]")
						}
						tname := g2TaskName
						if shid == int(ShId2TaskCutover) {
							tname = g2TaskCutoverName
						}
						wpool.ChangeCutoverInfo(newcfg.Phase, newcfg.DbBy2task[tname])
					} else {
						if logger.GetLogger().V(logger.Warning) {
							logger.GetLogger().Log(logger.Warning, "CP 14 can't get workerpool [shid, type] [", shid, ",", t, "]")
						}
					}
				}
			}
		}

		cfgwkrchange := GetConfig().NumWorkersChW()
		cfgwkrchange <- validatePhase(newcfg.Phase)
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Verbose, "CP 14 cutovercfg change is processed and updated.")
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
		info := fmt.Sprint(curcfg.ActiveTwoTask, "_to_", nextcfg.ActiveTwoTask)
		evt := cal.NewCalEvent(EvtTypeCutover, "cfg_act_2task_chg", cal.TransOK, info)
		evt.Completed()
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "cfg ActiveTwoTask change", info)
		}
		whatchanged |= 0x0020
	}

	if curcfg.Phase != nextcfg.Phase {
		changed = true
		info := fmt.Sprint(curcfg.Phase, "_to_", nextcfg.Phase)
		evt := cal.NewCalEvent(EvtTypeCutover, "cfg_phase_chg", cal.TransOK, info)
		evt.Completed()
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "cfg cutover phase change", info)
		}
		whatchanged |= 0x0001
	}

	if curcfg.DbBy2task[g2TaskName] != nextcfg.DbBy2task[g2TaskName] {
		changed = true
		info := fmt.Sprint(g2TaskName, "_", curcfg.DbBy2task[g2TaskName], "_to_", nextcfg.DbBy2task[g2TaskName])
		evt := cal.NewCalEvent(EvtTypeCutover, "cfg_2task_db_chg", cal.TransOK, info)
		evt.Completed()
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, g2TaskName, "Conn DBUname is changed:", info)
		}
		whatchanged |= 0x0002
	}

	if curcfg.DbBy2task[g2TaskCutoverName] != nextcfg.DbBy2task[g2TaskCutoverName] {
		changed = true
		info := fmt.Sprint(g2TaskCutoverName, "_", curcfg.DbBy2task[g2TaskCutoverName], "_to_", nextcfg.DbBy2task[g2TaskCutoverName])
		evt := cal.NewCalEvent(EvtTypeCutover, "cfg_2taskcutover_db_chg", cal.TransOK, info)
		evt.Completed()
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, g2TaskCutoverName, "Conn DBUname is changed:", info)
		}
		whatchanged |= 0x0004
	}

	dbun := curcfg.DbBy2task[g2TaskName]
	if curcfg.RWstatusByDb[dbun] != nextcfg.RWstatusByDb[dbun] {
		changed = true
		info := fmt.Sprint(dbun, "_", curcfg.RWstatusByDb[dbun], "_to_", nextcfg.RWstatusByDb[dbun])
		evt := cal.NewCalEvent(EvtTypeCutover, "cfg_db_rw_chg", cal.TransOK, info)
		evt.Completed()
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, dbun, "RW status changed:", info)
		}
		whatchanged |= 0x0008
	}

	dbun = curcfg.DbBy2task[g2TaskCutoverName]
	if curcfg.RWstatusByDb[dbun] != nextcfg.RWstatusByDb[dbun] {
		changed = true
		info := fmt.Sprint(dbun, "_", curcfg.RWstatusByDb[dbun], "_to_", nextcfg.RWstatusByDb[dbun])
		evt := cal.NewCalEvent(EvtTypeCutover, "cfg_co_db_rw_chg", cal.TransOK, info)
		evt.Completed()
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, dbun, "RW status changed:", info)
		}
		whatchanged |= 0x0010
	}
	return changed, whatchanged
}

/*
when call workerpool of the shard that prev read_status or write _status or both, are changed from allowed to not allowed.
*/
func immediateStopReq(curcfg *CutoverCfg, nextcfg *CutoverCfg) {
	// Enter or continue in cutover phase: per rw status config, stop where it should
	// Exit cutover phase: take no action
	if nextcfg == nil {
		return
	}
	if nextcfg.Phase != CutoverPhStr { // we don't stop req for exiting Cutover
		return
	}

	if curcfg == nil { // at server init
		curcfg = nextcfg
	}

	curActDb := curcfg.DbBy2task[curcfg.ActiveTwoTask]
	curAct2task := curcfg.ActiveTwoTask
	nextAct2task := nextcfg.ActiveTwoTask
	stopR := false
	stopW := false
	if nextcfg.Phase == CutoverPhStr {
		// remains in cutover phase
		if curAct2task == nextAct2task {
			// active two_task remains the same
			if curAct2task == UnsetStr { // no active db
				return
			}
			curRw := curcfg.RWstatusByDb[curActDb]
			nextRw := nextcfg.RWstatusByDb[curActDb]
			if curRw == nextRw {
				return
			}
			stopR = ((curRw & ReadOk) == ReadOk) && ((nextRw & ReadOk) == 0)
			stopW = ((curRw & WriteOk) == WriteOk) && ((nextRw & WriteOk) == 0)
			if !(stopR || stopW) { // nothing to stop
				return
			}
			stopRwCalName := fmt.Sprint("stop_R", stopR, "_W", stopW)

			// sent the status to current active db.
			shid := int(curcfg.ActiveShardId)
			maxtype := int(wtypeRW)
			if GetConfig().ReadonlyPct > 0 {
				maxtype += 1
			}
			for t := 0; t <= maxtype; t++ {
				if logger.GetLogger().V(logger.Info) {
					logger.GetLogger().Log(logger.Alert, "same active two_task, stop in-progress request. stopR =", stopR, ", stopW =", stopW)
				}
				wpool, err := GetWorkerBrokerInstance().GetWorkerPool(HeraWorkerType(t), 0, shid)
				if err != nil {
					evt := cal.NewCalEvent(EvtTypeCutover, "err_wpool_stopRW", cal.TransOK, stopRwCalName)
					evt.Completed()
					if logger.GetLogger().V(logger.Warning) {
						logger.GetLogger().Log(logger.Warning, "CP 27 ", t, "error:", err.Error())
					}
				} else {
					if wpool != nil {
						evt := cal.NewCalEvent(EvtTypeCutover, stopRwCalName, cal.TransOK, "")
						evt.Completed()
						wpool.StopWorker(stopR, stopW)
					} else {
						if logger.GetLogger().V(logger.Alert) {
							logger.GetLogger().Log(logger.Alert, "error: can't get workerpool to stop r/w [shid, type] [", shid, ",", t, "]")
						}
					}
					wpool = nil
				}
			}
		} else {
			// cur active2task and next active2task are different
			//  * stop write on current active2task
			//  * no action needed for next active2task
			if curAct2task == UnsetStr { // from none to one active
				return
			}
			stopR = true
			stopW = true
			stopRwCalName := fmt.Sprint("stop_R", stopR, "_W", stopW)
			shid := int(curcfg.ActiveShardId)
			maxtype := int(wtypeRW)
			if GetConfig().ReadonlyPct > 0 {
				maxtype += 1
			}
			for t := 0; t <= maxtype; t++ {
				if logger.GetLogger().V(logger.Info) {
					logger.GetLogger().Log(logger.Info, "active two_task changes, stop in-progress request. stopR =", stopR, ", stopW =", stopW)
				}
				wpool, err := GetWorkerBrokerInstance().GetWorkerPool(HeraWorkerType(t), 0, shid)
				if err != nil {
					evt := cal.NewCalEvent(EvtTypeCutover, "err_wpool_stopRW_diff2task", cal.TransOK, stopRwCalName)
					evt.Completed()
					if logger.GetLogger().V(logger.Info) {
						logger.GetLogger().Log(logger.Info, "CP 27", t, "error:", err.Error())
					}
				} else {
					if wpool != nil {
						evt := cal.NewCalEvent(EvtTypeCutover, "stop_rw", cal.TransOK, "")
						evt.Completed()
						wpool.StopWorker(stopR, stopW)
					} else {
						if logger.GetLogger().V(logger.Info) {
							logger.GetLogger().Log(logger.Info, "CP 27 workerpool nil. [shid, type] [", shid, ",", t, "]")
						}
					}
					wpool = nil
				}
			}
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

// takes string input and return cooresponding non-zero phase ID. return 0 if input phase is defined
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
	default:
		logger.GetLogger().Log(logger.Warning, "config phase is invalid", phase)
		return 0
	}

}

// rec1 and rec2 are string representing write/read_status
// anything besides 'Y' or 'N' of first letter will return false
func isValidRw(rec1 string, rec2 string) bool {
	isValid := false
	if (rec1[0] == 'Y' || rec1[0] == 'N') && (rec2[0] == 'Y' || rec2[0] == 'N') {
		isValid = true
	}
	return isValid
}

// return true if two phases are the same otherwise false.
func isCfgSame(s1 string, s2 string) bool {
	same := (s1 == s2)
	return same
}

func setPermTwoTaskName() error {
	g2TaskName = strings.ToUpper(os.Getenv("TWO_TASK_0"))
	if g2TaskName == "" {
		g2TaskName = strings.ToUpper(os.Getenv("TWO_TASK"))
	}
	g2TaskCutoverName = strings.ToUpper(os.Getenv("TWO_TASK_CUTOVER_0"))
	if g2TaskCutoverName == "" {
		g2TaskCutoverName = strings.ToUpper(os.Getenv("TWO_TASK_CUTOVER"))
	}

	if g2TaskName == "" || g2TaskCutoverName == "" {
		return fmt.Errorf("error incomplete cutover env setup [%s] [%s]", g2TaskName, g2TaskRCutoverName)
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
			return fmt.Errorf("error incomplete cutover env setup [%s] [%s]", g2TaskRName, g2TaskRCutoverName)
		}
	}
	return nil
}

func setCheckUserRoleFlag(nextcfg *CutoverCfg) {
	if nextcfg == nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "cutover setUserRole cannot determine next cutovercfg")
		}
		return
	}

	var execSetUserRole uint = 0 // 0 disable, >0 enable
	if nextcfg.Phase == CutoverPhStr {
		// init and next phase is not cutover.
		execSetUserRole = 1
	}
	maxtype := int(wtypeRW)
	if GetConfig().ReadonlyPct > 0 {
		maxtype += 1
	}
	for shid := 0; shid < int(MaxDbInCutover); shid++ {
		for t := 0; t <= maxtype; t++ {
			wpool, err := GetWorkerBrokerInstance().GetWorkerPool(HeraWorkerType(t), 0, shid)
			if err != nil {
				evt := cal.NewCalEvent(EvtTypeCutover, "err_wpool_user_flag", cal.TransOK, err.Error())
				evt.Completed()
				if logger.GetLogger().V(logger.Warning) {
					logger.GetLogger().Log(logger.Warning, "error cutover set_user_role_flag[", shid, ",", t, "]", err.Error())
				}
			} else {
				if wpool != nil {
					evt := cal.NewCalEvent(EvtTypeCutover, "set_role_flag", cal.TransOK, "")
					evt.Completed()
					if logger.GetLogger().V(logger.Info) {
						logger.GetLogger().Log(logger.Info, "cutover set_user_role_flag [", shid, ",", t, "] to ", execSetUserRole)
					}
					wpool.CheckSetUserRole(execSetUserRole)
				} else {
					if logger.GetLogger().V(logger.Warning) {
						logger.GetLogger().Log(logger.Warning, "error cutover set_user_role_flag nil wpool [", shid, ",", t, "]")
					}
				}
			}
		}
	}
}
