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
	"github.com/paypal/hera/client/gosqldriver"
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
)

const (
	EnablePhId   = 1
	PrePhId      = 2
	CutoverPhId  = 3
	CompletePhId = 4
	BroomPhId    = 5
)

const (
	ReadOk  int = 0x0001
	WriteOk int = 0x0002
)

// each record represents a table's row of record
type CutoverRecord struct {
	poolName string
	dbUname  string
	occ2task string
	wstatus  sql.NullString
	rstatus  sql.NullString
	phase    string
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
	ActiveTwoTask  string            // FOO or FOO_CUTOVER is the active, maybe we don't need this because ActiveShardId
	ActiveShardId  ShardByTwoTask    // active shard id mapped to FOO or FOO_CUTOVER
	Phase          string            // current cutover phase
	DbBy2task      map[string]string // DB_UNAME by two_task and two_task_cutover
	RWstatusByDb   map[string]int    // uniqute db name --> rw status, 1 R, 2 W, 3 RW, 0 NRNW
	RWstatusByComb map[string]int    // two_task + dbuname --> rw status. e.g. CLOC_HERADB_PRIMARY, CLOC_CUTOVER_HERADB_PRIMARY as key
	UpdateTime     int
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

	// always query the two_task shard
	i := 0
	ctx := context.Background()
	var db *sql.DB
	var err error
	maxRetry := 10
	for ; i < maxRetry; i++ {

		if db != nil {
			db.Close()
		}
		// always send cfg query to two_task connections
		db, err = cutoverOpenDb(ShId2Task)
		evtname := "init_cfg_"
		if err == nil {
			err = loadCutoverCfg(ctx, db)
			if err != nil {
				evt := cal.NewCalEvent(EvtTypeCutover, evtname+strconv.Itoa(i), cal.TransOK, err.Error())
				evt.Completed()
			} else {
				evt := cal.NewCalEvent(EvtTypeCutover, evtname+"successful_"+strconv.Itoa(i), cal.TransOK, strconv.Itoa(i))
				evt.Completed()
				break
			}
		} else {
			evt := cal.NewCalEvent(EvtTypeCutover, evtname+"opendb_error_"+strconv.Itoa(i), cal.TransOK, err.Error())
			evt.Completed()
		}
		time.Sleep(time.Second)
	}

	if i == maxRetry {
		return errors.New("failed to load cutovercfg from two_task pool, no more retry")
	}

	err = writeCutoverLog(ctx)
	if err != nil {
		logger.GetLogger().Log(logger.Warning, "CP 0 InitCutoverCfg write to log failed", err.Error())
		// best effort. continue.
	}

	// spawn the routine to load config
	go func() {
		for {
			time.Sleep(time.Second * time.Duration(GetConfig().CutoverCfgReloadInterval))
			if db != nil {
				db.Close()
			}

			// always two_task connections
			db, err = cutoverOpenDb(ShId2Task)
			if err == nil {
				err = loadCutoverCfg(ctx, db)
				if err != nil {
					logger.GetLogger().Log(logger.Warning, "Error <", err, "> loading the cutovercfg from workerpool", GetCutoverCfg().DbBy2task[os.Getenv("TWO_TASK")])
					evt := cal.NewCalEvent(EvtTypeCutover, "load cfgerror", cal.TransOK, err.Error())
					evt.Completed()
				} else {
					evt := cal.NewCalEvent(EvtTypeCutover, "loadcfg", cal.TransOK, "success")
					evt.Completed()
					//err = writeCutoverLog(ctx)
					//if err != nil {
					//	logger.GetLogger().Log(logger.Warning, "CP 0 InitCutoverCfg write to log failed but continue", err.Error())
					// best effort. continue.
					//}
				}
			} else {
				evt := cal.NewCalEvent(EvtTypeCutover, "load opendb error", cal.TransOK, err.Error())
				evt.Completed()
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
	sqltxt := fmt.Sprintf("select occ_name, dbuname, occ_two_task, cutover_phase, write_status, read_status from %s_cutover where occ_name = '%s' and occ_two_task IN ('%s', '%s')",
		GetConfig().ManagementTablePrefix,
		//GetConfig().CutoverPostfix, // why do we need postfix for table name ?
		gModuleName,
		g2TaskName,
		g2TaskCutoverName)
	return sqltxt
}

func loadCutoverCfg(ctx context.Context, db *sql.DB) error {

	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "CP 7 Begin loading cutover cfg")
		defer func() {
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
		err = rows.Scan(&(rec.poolName), &(rec.dbUname), &(rec.occ2task), &(rec.phase), &(rec.wstatus), &(rec.rstatus))
		if err != nil {
			return err
		}
		nrow++
	}

	if nrow != 2 {
		return fmt.Errorf("CP 7 error expected 2 rows but get %d from cfg table", nrow)
	}

	// standardize a few things
	for i := 0; i < 2; i++ {
		records[i].phase = strings.ToUpper(strings.TrimSpace(records[i].phase))
		records[i].dbUname = strings.ToUpper(strings.TrimSpace(records[i].dbUname))
		records[i].occ2task = strings.ToUpper(strings.TrimSpace(records[i].occ2task))
	}
	// Having the query result, validate a few basic things
	same := isCfgSame(records[0].phase, records[1].phase) // phase is consistent
	if !same {
		logger.GetLogger().Log(logger.Alert, "CP 7 error cutovercfg load inconsistent phase", records[0].phase, records[1].phase)
		return fmt.Errorf("error cutovercfg query result has inconsistent phase %s, %s", records[0].phase, records[1].phase)
	}

	ph := validatePhase(records[0].phase) // phase is valid
	if ph == 0 {
		return fmt.Errorf("CP 7 error cutover cfg query result has invalid phase")
	}

	same = isCfgSame(records[0].occ2task, records[1].occ2task) // occ_two_task cannot be the same
	if same {
		return fmt.Errorf("CP 7 error cutovercfg query result has same two_task [%s, %s] [%s, %s]",
			records[0].occ2task, records[0].dbUname,
			records[1].occ2task, records[1].dbUname)
	}

	// check how many active db when populating the information into the newcfg struct
	var newcfg CutoverCfg
	newcfg.DbBy2task = make(map[string]string, 10)
	newcfg.RWstatusByDb = make(map[string]int, 10)
	newcfg.Phase = records[0].phase
	active := 0
	for i := 0; i < 2; i++ {
		newcfg.DbBy2task[records[i].occ2task] = records[i].dbUname
		newcfg.RWstatusByDb[records[i].dbUname] = 0
		if records[i].rstatus.Valid && records[i].rstatus.String[0] == 'Y' {
			logger.GetLogger().Log(logger.Debug, "CP 7 ==")
			newcfg.RWstatusByDb[records[i].dbUname] |= 0x0001
		}
		if records[i].wstatus.Valid && records[i].wstatus.String[0] == 'Y' {
			logger.GetLogger().Log(logger.Debug, "CP 7 === ")
			newcfg.RWstatusByDb[records[i].dbUname] |= 0x0002
		}

		logger.GetLogger().Log(logger.Debug, "CP 7 newcfg.RWStatusByDb[", records[i].dbUname, "], value =", newcfg.RWstatusByDb[records[i].dbUname])
		logger.GetLogger().Log(logger.Debug, "CP 7 newcfg.RWStatusByDb[", newcfg.DbBy2task[records[i].occ2task], "], value =", newcfg.RWstatusByDb[newcfg.DbBy2task[records[i].occ2task]])
		if newcfg.RWstatusByDb[newcfg.DbBy2task[records[i].occ2task]] > 0 {
			active++
			logger.GetLogger().Log(logger.Debug, "CP 7 Active is ", i)
			newcfg.ActiveTwoTask = records[i].occ2task // set it to active
			logger.GetLogger().Log(logger.Debug, "CP 7 check active db: newcfg.RWStatusByDb[", records[i].dbUname, "], value =", newcfg.RWstatusByDb[records[i].dbUname])
			logger.GetLogger().Log(logger.Debug, "CP 7 check active db: newcfg.RWStatusByDb[", newcfg.DbBy2task[records[i].occ2task], "], value =", newcfg.RWstatusByDb[newcfg.DbBy2task[records[i].occ2task]])
			if records[i].occ2task == g2TaskName { // set shardid based on ActiveTwoTask.
				logger.GetLogger().Log(logger.Alert, "CP 7 Setting ActiveShardId to", ShId2Task)
				newcfg.ActiveShardId = ShId2Task
			} else if records[i].occ2task == g2TaskCutoverName {
				logger.GetLogger().Log(logger.Alert, "CP 7 Setting ActiveShardId to", ShId2TaskCutover)
				newcfg.ActiveShardId = ShId2TaskCutover
			} else {
				logger.GetLogger().Log(logger.Alert, "CP 7 error unrecognized occ2task")
				newcfg.ActiveShardId = ShIdUnset
				// unrecognized
			}

			logger.GetLogger().Log(logger.Alert, "CP 7 setting newcfg.ActiveTwoTask", newcfg.ActiveTwoTask)
			if active >= 2 {
				logger.GetLogger().Log(logger.Alert, "CP 7 error cutovercfg both active")
				if newcfg.Phase == CutoverPhStr {
					newcfg.ActiveTwoTask = "INVALID" // just to be safe.
					logger.GetLogger().Log(logger.Alert, "CP 7 error cutovercfg both active, skip loading", records[0], records[1])
					evt := cal.NewCalEvent(EvtTypeCutover, "daul_active_skip_loading", cal.TransOK, "dual active db cfg")
					evt.Completed()
					return fmt.Errorf("error dual active db")
				} else {
					//outside Cutover Phase, read/write config is not applied. log warning and move on
					logger.GetLogger().Log(logger.Warning, "CP 7error cutovercfg both active", records[0], records[1])
					evt := cal.NewCalEvent(EvtTypeCutover, "CP 7 cfgerror_dual_active", cal.TransOK, "dual active db cfg")
					evt.Completed()
					newcfg.ActiveTwoTask = "INVALID" // just to be safe.
					// maybe we should also error out
				}
			}
		}
		logger.GetLogger().Log(logger.Verbose, "CP 7 load newcfg[", i, "](phase, dbuname, wstatus, rstatus)(", records[i].phase, records[i].dbUname, records[i].wstatus, records[i].rstatus, ")")

	}

	if active == 0 {
		if newcfg.Phase == CompletePhStr {
			newcfg.ActiveTwoTask = g2TaskCutoverName // reset to two_task_cutover
		} else {
			newcfg.ActiveTwoTask = g2TaskName // reset to two_task
		}
		logger.GetLogger().Log(logger.Alert, "CP 7 no active DB reset newcfg.ActiveTwoTask based on cutover phase ", newcfg.ActiveTwoTask)
	}

	/*
	   	ActiveTwoTask string            // FOO or FOO_CUTOVER is the active, maybe we don't need this because ActiveShardId
	           ActiveShardId ShardByTwoTask    // active shard id mapped to FOO or FOO_CUTOVER
	           Phase         string            // current cutover phase
	           DbBy2task     map[string]string // DB_UNAME by two_task and two_task_cutover
	           RWstatusByDb  map[string]int    // uniqute db name --> rw status, 1 R, 2 W, 3 RW, 0 NRNW
	*/

	logger.GetLogger().Log(logger.Verbose, "CP 7 dump newcfg (ActiveTwoTask,ActiveShardId,Phase, rwstatus)=(",
		newcfg.ActiveTwoTask, newcfg.ActiveShardId, newcfg.Phase, newcfg.RWstatusByDb[newcfg.DbBy2task[newcfg.ActiveTwoTask]], ")")
	precfg := GetCutoverCfg()
	if precfg == nil {
		logger.GetLogger().Log(logger.Verbose, "CP 7 INIT after run cutovercfg sql")

		//this means we are at init
		if newcfg.ActiveTwoTask != "" {
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Verbose, "CP 14 INIT cutovercfg init dump activecfg", newcfg)
			}
			// TODO we need to notify workersize change based on the Phase we are in
			cfgwkrchange := GetConfig().NumWorkersChW()
			// maybe we can define
			// Enable,Pre: 1 - two_task 100%, two_task_cutover 25%
			// Cutover : 2 - two_task 100%, two_task_cutover 100%
			// Broom: 3 - two_task 25%, two_task_cutover 100%
			// and write to the channel
			cfgwkrchange <- validatePhase(newcfg.Phase)
			// publish the cfg
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
					logger.GetLogger().Log(logger.Alert, "CP 14 INIT [shid, wtype][", shid, ",", t, "]")
					wpool, initerr := GetWorkerBrokerInstance().GetWorkerPool(HeraWorkerType(t), 0, shid)
					if initerr != nil {
						logger.GetLogger().Log(logger.Alert, "CP 14 INIT Error [shid, wtype] [", shid, ",", t, "]", err.Error())
					} else {
						if wpool != nil {
							if shid == int(ShId2Task) {
								wpool.ChangeCutoverInfo(newcfg.Phase, newcfg.DbBy2task[g2TaskName])
							}
							if shid == int(ShId2TaskCutover) {
								wpool.ChangeCutoverInfo(newcfg.Phase, newcfg.DbBy2task[g2TaskCutoverName])
							}
						} else {
							logger.GetLogger().Log(logger.Alert, "CP 14 INIT can't get workerpool [shid, type] [", shid, ",", t, "]")
						}
						wpool = nil
					}
				}
			}
		}
	} else {
		changed, changedAttr := CheckCfgChange(*precfg, newcfg)
		if !changed {
			logger.GetLogger().Log(logger.Alert, "CP 14 cutovercfg has no change")
		} else {
			logger.GetLogger().Log(logger.Alert, "CP 14 detected cutovercfg change", changedAttr)

			// gCutoverCfg.Store(&newcfg)
			// 1. Coordinator could retrieve (pull per sql) the new cfg after gCutoverCfg.Store(&newcfg)
			// 2. we notify workerpool to update (push once) when the two_task to dbname mapping is changed.
			// Comments: Every sql invokes the check and load latest cutover cfg but workerpool takes time to process and recycle workers that (dbuname) mismatch.
			// During the period, the dispatch can detect mismatch (between coordinator and workerpool) and fail that request.
			// or shall we delay Store call?
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
						// workerpool tracks phase, dbuname.
						// if phase unchanges but dbuname change
						// Return true if changed, false if the same
						// 0x0000 identical
						// 0x0001 phase
						// 0x0002 2tashShard's dbuname
						// 0x0004 2taskCutoverShard's dbuname
						// 0x0008 2taskShard's RW
						// 0x00016 2taskCutoverShard's RW
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

			logger.GetLogger().Log(logger.Warning, "CP 14 write to log")
			//err = writeCutoverLog(ctx)
			//if err != nil {
			//	logger.GetLogger().Log(logger.Warning, "CP 14 write to log failed", err.Error())
			// best effort. continue.
			//}
		}
	}

	return nil
}

// Return true if changed, false if the same
// 0x0000 identical
// 0x0001 phase
// 0x0002 2tashShard's dbuname
// 0x0004 2taskCutoverShard's dbuname
// 0x0008 2taskShard's RW
// 0x00016 2taskCutoverShard's RW
func CheckCfgChange(curcfg CutoverCfg, newcfg CutoverCfg) (bool, int) {
	changed := false
	whatchanged := 0
	if curcfg.Phase != newcfg.Phase {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "CP 18 cutover phase change", curcfg.Phase, "->", newcfg.Phase)
		}
		whatchanged |= 0x0001
	}

	if curcfg.DbBy2task[g2TaskName] != newcfg.DbBy2task[g2TaskName] {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "CP 18 TWO_TASK", g2TaskName, "Conn DBUname is changed:", curcfg.DbBy2task[g2TaskName], newcfg.DbBy2task[g2TaskName])
		}
		whatchanged |= 0x0002
	}

	if curcfg.DbBy2task[g2TaskCutoverName] != newcfg.DbBy2task[g2TaskCutoverName] {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "CP 18 TWO_TASK_CUTOVER", g2TaskCutoverName, "Conn DBUname is changed:",
				curcfg.DbBy2task[g2TaskCutoverName], newcfg.DbBy2task[g2TaskCutoverName])
		}
		whatchanged |= 0x0004
	}

	if curcfg.RWstatusByDb[curcfg.DbBy2task[g2TaskName]] != newcfg.RWstatusByDb[newcfg.DbBy2task[g2TaskName]] {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "CP 18 TWO_TASK", g2TaskName, "RW status changed from %s to %s",
				curcfg.RWstatusByDb[curcfg.DbBy2task[g2TaskName]], newcfg.RWstatusByDb[newcfg.DbBy2task[g2TaskName]])
		}
		whatchanged |= 0x0008
	}

	if curcfg.RWstatusByDb[g2TaskCutoverName] != newcfg.RWstatusByDb[g2TaskCutoverName] {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "CP 18 TWO_TASK_CUTOVER DBUname %s RW status changed from %s to %s", curcfg.RWstatusByDb[g2TaskCutoverName], newcfg.RWstatusByDb[g2TaskCutoverName])
		}
		whatchanged |= 0x0010
	}

	logger.GetLogger().Log(logger.Info, "CP 18 whatchanged", whatchanged)
	return changed, whatchanged
}

// initialize the golang's database/sql object used to read the database configuration. The connection is created using the loopdriver,
// a sql driver used internally for ease of programming: the config load routines use standard database/sql interface.
func cutoverOpenDb(wkpool ShardByTwoTask) (*sql.DB, error) {
	if wkpool > 1 {
		return nil, errors.New("rapid cutover not support more than 2 database")
	}

	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", wkpool))
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(0)
	return db, nil
}

// Generate the insert log sql
func getLogSQL() string {
	//poolname, hostname, two_task, db_uname, phase, write_status, read_status, last_update_time
	return fmt.Sprintf("insert into %s_cutover_log (occ_name, host_name, occ_two_task, dbuname, phase,  write_status, read_status, time_last_update) values (:occ_name, :host_name, :occ_two_task, :dbuname, :phase, :write_status, :read_status, :time_last_update)",
		GetConfig().ManagementTablePrefix)
}

// Best efforts log writing. Insert to both database connection pools two_task and two_task_cutover
func writeCutoverLog(ctx context.Context) error {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "CP 8 Begin loading cutover cfg")
		defer func() {
			logger.GetLogger().Log(logger.Verbose, "CP 8 Done loading cutover cfg")
		}()

	}

	cfg := GetCutoverCfg()
	if cfg == nil {
		logger.GetLogger().Log(logger.Verbose, "CP 8 cfg is nil")
		return nil
	}

	for sh := 0; sh < 2; sh++ {
		var db *sql.DB
		var err error
		// best efforts, write to both shard
		evtname := "write_log_"

		db, err = cutoverOpenDb(ShardByTwoTask(sh))
		if err != nil {
			evtname = evtname + "opendb_error_" + strconv.Itoa(sh)
			evt := cal.NewCalEvent(EvtTypeCutover, evtname, cal.TransOK, err.Error())
			evt.Completed()
		}
		conn, err := db.Conn(ctx)
		if err != nil {
			conn.Close()
			return fmt.Errorf("CP 8 error (conn) write cutover cfg to Db: %s", err.Error())
		}
		defer conn.Close()

		txn, err := db.BeginTx(ctx, nil)
		if err != nil {
			logger.GetLogger().Log(logger.Debug, "CP 8 BeginTx error", err.Error())
			return fmt.Errorf("CP 8 error BeginTx error %s", err.Error())
		}
		defer txn.Rollback()

		tomux := gosqldriver.InnerConn(conn)
		logger.GetLogger().Log(logger.Verbose, "CP 8 get connection to SetShardID")
		// Internal READ query has contract controlled by mux
		// Internal WRITE query, mux will follow to sessional shard setting
		tomux.SetShardID(int(ShId2Task))

		stmt, err := conn.PrepareContext(ctx, getLogSQL())
		if err != nil {
			conn.Close()
			return fmt.Errorf("error (stmt) loading cutover cfg: %s", err.Error())
		}
		defer stmt.Close()

		temp_hostname := "dummyhost"
		var bindIns []interface{}
		
		//("insert into %s_cutover_log (occ_name, host_name, occ_two_task, dbuname, phase,  write_status, read_status, time_last_update) values (:occ_name, :host_name, :occ_two_task, :dbuname, :phase, :write_status, :read_status, :time_last_update)",
		var BindInNames = []string{"occ_name", "host_name", "occ_two_task", "dbuname", "phase", "write_status", "read_status", "time_last_update"}
		ws, rs:= "N", "N"
		if cfg.RWstatusByDb[cfg.DbBy2task[g2TaskName]]&WriteOk == WriteOk {
			ws = "Y"
		}

		if cfg.RWstatusByDb[cfg.DbBy2task[g2TaskName]]&ReadOk == ReadOk {
			rs = "Y"
		}
		var BindInValues = []string{gModuleName, 
			temp_hostname, 
			g2TaskName, 
			cfg.DbBy2task[g2TaskName], 
			cfg.Phase,
			ws,
			rs,
			strconv.Itoa(cfg.UpdateTime)} // change to populate as int
		for i := 0; i < 8; i++ {
			bindIns = append(bindIns, sql.Named(BindInNames[i], BindInValues[i]))
		}

		//poolname, hostname, two_task, db_uname, phase, write_status, read_status, last_update_time
		result, err := stmt.ExecContext(ctx, bindIns...)
		if err != nil {
			conn.Close()
			return fmt.Errorf("CP 8 error (log query) insert error: %s", err.Error())
		}
		rows, err := result.RowsAffected()
		if err != nil {
			logger.GetLogger().Log(logger.Debug, "CP 8 RowsAffected error", err.Error())
			conn.Close()
			return fmt.Errorf("CP 8 error (log query) insert error: %s", err.Error())
		}
		err = txn.Commit()
		if err != nil {
			logger.GetLogger().Log(logger.Debug, "CP 8 inserted commit failure", rows)
			return fmt.Errorf("CP 8 insert commit failure %s", err.Error())
		}
		logger.GetLogger().Log(logger.Debug, "CP 8 inserted log", rows)
		conn.Close()
	}
	return nil
}

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
	case BroomPhStr:
		return BroomPhId
	default:
		logger.GetLogger().Log(logger.Warning, "config phase is invalid", phase)
		return 0
	}

}

// return true if two phases are the same otherwise false.
func isCfgSame(s1 string, s2 string) bool {
	n1 := strings.ToLower(strings.TrimSpace(s1))
	n2 := strings.ToLower(strings.TrimSpace(s2))
	same := (n1 == n2)
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
