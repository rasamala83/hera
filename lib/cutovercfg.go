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
	EnabledPh  = "enable"
	PrePh      = "pre"
	CutoverPh  = "cutover"
	CompletePh = "complete"
	BroomPh    = "broom"
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
	WriteOK int = 0x0002
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

var moduleName string

// 2task names once initialized will never change
var twoTaskName string         // e.g.  MONEY
var twoTaskCutoverName string  // e.g.  MONEY_CUTOVER
var twoTaskRName string        //e.g. MONEY_OCC
var twoTaskRCutoverName string // e.g. MONEY_OCC_CUTOVER

// a comphrehensive version of the state
type CutoverCfg struct {
	ActiveTwoTask string            // FOO or FOO_CUTOVER is the active, maybe we don't need this because ActiveShardId
	ActiveShardId ShardByTwoTask    // active shard id mapped to FOO or FOO_CUTOVER
	Phase         string            // current cutover phase
	DbBy2task     map[string]string // DB_UNAME by two_task and two_task_cutover
	RWstatusByDb  map[string]int    // uniqute db name --> rw status, 1 R, 2 W, 3 RW, 0 NRNW
	UpdateTime    int
}

// we will have to view the records atomically.
var gCutoverCfg atomic.Value

// Get the cfg atomically
func GetCutoverCfg() *CutoverCfg {
	cfg := gCutoverCfg.Load()
	if cfg == nil {
		return nil
	}
	return cfg.(*CutoverCfg)
}

// validate if the phase is unregonized.
func validatePhase(phase string) int {
	switch phase {
	case EnabledPh:
		return EnablePhId
	case PrePh:
		return PrePhId
	case CutoverPh:
		return CutoverPhId
	case CompletePh:
		return CompletePhId
	case BroomPh:
		return BroomPhId
	default:
		logger.GetLogger().Log(logger.Warning, "config phase is invalid", phase)
		return 0
	}

}

// return true if two phases are the same otherwise false.
func isCfgSame(ph1 string, ph2 string) bool {
	n1 := strings.ToLower(strings.TrimSpace(ph1))
	n2 := strings.ToLower(strings.TrimSpace(ph1))
	same := (n1 == n2)
	return same
}

// main.go calls this InitCutoverCfg.
// Assumption:
// DB will not suspend the session process. Is the two database will have the same data in this table.
func InitCutoverCfg(poolname string) error {
	if !GetConfig().EnableCutover {
		return nil
	}
	ctx := context.Background()
	var db *sql.DB
	var err error
	moduleName = poolname
	twoTaskName = strings.ToUpper(os.Getenv("TWO_TASK_0"))
	if twoTaskName == "" {
		twoTaskName = strings.ToUpper(os.Getenv("TWO_TASK_0"))
	}
	twoTaskCutoverName = strings.ToUpper(os.Getenv("TWO_TASK_CUTOVER_0"))
	if twoTaskCutoverName == "" {
		twoTaskCutoverName = strings.ToUpper(os.Getenv("TWO_TASK_CUTOVER"))
	}

	if twoTaskName == "" || twoTaskCutoverName == "" {
		return fmt.Errorf("error can't proceed due to env not completed [%s] [%s]", twoTaskName, twoTaskRCutoverName)
	}

	if GetConfig().ReadonlyPct > 0 {
		twoTaskRName = strings.ToUpper(os.Getenv("TWO_TASK_READ_0"))
		if twoTaskRName == "" {
			twoTaskRName = strings.ToUpper(os.Getenv("TWO_TASK_READ"))
		}
		twoTaskRCutoverName = strings.ToUpper(os.Getenv("TWO_TASK_READ_CUTOVER_0"))
		if twoTaskRCutoverName == "" {
			twoTaskRCutoverName = strings.ToUpper(os.Getenv("TWO_TASK_READ_CUTOVER"))
		}
		if twoTaskRCutoverName == "" || twoTaskCutoverName == "" {
			// can't proceed
			return fmt.Errorf("error can't proceed due to env not completed [%s] [%s]", twoTaskRName, twoTaskRCutoverName)
		}
	}

	// only query the two_task shard
	i := 0
	for ; i < 60; i++ {

		if db != nil {
			db.Close()
		}
		// always send cfg query to two_task connections
		db, err = cutoverOpenDb(ShId2Task)
		evtname := "init_cfg_"
		if err == nil {
			err = loadCutoverCfg(ctx, db)
			if err != nil {
				evtname = evtname + strconv.Itoa(i)
				evt := cal.NewCalEvent(EvtTypeCutover, evtname, cal.TransOK, err.Error())
				evt.Completed()
			} else {
				evtname = evtname + "successful_" + strconv.Itoa(i)
				evt := cal.NewCalEvent(EvtTypeCutover, evtname, cal.TransOK, strconv.Itoa(i))
				evt.Completed()
				break
			}
		} else {
			evtname = evtname + "opendb_error_" + strconv.Itoa(i)
			evt := cal.NewCalEvent(EvtTypeCutover, evtname, cal.TransOK, err.Error())
			evt.Completed()
		}
		time.Sleep(time.Second)
	}

	if i == 60 {
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
	sqltxt := fmt.Sprintf("select occ_name, dbuname, occ_two_task, write_status, read_status, cutover_phase from %s_cutover_%s where poolname = '%s' and occ_two_task IN ('%s', '%s)",
		GetConfig().ManagementTablePrefix,
		GetConfig().CutoverPostfix,
		moduleName,
		twoTaskName,
		twoTaskCutoverName)
	return sqltxt
}

func loadCutoverCfg(ctx context.Context, db *sql.DB) error {

	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "Begin loading cutover cfg")
		defer func() {
			logger.GetLogger().Log(logger.Verbose, "Done loading cutover cfg")
		}()

	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("error (conn) loading cutover cfg: %s", err.Error())
	}
	defer conn.Close()
	stmt, err := conn.PrepareContext(ctx, getCutoverSQL())
	if err != nil {
		return fmt.Errorf("error (stmt) loading cutover cfg: %s", err.Error())
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return fmt.Errorf("error (query) loading cutover cfg: %s", err.Error())
	}
	defer rows.Close()

	var newcfg CutoverCfg
	var records [2]CutoverRecord
	nrow := 0
	precfg := GetCutoverCfg()
	for rows.Next() {
		if nrow > 2 {
			return fmt.Errorf("error more than 2 rows from cfg table")
		}
		rec := &records[nrow]
		err = rows.Scan(&(rec.poolName), &(rec.dbUname), &(rec.occ2task), &(rec.phase), &(rec.rstatus), &(rec.wstatus))
		if err != nil {
			return err
		}
		nrow++
	}

	if nrow != 2 {
		return fmt.Errorf("error expected 2 rows but get %d from cfg table", nrow)
	}

	// standardize a few things

	for i := 0; i < 2; i++ {
		records[i].phase = strings.ToUpper(strings.TrimSpace(records[i].phase))
		records[i].dbUname = strings.ToUpper(strings.TrimSpace(records[i].dbUname))
	}
	// Having the query result, validate a few basic things
	same := isCfgSame(records[0].phase, records[1].phase) // phase is consistent
	if !same {
		return fmt.Errorf("error cutovercfg query result has inconsistent phase %s, %s", records[0].phase, records[1].phase)
	}

	ph := validatePhase(records[0].phase) // phase is valid
	if ph == 0 {
		return fmt.Errorf("error cutover cfg query result has invalid phase")
	}

	same = isCfgSame(records[0].occ2task, records[1].occ2task) // occ_two_task cannot be the same
	if same {
		return fmt.Errorf("error cutovercfg query result has same two_task [%s, %s] [%s, %s]",
			records[0].occ2task, records[0].dbUname,
			records[0].occ2task, records[0].dbUname)
	}

	// check how many active db when populating the information into the newcfg struct
	newcfg.Phase = records[0].phase
	active := 0
	for i := 0; i < 2; i++ {
		newcfg.DbBy2task[records[i].occ2task] = records[i].dbUname
		newcfg.RWstatusByDb[records[i].dbUname] = 0
		if records[i].rstatus.Valid && records[i].rstatus.String[0] == 'Y' {
			newcfg.RWstatusByDb[records[i].dbUname] |= 0x0001
		}
		if records[i].wstatus.Valid && records[i].wstatus.String[0] == 'Y' {
			newcfg.RWstatusByDb[records[i].dbUname] |= 0x0002
		}
		if newcfg.RWstatusByDb[records[i].dbUname] > 0 {
			active++
			if active >= 2 {
				if newcfg.Phase == CutoverPh {
					logger.GetLogger().Log(logger.Alert, "error cutovercfg both active, skip loading", records[0], records[1])
					evt := cal.NewCalEvent(EvtTypeCutover, "daul_active_skip_loading", cal.TransOK, "dual active db cfg")
					evt.Completed()
					return fmt.Errorf("error dual active db")
				} else {
					//outside Cutover Phase, read/write config is not applied. log warning and move on
					logger.GetLogger().Log(logger.Warning, "error cutovercfg both active", records[0], records[1])
					evt := cal.NewCalEvent(EvtTypeCutover, "cfgerror_dual_active", cal.TransOK, "dual active db cfg")
					evt.Completed()
					newcfg.ActiveTwoTask = records[i].occ2task
					// maybe we should also error out
				}
			}
		}
	}

	if precfg == nil {
		//this means we are at init
		if newcfg.ActiveTwoTask != "" {
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Verbose, "cutovercfg init and loaded")
			}
			// TODO we need to notify workersize change based on the Phase we are in
			GetConfig().NumWorkersCh()
			// maybe we can define
			// Enable,Pre: 1 - two_task 100%, two_task_cutover 25%
			// Cutover : 2 - two_task 100%, two_task_cutover 100%
			// Broom: 3 - two_task 25%, two_task_cutover 100%
			// and write to the channel
			gAppConfig.numWorkersCh <- ph
			// publish the cfg
			gCutoverCfg.Store(&newcfg)
			// ensure the change-triggered action are done as well
			//

		}
	} else {
		changed, chgprofile := CheckCfgChange(*precfg, newcfg)
		if !changed {
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Alert, "cutovercfg load has no change")
			}
		} else {

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
						logger.GetLogger().Log(logger.Alert, "error cutovercfg failed to udpate workerpool ", t, shid)
					} else {
						// workerpool tracks phase, dbuname.
						// if phase unchanges but dbuname change
						if wpool != nil {
							_ph := precfg.Phase
							if chgprofile&0x0001 > 0 {
								_ph = newcfg.Phase
							}
							if chgprofile&0x0002 > 0 { // twotaskshard dbuname changed
								wpool.ChangeCutoverInfo(newcfg.DbBy2task[twoTaskName], _ph)
							}
							if chgprofile&0x0004 > 0 { // twotaskcutover shard dbuname changed
								wpool.ChangeCutoverInfo(newcfg.DbBy2task[twoTaskCutoverName], _ph)
							}
						}
					}
				}
			}

			gCutoverCfg.Store(&newcfg)
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Verbose, "cutovercfg loaded.")
			}
		}
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Alert, "cutovercfg loaded and change detected ")
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
	chgdProf := 0
	if curcfg.Phase != newcfg.Phase {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "cutover phase change %s to %s", curcfg.Phase, newcfg.Phase)
		}
		chgdProf |= 0x0001
	}

	if curcfg.DbBy2task[twoTaskName] != newcfg.DbBy2task[twoTaskName] {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "TWO_TASK Conn DBUname change %s to %s", curcfg.DbBy2task["FOO"], newcfg.DbBy2task["FOO"])
		}
		chgdProf |= 0x0002
	}

	if curcfg.DbBy2task[twoTaskCutoverName] != newcfg.DbBy2task[twoTaskCutoverName] {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "TWO_TASK_CUTOVER Conn DBUname change %s to %s", curcfg.DbBy2task["FOO_CUTOVER"], newcfg.DbBy2task["FOO_CUTOVER"])
		}
		chgdProf |= 0x0004
	}

	if curcfg.RWstatusByDb[twoTaskName] != newcfg.RWstatusByDb[twoTaskName] {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "TWO_TASK DBUname %s RW status changed from %s to %s", curcfg.RWstatusByDb[twoTaskName], newcfg.RWstatusByDb[twoTaskName])
		}
		chgdProf |= 0x0008
	}

	if curcfg.RWstatusByDb[twoTaskCutoverName] != newcfg.RWstatusByDb[twoTaskCutoverName] {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "TWO_TASK_CUTOVER DBUname %s RW status changed from %s to %s", curcfg.RWstatusByDb[twoTaskCutoverName], newcfg.RWstatusByDb[twoTaskCutoverName])
		}
		chgdProf |= 0x0010
	}
	return changed, 0
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

// we should use bind
func getLogSQL() string {
	return fmt.Sprintf("insert into %s_cutove_log_%s (poolname, db_uname, two_task, write_status, read_status, last_update_time) values (?, ?, ?, ?, ?, ?)",
		GetConfig().ManagementTablePrefix, GetConfig().CutoverPostfix)
}

// Best efforts log writing. Insert to both database connection pools two_task and two_task_cutover
func WriteCutoverLog(cfg CutoverCfg) error {

	ctx := context.Background()
	var db *sql.DB
	var err error
	//moduleName = poolname
	//twoTaskName = os.Getenv("TWO_TASK")

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("error (conn) write cutover cfg to Db: %s", err.Error())
	}
	defer conn.Close()

	tomux := gosqldriver.InnerConn(conn)
	tomux.SetShardID(int(ShId2Task))

	stmt, err := conn.PrepareContext(ctx, getLogSQL())
	if err != nil {
		return fmt.Errorf("error (stmt) loading cutover cfg: %s", err.Error())
	}

	result, err := stmt.Exec(cfg.ActiveTwoTask, cfg.Phase, cfg.DbBy2task[cfg.ActiveTwoTask],
		cfg.RWstatusByDb[cfg.DbBy2task[cfg.ActiveTwoTask]],
		cfg.UpdateTime)
	//what do we do about this?
	result.RowsAffected()
	logger.GetLogger().Log(logger.Debug, "inserted log ", cfg.RWstatusByDb[cfg.DbBy2task[cfg.ActiveTwoTask]], ", ", cfg.UpdateTime)

	if err != nil {
		return fmt.Errorf("error (log query) insert error: %s", err.Error())
	}
	return nil
}
