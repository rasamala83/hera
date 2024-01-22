package lib

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"
)

type PoolByTwoTask int

// rapid overloaded the sharding setting
// two_task pool as shard 0
// two_task_cutover as shard 1
// max support 2 db at this time.
// > 2 is as undefined
const (
	Pool2Task PoolByTwoTask = iota
	Pool2TaskCutover
	MaxDbInCutover
	UndefP2T
)

const (
	EnabledPhase  = "enable"
	PrePhase      = "pre"
	CutoverPhase  = "cutover"
	CompletePhase = "complete"
	BroomPhase    = "broom"
)

// each record represents a table's row of record
type CutoverRecord struct {
	poolName    string
	dbUname     string
	twoTaskname string
	wstatus     sql.NullString
	rstatus     sql.NullString
	phase       string
	expiration  int
}

var moduleName string
var twoTaskName string        // i.e. MONEY
var twoTaskCutoverName string // i.e MONEY_CUTOVER

// a comphrehensive version of the state
type CutoverCfg struct {
	ActiveTwoTask string            // FOO or FOO_CUTOVER is the active
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
func validatePhase(phase string) bool {
	phase = strings.ToLower(strings.TrimSpace(phase))
	if phase == EnabledPhase || phase == PrePhase || phase == CutoverPhase || phase == CompletePhase || phase == BroomPhase {
		return true
	}
	return false
}

// compare two string case insensitive, return true if the same
func compCfgStr(name1 string, name2 string) bool {
	n1 := strings.ToLower(strings.TrimSpace(name1))
	n2 := strings.ToLower(strings.TrimSpace(name1))
	return n1 == n2
}

// main.go calls this InitCutoverCfg.
// Assumption:
// DB will not suspend the session process. Is the two database will have the same data in this table.
func InitCutoverCfg(poolname string) error {
	if GetConfig().EnableCutover {

		ctx := context.Background()
		var db *sql.DB
		var err error
		moduleName = poolname
		twoTaskName = os.Getenv("TWO_TASK")
		twoTaskCutoverName = os.Getenv("TWO_TASK_CUTOVER")
		// 1. start by connecting to primary pool (TWO_TASK).
		// 2. query to fetch shard (only allow 1 shard for now)  info and dbuname
		// 3. The instruction should have been populated.
		i := 0
		for ; i < 60; i++ {

			if db != nil {
				db.Close()
			}

			// always send cfg query to two_task connections
			db, err = cutoverOpenDb(Pool2Task)
			if err == nil {
				err = loadCutoverCfg(ctx, db)
				if err != nil {
					evt := cal.NewCalEvent(EvtTypeCutover, "init load cfg error", cal.TransOK, err.Error())
					evt.Completed()
				} else {
					evt := cal.NewCalEvent(EvtTypeCutover, "init load cfg", cal.TransOK, "successful")
					evt.Completed()
					break
				}
			} else {
				evt := cal.NewCalEvent(EvtTypeCutover, "init load opendb error", cal.TransOK, err.Error())
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

				// always get the record via two_task connections instead of two_task_cutover
				db, err = cutoverOpenDb(Pool2Task)
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
	}
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
	return fmt.Sprintf("select poolname, db_uname, two_task, write_status, read_status from %s_cutover_%s where poolname = '%s'", GetConfig().ManagementTablePrefix, GetConfig().CutoverPostfix, moduleName)
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
			return fmt.Errorf("error loaded more than 2 rows from cfg table")
		}
		rec := &records[nrow]
		err = rows.Scan(&(rec.poolName), &(rec.dbUname), &(rec.twoTaskname), &(rec.phase), &(rec.rstatus), &(rec.wstatus))
		if err != nil {
			return err
		}
		nrow++
	}

	if nrow < 2 {
		return fmt.Errorf("error expected 2 rows but get %d from cfg table", nrow)
	}

	if !compCfgStr(records[0].phase, records[1].phase) || !validatePhase(records[0].phase) {
		return fmt.Errorf("cutover cfg inconsistent phase %s, %s", records[0].phase, records[1].phase)
	}

	if !compCfgStr(records[0].twoTaskname, records[1].twoTaskname) {
		// two rows can't have same two_task name
		return fmt.Errorf("cutover cfg src and tgt two_task cannot be identical")
	}
	// ok to proceed
	newcfg.Phase = records[0].phase
	act := 0
	for i := 0; i < 2; i++ {
		records[i].twoTaskname = strings.ToUpper(records[i].twoTaskname)
		newcfg.DbBy2task[records[i].twoTaskname] = records[i].dbUname

		if records[i].rstatus.Valid && records[i].rstatus.String[0] == 'Y' {
			newcfg.RWstatusByDb[records[i].dbUname] |= 0x0001
		}
		if records[i].wstatus.Valid && records[i].wstatus.String[0] == 'Y' {
			newcfg.RWstatusByDb[records[i].dbUname] |= 0x0002
		}
		if newcfg.RWstatusByDb[records[i].dbUname] > 0 {
			logger.GetLogger().Log(logger.Warning, "Set active db two task name", records[i].twoTaskname)
			newcfg.ActiveTwoTask = records[i].twoTaskname
			act++
		}
	}

	if act > 1 {
		//critical condition dual active
		logger.GetLogger().Log(logger.Warning, "Error cutovercfg dual active db", records[0], records[1])
		evt := cal.NewCalEvent(EvtTypeCutover, "load cfgerror", cal.TransOK, "dual active db cfg")
		evt.Completed()
		return fmt.Errorf("error dual active db")
	}

	if precfg == nil {
		//this means from init
		if newcfg.ActiveTwoTask != "" {
			gCutoverCfg.Store(&newcfg)
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Verbose, "cutovercfg loaded:", GetConfig().MaxScuttleBuckets, "buckets")
			}
		}
	} else {
		if checkCfgChange(*precfg, newcfg) {
			gCutoverCfg.Store(&newcfg)
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Verbose, "cutovercfg loaded:", GetConfig().MaxScuttleBuckets, "buckets")
			}
		}
	}

	return nil
}

func checkCfgChange(curcfg CutoverCfg, newcfg CutoverCfg) bool {
	changed := false

	if curcfg.Phase != newcfg.Phase {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "cutover phase change %s to %s", curcfg.Phase, newcfg.Phase)
		}
	}

	if curcfg.DbBy2task[twoTaskName] != newcfg.DbBy2task[twoTaskName] {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "TWO_TASK Conn DBUname change %s to %s", curcfg.DbBy2task["FOO"], newcfg.DbBy2task["FOO"])
		}
	}

	if curcfg.DbBy2task[twoTaskCutoverName] != newcfg.DbBy2task[twoTaskCutoverName] {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "TWO_TASK_CUTOVER Conn DBUname change %s to %s", curcfg.DbBy2task["FOO_CUTOVER"], newcfg.DbBy2task["FOO_CUTOVER"])
		}
	}

	if curcfg.RWstatusByDb[twoTaskName] != newcfg.RWstatusByDb[twoTaskName] {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "TWO_TASK DBUname %s RW status changed from %s to %s", curcfg.RWstatusByDb[twoTaskName], newcfg.RWstatusByDb[twoTaskName])
		}
	}

	if curcfg.RWstatusByDb[twoTaskCutoverName] != newcfg.RWstatusByDb[twoTaskCutoverName] {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "TWO_TASK_CUTOVER DBUname %s RW status changed from %s to %s", curcfg.RWstatusByDb[twoTaskCutoverName], newcfg.RWstatusByDb[twoTaskCutoverName])
		}
	}
	return changed
}

// initialize the golang's database/sql object used to read the database configuration. The connection is created using the loopdriver,
// a sql driver used internally for ease of programming: the config load routines use standard database/sql interface.
func cutoverOpenDb(wkpool PoolByTwoTask) (*sql.DB, error) {
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

// looks like we only support bind by position
func writeDbLog(cfg CutoverCfg, ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("error (conn) write cutover cfg to Db: %s", err.Error())
	}
	defer conn.Close()
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
		return fmt.Errorf("Error (query) loading cutover cfg: %s", err.Error())
	}
	return nil
}
