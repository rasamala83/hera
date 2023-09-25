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
	P2TUndefined
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
	ActiveTwoTask     string            // FOO or FOO_CUTOVER is the active
	Phase             string            // current cutover phase
	DbUnameBy2task    map[string]string // DB_UNAME by two_task and two_task_cutover
	RWstatusByDbUname map[string]int    //dbuname --> rw status, 1 R, 2 W, 3 RW, 0 NRNW
	UpdateTime        int
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

func checkCfgPhase(coph string) bool {
	coph = strings.ToLower(strings.TrimSpace(coph))
	if coph == EnabledPhase || coph == PrePhase || coph == CutoverPhase || coph == CompletePhase || coph == BroomPhase {
		return true
	}

	return false
}

func checkCfg2task(name1 string, name2 string) bool {
	n1 := strings.ToLower(strings.TrimSpace(name1))
	n2 := strings.ToLower(strings.TrimSpace(name1))
	if n1 != n2 {
		return true
	}
	return false
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
		twoTaskName = "FOO"
		twoTaskCutoverName = "FOO_CUTOVER"
		// 1. start by connecting to primary pool (TWO_TASK).
		// 2. query to fetch shard (only allow 1 shard for now)  info and dbuname
		// 3. The instruction should have been populated.
		i := 0
		for ; i < 60; i++ {

			if db != nil {
				db.Close()
			}

			db, err = cutoverOpenDb(Pool2Task)
			if err == nil {
				err = loadCutoverCfg(ctx, db)
				if err == nil {
					break
				}
			} else {
				// can't connect, break to try TWO_TASK_CUTOVER
				break
			}
			time.Sleep(time.Second)
		}
		if i == 60 {
			return errors.New("Failed to load cutovercfg from two_task pool, no more retry")
		}
		// during the init set up, perhaps we need to do a secondary lookup if primary is not avaiable?

		go func() {
			for {
				time.Sleep(time.Second * time.Duration(GetConfig().CutoverCfgReloadInterval))
				if db != nil {
					db.Close()
				}
				// always get the record via two_task connections instead of two_task_cutover
				db, err = cutoverOpenDb(Pool2Task)
				if err == nil {
					err = loadMap(ctx, db)
					if err != nil {
						//what to do ?
					}
				}
				logger.GetLogger().Log(logger.Warning, "Error <", err, "> loading the cutovercfg from workerpool", GetCutoverCfg().DbUnameBy2task[os.Getenv("TWO_TASK")])
				evt := cal.NewCalEvent(cal.EventTypeError, "no_shard_map", cal.TransOK, "Error loading shard map")
				evt.Completed()
			}
		}()
	} else {
		var cfg ShardingCfg
		gShardingCfg.Store(&cfg)
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
		return fmt.Errorf("Error (conn) loading cutover cfg: %s", err.Error())
	}
	defer conn.Close()
	stmt, err := conn.PrepareContext(ctx, getCutoverSQL())
	if err != nil {
		return fmt.Errorf("Error (stmt) loading cutover cfg: %s", err.Error())
	}
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		fmt.Errorf("Error (query) loading cutover cfg: %s", err.Error())
	}
	defer rows.Close()

	var newcfg CutoverCfg
	var records [2]CutoverRecord
	nrow := 0
	precfg := GetCutoverCfg()
	for rows.Next() {
		if nrow > 2 {
			return fmt.Errorf("Error loaded more than 2 rows from cfg table")
		}
		rec := &records[nrow]
		err = rows.Scan(&(rec.poolName), &(rec.dbUname), &(rec.twoTaskname), &(rec.phase), &(rec.rstatus), &(rec.wstatus))
		if err != nil {
			return err
		}
		nrow++
	}

	if nrow < 2 {
		return fmt.Errorf("Error no data return from cfg table")
	}

	if records[0].phase != records[1].phase {
		// if two rows have different phase
		return fmt.Errorf("cutover cfg has inconsistent phase definition, ignore data.")
	} else if !checkCfgPhase(records[0].phase) {
		// if the phase is not defined
		return fmt.Errorf("cutover cfg phase is not valid")
	} else if !checkCfg2task(records[0].twoTaskname, records[1].twoTaskname) {
		// two rows can't have same two_task name
		return fmt.Errorf("cutover cfg two task cannot be identical")
	} else {
		// ok to proceed
		newcfg.Phase = records[0].phase
		act := 0
		for i := 0; i < 2; i++ {
			records[i].twoTaskname = strings.ToUpper(records[i].twoTaskname)
			newcfg.DbUnameBy2task[records[i].twoTaskname] = records[i].dbUname

			if records[i].rstatus.Valid && records[i].rstatus.String[0] == 'Y' {
				newcfg.RWstatusByDbUname[records[i].dbUname] |= 0x0001
			}
			if records[i].wstatus.Valid && records[i].wstatus.String[0] == 'Y' {
				newcfg.RWstatusByDbUname[records[i].dbUname] |= 0x0002
			}
			if newcfg.RWstatusByDbUname[records[i].dbUname] > 0 {
				newcfg.ActiveTwoTask = records[i].twoTaskname
				act++
			}
		}

		if act > 1 {
			//dual active, invalid
			return fmt.Errorf("Error dual active db")
		}
	}

	if newcfg.RWstatusByDbUname["FOO"] > 0 {
		newcfg.ActiveTwoTask = "FOO"
	} else if newcfg.RWstatusByDbUname["FOO_CUTOVER"] > 0 {
		newcfg.ActiveTwoTask = "FOOCUTOVER"
	} else {
		newcfg.ActiveTwoTask = ""
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
		changed := checkCfgChange(*precfg, newcfg)
		if changed == true {
			gCutoverCfg.Store(&newcfg)
			if logger.GetLogger().V(logger.Debug) {
				logger.GetLogger().Log(logger.Verbose, "cutovercfg loaded:", GetConfig().MaxScuttleBuckets, "buckets")
			}
		}
	}

	return nil
}

func checkCfgChange(cur CutoverCfg, newcfg CutoverCfg) bool {
	changed := false
	if cur.DbUnameBy2task["FOO"] != newcfg.DbUnameBy2task["FOO"] {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "TWO_TASK Conn DBUname change %s to %s", cur.DbUnameBy2task["FOO"], newcfg.DbUnameBy2task["FOO"])
		}
	}

	if cur.DbUnameBy2task["FOO_CUTOVER"] != newcfg.DbUnameBy2task["FOO_CUTOVER"] {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "TWO_TASK_CUTOVER Conn DBUname change %s to %s", cur.DbUnameBy2task["FOO_CUTOVER"], newcfg.DbUnameBy2task["FOO_CUTOVER"])
		}
	}

	if cur.Phase != newcfg.Phase {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "cutover phase change %s to %s", cur.Phase, newcfg.Phase)
		}
	}

	cur_rw_2t := cur.RWstatusByDbUname["FOO"]
	new_rw_2t := newcfg.RWstatusByDbUname["FOO"]
	cur_rw_2tco := cur.RWstatusByDbUname["FOO_CUTOVER"]
	new_rw_2tco := newcfg.RWstatusByDbUname["FOO_CUTOVER"]
	if cur_rw_2t != new_rw_2t {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "TWO_TASK DBUname %s RW status changed from %s to %s", cur_rw_2t, new_rw_2t)
		}
	}

	if cur_rw_2tco != new_rw_2tco {
		changed = true
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "TWO_TASK_CUTOVER DBUname %s RW status changed from %s to %s", cur_rw_2tco, new_rw_2tco)
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
		return fmt.Errorf("Error (conn) write cutover cfg to Db: %s", err.Error())
	}
	defer conn.Close()
	stmt, err := conn.PrepareContext(ctx, getLogSQL())
	if err != nil {
		return fmt.Errorf("Error (stmt) loading cutover cfg: %s", err.Error())
	}

	result, err := stmt.Exec(cfg.ActiveTwoTask, cfg.Phase, cfg.DbUnameBy2task[cfg.ActiveTwoTask],
		cfg.RWstatusByDbUname[cfg.DbUnameBy2task[cfg.ActiveTwoTask]],
		cfg.UpdateTime)
	//what do we do about this?
	result.RowsAffected()
	logger.GetLogger().Log(logger.Debug, "inserted log ", cfg.RWstatusByDbUname[cfg.DbUnameBy2task[cfg.ActiveTwoTask]], ", ", cfg.UpdateTime)

	if err != nil {
		fmt.Errorf("Error (query) loading cutover cfg: %s", err.Error())
	}
	return nil
}
