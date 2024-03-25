package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/paypal/hera/client/gosqldriver"
	"github.com/paypal/hera/tests/util"
	"github.com/paypal/hera/utility/logger"
	"os"
	"strings"
	"testing"
	"time"
)

func init() {
	logger.CreateLogger("log.txt", "UT", logger.Info)
}

func initialSetup(t *testing.T) []util.DBStatus {
	println("SETTING THE ENV TO INITIAL SETUP")
	println("********************************")
	util.OCCBinarySetup(t, os.Getenv("GOPATH")+"/src/bin/mux")
	util.ResetOCCDocker(t)

	util.OCCConfig(t, "readonly_children_pct", "0", "/x/web/LIVE/occ/occ.cdb")
	util.CleanCutOverTable(t)
	util.RestartOCC(t)

	util.StartDBService("HERADB_ONE", "herabox_primary_srv", t)
	util.ShutDownDBService("HERADB_ONE", "herabox_secondary_srv", t)
	util.ShutDownDBService("HERADB_TWO", "herabox_primary_srv", t)
	util.StartDBService("HERADB_TWO", "herabox_secondary_srv", t)
	util.LockUnlockUser(t, "unlock", false)
	dbStatus, _ := util.GetDBStatus()
	for _, db := range dbStatus {
		for _, service := range db.DatabaseServices {
			if strings.TrimSpace(db.DBUniqueName) == "HERADB_TWO" &&
				strings.TrimSpace(service.ServiceName) == "herabox_primary_srv" && service.WorkerCount > 0 {
				util.KillSessions(t, true, "herabox_primary_srv")
				fmt.Println("Sleeping for 120 seconds for connection to jump back to main db")
				time.Sleep(120 * time.Second)
			}
		}
	}
	println("********************************")
	return util.LockUnlockUser(t, "unlock", true)
}

func TestCutOverEnable(t *testing.T) {

	initialSetup(t)

	util.EnableCutOver(t, false)

	fmt.Println("Sleeping for 30 seconds")
	time.Sleep(30 * time.Second)
	dbStatus, _ := util.GetDBStatus()
	util.ValidateWorkerCount("HERADB_ONE", "herabox_primary_srv", 25, 5, dbStatus, t)
	util.ValidateWorkerCount("HERADB_TWO", "herabox_secondary_srv", 12, 3, dbStatus, t)
}

func TestCutOverPreMode(t *testing.T) {

	initialSetup(t)

	util.EnableCutOver(t, false)
	time.Sleep(30 * time.Second)
	util.MoveCutOverPhase(t, "pre", "TestCutOverEnable")
	fmt.Println("Sleeping for 30 seconds")
	time.Sleep(30 * time.Second)
	dbStatus, _ := util.GetDBStatus()
	util.ValidateWorkerCount("HERADB_ONE", "herabox_primary_srv", 25, 3, dbStatus, t)
	util.ValidateWorkerCount("HERADB_TWO", "herabox_secondary_srv", 25, 3, dbStatus, t)
}

//func TestLegacyCutOver(t *testing.T) {
//	var wg sync.WaitGroup
//
//	dbStatus := initialSetupStats(t)
//	respChan := util.CT.SendClientTraffic(&wg)
//
//	util.ValidateWorkerCount("HERADB_ONE", "herabox_secondary_srv", -1, 0,
//		dbStatus, t)
//	util.ValidateWorkerCount("HERADB_TWO", "herabox_secondary_srv", -1, 0,
//		dbStatus, t)
//	util.ValidateWorkerCount("HERADB_ONE", "herabox_primary_srv", 25, 10,
//		dbStatus, t)
//	util.ValidateWorkerCount("HERADB_TWO", "herabox_primary_srv", 0, 0,
//		dbStatus, t)
//	time.Sleep(20 * time.Second)
//	util.LockUnlockUser(t, "lock", true)
//
//	util.EnableCutOver(t)
//	fmt.Println("sleeping for 20 to pick up new tns")
//	time.Sleep(20 * time.Second)
//	beforeServiceStop := time.Now().Unix() - 2
//	util.ShutDownDBService("HERADB_ONE", "herabox_primary_srv", t)
//	util.StartDBService("HERADB_ONE", "herabox_secondary_srv", t)
//	util.StartDBService("HERADB_TWO", "herabox_secondary_srv", t)
//
//	status := util.KillSessions(t, false, "herabox_primary_srv")
//	serviceStopTime := time.Now().Unix() + 1
//	fmt.Printf("Cutover Start time: %d\n", serviceStopTime)
//	fmt.Printf("beforeServiceStop time: %d\n", beforeServiceStop)
//	util.ValidateWorkerCount("HERADB_ONE", "herabox_primary_srv", -1, 0,
//		status, t)
//	util.ValidateWorkerCount("HERADB_TWO", "herabox_primary_srv", 0, 0,
//		status, t)
//	fmt.Println("sleeping for 15 seconds before unlocking")
//	time.Sleep(15 * time.Second)
//	util.LockUnlockUser(t, "unlock", true)
//	recoverTime := time.Now().Unix()
//	fmt.Println("sleeping for 15 seconds before validating for successful calls")
//	time.Sleep(15 * time.Second)
//	serviceUpTime := time.Now().Unix()
//	fmt.Printf("Service Jump time: %d\n", serviceUpTime)
//	fmt.Println("sleeping for 105 seconds to give time for worker to reconnect")
//	time.Sleep(105 * time.Second)
//	status, _ = util.GetDBStatus()
//	util.ValidateWorkerCount("HERADB_TWO", "herabox_primary_srv", 25, 10,
//		status, t)
//	trafficStats := util.CT.StopClientTraffic(respChan)
//	util.ValidateTraffic(t, trafficStats, beforeServiceStop, serviceUpTime, serviceStopTime, serviceUpTime, recoverTime)
//}
