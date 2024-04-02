package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/paypal/hera/client/gosqldriver"
	"github.com/paypal/hera/tests/util"
	"github.com/paypal/hera/utility/logger"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func init() {
	logger.CreateLogger("log.txt", "UT", logger.Info)
}

func initialSetup(t *testing.T) []util.DBStatus {
	println("********************************")
	println("SETTING THE ENV TO INITIAL SETUP")
	println("********************************")

	util.OCCBinarySetup(t, os.Getenv("GOPATH")+"/src/bin/mux")
	util.ResetOCCDocker(t)

	// disable read write split feature
	util.OCCConfig(t, "readonly_children_pct", "0", "/x/web/LIVE/occ/occ.cdb")
	util.EnableDebugLog(t)

	// delete all the entries in the cut over metadata table
	util.CleanCutOverTable(t)

	// restart occ (without restarting docker) to pick the changes
	util.RestartOCC(t)

	// prepare db to initial state
	util.StartDBService("HERADB_ONE", "herabox_primary_srv", t)
	util.StartDBService("HERADB_TWO", "herabox_secondary_srv", t)

	util.ShutDownDBService("HERADB_ONE", "herabox_secondary_srv", t)
	util.ShutDownDBService("HERADB_TWO", "herabox_primary_srv", t)

	util.LockUnlockUser(t, "unlock", false)

	// validate if we are good in initial state
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
	dbStatus = util.LockUnlockUser(t, "unlock", true)
	println("********************************")
	println("END OF INITIAL SETUP")
	println("********************************")
	return dbStatus
}

func TestCutOverPositive(t *testing.T) {

	// bring the setup to initial state
	//OCC running with only one db in TNS, no env set for cut over, cut over table is empty
	initialSetup(t)

	// enable cut over env and tns changes
	util.EnableCutOver(t, false)

	fmt.Println("Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	// send client traffic
	var wg sync.WaitGroup
	respChan, dumpChan := util.CT.SendClientTraffic(&wg)

	stateLog := make(map[string]int)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 1
	util.ValidateStateLog(t, stateLog)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 1, t)

	startClientTraffic := time.Now().Unix()
	fmt.Printf("Moving from Enable to Pre Cutover state: %d\n", startClientTraffic)
	util.MoveCutOverPhase(t, util.CutOverPre, "TestCutOverPositive-Init")
	fmt.Println("Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 25, t)

	beforeCutOverStart := time.Now().Unix()

	trafficStats := util.CT.DumpTrafficStat(dumpChan)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, beforeCutOverStart-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, beforeCutOverStart-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, beforeCutOverStart-3, 1, 2)

	fmt.Printf("Moving from Pre to Cutover state(stopping write in main DB): %d\n", beforeCutOverStart)
	util.MoveCutOverPhase(t, util.CutOverPhaseI, "TestCutOverPositive-StopWrite")
	fmt.Println("Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	afterServiceStop := time.Now().Unix()
	fmt.Printf("Moved to Cutover state(stopped write in main db): %d\n", afterServiceStop)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 25, t)
	trafficStats = util.CT.DumpTrafficStat(dumpChan)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, beforeCutOverStart, afterServiceStop-3, 1, 2)

	fmt.Println("Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	trafficStats = util.CT.DumpTrafficStat(dumpChan)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterServiceStop, time.Now().Unix()-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterServiceStop, time.Now().Unix()-3)

	fmt.Printf("Moving Read to CutOver: %d\n", time.Now().Unix())
	util.MoveCutOverPhase(t, util.CutOverPhaseII, "TestCutOverPositive-CutOverRead")
	fmt.Println("Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	afterReadCutOver := time.Now().Unix()
	fmt.Printf("Moved Read to Cutover database: %d\n", afterReadCutOver)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 25, t)
	fmt.Println("Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	trafficStats = util.CT.DumpTrafficStat(dumpChan)
	readCutOverValidation := time.Now().Unix() - 3
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, afterReadCutOver, readCutOverValidation, 2, 1)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterServiceStop, readCutOverValidation)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterServiceStop, readCutOverValidation)

	fmt.Printf("Moving Write to CutOver: %d\n", time.Now().Unix())
	util.MoveCutOverPhase(t, util.CutOverPhaseIII, "TestCutOverPositive-CutOverWrite")
	fmt.Println("Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	afterWriteCutOver := time.Now().Unix()
	fmt.Printf("Moved Write to Cutover database: %d\n", afterWriteCutOver)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 25, t)
	fmt.Println("Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	trafficStats = util.CT.DumpTrafficStat(dumpChan)
	writeCutOverValidation := time.Now().Unix() - 3
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, readCutOverValidation, writeCutOverValidation, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, afterWriteCutOver, writeCutOverValidation, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, afterWriteCutOver, writeCutOverValidation, 2, 1)

	fmt.Printf("Moving to Complete State: %d\n", time.Now().Unix())
	util.MoveCutOverPhase(t, util.CutOverComplete, "TestCutOverPositive-CutOverComplete")
	util.StartDBService("HERADB_TWO", "herabox_primary_srv", t)
	util.ShutDownDBService("HERADB_ONE", "herabox_primary_srv", t)
	//util.PushTNSForComplete(t)
	fmt.Println("Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	afterComplete := time.Now().Unix() - 3
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, writeCutOverValidation, afterComplete, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, writeCutOverValidation, afterComplete, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, writeCutOverValidation, afterComplete, 2, 1)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 50, t)

	util.CT.StopClientTraffic(respChan)
}
