package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/paypal/hera/client/gosqldriver"
	"github.com/paypal/hera/tests/util"
	"sync"
	"testing"
	"time"
)

func TestCutOverPositive(t *testing.T) {

	// bring the setup to initial state
	//OCC running with only one db in TNS, no env set for cut over, cut over table is empty
	util.InitialSetup(t)

	// enable cut over env and tns changes
	util.EnableCutOver(t, false, false)
	util.MoveCutOverPhase(t, util.CreateTable, "TestCutOverEnable", true, true)
	util.MoveCutOverPhase(t, util.CutOverEnable, "TestCutOverEnable", true, true)
	util.RestartOCC(t)

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
	util.MoveCutOverPhase(t, util.CutOverPre, "TestCutOverPositive-Init", true, true)
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
	util.MoveCutOverPhase(t, util.CutOverPhaseI, "TestCutOverPositive-StopWrite", true, true)
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
	util.MoveCutOverPhase(t, util.CutOverPhaseII, "TestCutOverPositive-CutOverRead", true, true)
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
	util.MoveCutOverPhase(t, util.CutOverPhaseIII, "TestCutOverPositive-CutOverWrite", true, true)
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
	util.MoveCutOverPhase(t, util.CutOverComplete, "TestCutOverPositive-CutOverComplete", true, true)
	fmt.Println("Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	trafficStats = util.CT.DumpTrafficStat(dumpChan)
	afterComplete := time.Now().Unix() - 3
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, writeCutOverValidation, afterComplete, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, writeCutOverValidation, afterComplete, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, writeCutOverValidation, afterComplete, 2, 1)
	stateLog["occ"] = 1
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 1, t)

	fmt.Printf("Moving to Broom State: %d\n", time.Now().Unix())
	util.MoveCutOverPhase(t, util.CutOverBroom, "TestCutOverPositive-CutOverBroom", true, true)
	fmt.Println("Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	trafficStats = util.CT.DumpTrafficStat(dumpChan)
	afterBroom := time.Now().Unix() - 3
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, afterComplete, afterBroom, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, afterComplete, afterBroom, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, afterComplete, afterBroom, 2, 1)
	stateLog["occ"] = 1
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 1, t)

	util.CT.StopClientTraffic(respChan)
}
