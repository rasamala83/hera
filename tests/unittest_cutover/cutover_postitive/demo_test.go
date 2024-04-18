package main

import (
	"fmt"
	"github.com/paypal/hera/tests/util"
	"github.com/paypal/hera/utility/logger"
	"sync"
	"testing"
	"time"
)

func TestCutOverDemo(t *testing.T) {
	// bring the setup to initial state
	//OCC running with only one db in TNS, no env set for cut over, cut over table is empty
	logger.CreateLogger("log.txt", "UT", logger.Alert, false)

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
	util.CT.DumpStats(trafficStats)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, beforeCutOverStart-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, beforeCutOverStart-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, beforeCutOverStart-3, 1, 2)

	count := 0
	for {
		trafficStats = util.CT.DumpTrafficStat(dumpChan)
		util.CT.DumpStats(trafficStats)
		_, activeResponse := util.GetDBStatus()
		fmt.Println(activeResponse)
		fmt.Println("Sleeping for 10 seconds")
		time.Sleep(10 * time.Second)
		count += 1
		if count >= 120 {
			break
		}
	}
	util.CT.StopClientTraffic(respChan)
}
