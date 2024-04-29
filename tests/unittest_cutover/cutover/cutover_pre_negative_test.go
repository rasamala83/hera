package main

import (
	"fmt"
	"github.com/paypal/hera/tests/util"
	"sync"
	"testing"
	"time"
)

/*
TEST:
 1. Before entering pre cut-over all traffic should go to primary database
 2. Primary database should have 25 valid connections and cut-over database should have 0
 3. ENABLE CUT-OVER, then move to PRE CUT-OVER PHASE
 4. Shutdown primary service in primary database and make sure cut-over database service is up and running

VALIDATE
 1. No connection for primary database
 2. 25 connection to cut-over database
 3. occ is up and running with ORA error for primary database

OCC should exit - but as of now it is not
TODO: Need to add logs and CAL log verification
*/
func TestCutOverPreSourceDBDown(t *testing.T) {
	util.InitialSetup(t)

	var wg sync.WaitGroup
	respChan, dumpChan := util.CT.SendClientTraffic(&wg)
	beforeStart := time.Now().Unix() + 2
	fmt.Println("Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)
	afterComplete := time.Now().Unix() - 3
	trafficStats := util.CT.DumpTrafficStat(dumpChan)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, beforeStart, afterComplete, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, beforeStart, afterComplete, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, beforeStart, afterComplete, 1, 2)

	stateLog := make(map[string]int)
	stateLog["occ"] = 25
	util.ValidateStateLog(t, stateLog, true)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 0, t)

	// enable cut over env and tns changes
	util.EnableCutOver(t, false, false)
	util.MoveCutOverPhase(t, util.CreateTable, "TestCutOverPreSourceDBDown", true, true)
	util.MoveCutOverPhase(t, util.CutOverEnable, "TestCutOverPreSourceDBDown", true, true)
	util.RestartOCC(t)

	fmt.Println("Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 1, t)

	stateLog["occ"] = 25
	stateLog["occ.co"] = 1
	util.ValidateStateLog(t, stateLog, true)

	startClientTraffic := time.Now().Unix()
	fmt.Printf("Moving from Enable to Pre Cutover state: %d\n", startClientTraffic)
	util.MoveCutOverPhase(t, util.CutOverPre, "TestCutOverPreSourceDBDown", true, true)
	fmt.Println("Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 25, t)
	beforeSourceDBGoesDown := time.Now().Unix()
	trafficStats = util.CT.DumpTrafficStat(dumpChan)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, beforeSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, beforeSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, beforeSourceDBGoesDown-3, 1, 2)

	fmt.Println("Shutting down source database")
	util.ShutDownDBService("HERADB_ONE", "herabox_primary_srv", t)
	fmt.Println("Sleeping for 20 seconds")
	shutdownPrimary := time.Now().Unix()
	time.Sleep(20 * time.Second)
	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}
	afterSourceDBGoesDown := time.Now().Unix()

	trafficStats = util.CT.DumpTrafficStat(dumpChan)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, shutdownPrimary, afterSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, shutdownPrimary, afterSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, shutdownPrimary, afterSourceDBGoesDown-3, 1, 2)

	util.KillSessions(t, false, "herabox_primary_srv")
	fmt.Println("Sleeping for 15 seconds")
	afterKill := time.Now().Unix()
	time.Sleep(15 * time.Second)

	trafficStats = util.CT.StopClientTraffic(respChan)

	util.ValidateFailureTraffic(t, trafficStats, util.READ, afterKill+3, afterSourceDBGoesDown-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterKill+3, afterSourceDBGoesDown-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterKill+3, afterSourceDBGoesDown-3)
}

/*
TEST:
 1. Before entering pre cut-over all traffic should go to primary database
 2. Primary database should have 25 valid connections and cut-over database should have 0
 3. ENABLE CUT-OVER, then move to PRE CUT-OVER PHASE
 4. Shutdown primary service in target database and make sure cut-over database service is up and running

VALIDATE
 1. No connection for primary database
 2. 0 connection to cut-over database
 3. occ is up and running with ORA error for primary database

OCC should exit - but as of now it is not
TODO: Need to add logs and CAL log verification
*/
func TestCutOverPreTargetDBDown(t *testing.T) {
	util.InitialSetup(t)

	var wg sync.WaitGroup

	fmt.Println("Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	stateLog := make(map[string]int)
	stateLog["occ"] = 25
	util.ValidateStateLog(t, stateLog, true)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 0, t)

	// enable cut over env and tns changes
	util.EnableCutOver(t, false, false)
	util.MoveCutOverPhase(t, util.CreateTable, "TestCutOverPreSourceDBDown", true, true)
	util.MoveCutOverPhase(t, util.CutOverEnable, "TestCutOverPreSourceDBDown", true, true)
	util.RestartOCC(t)

	fmt.Println("Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 1, t)

	stateLog["occ"] = 25
	stateLog["occ.co"] = 1
	util.ValidateStateLog(t, stateLog, true)
	respChan, dumpChan := util.CT.SendClientTraffic(&wg)
	fmt.Println("Sleeping for 5 seconds")
	time.Sleep(5 * time.Second)
	startClientTraffic := time.Now().Unix()
	fmt.Printf("Moving from Enable to Pre Cutover state: %d\n", startClientTraffic)
	util.MoveCutOverPhase(t, util.CutOverPre, "TestCutOverPreSourceDBDown", true, true)
	fmt.Println("Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 25, t)
	beforeSourceDBGoesDown := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, beforeSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, beforeSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, beforeSourceDBGoesDown-3, 1, 2)

	fmt.Println("Shutting down target database")
	util.ShutDownDBService("HERADB_TWO", "herabox_secondary_srv", t)
	fmt.Println("Sleeping for 20 seconds")
	shutdownPrimary := time.Now().Unix()
	time.Sleep(20 * time.Second)
	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}
	afterSourceDBGoesDown := time.Now().Unix()

	trafficStats = util.CT.DumpTrafficStat(dumpChan)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, shutdownPrimary, afterSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, shutdownPrimary, afterSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, shutdownPrimary, afterSourceDBGoesDown-3, 1, 2)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)

	beforeKill := time.Now().Unix()

	util.KillSessions(t, true, "herabox_secondary_srv")
	fmt.Println("Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	trafficStats = util.CT.StopClientTraffic(respChan)

	stateLog["occ"] = 25
	stateLog["occ.co"] = 0
	cnt := 0
	for {
		if util.ValidateStateLog(t, stateLog, false) || cnt > 5 {
			break
		}
		util.KillSessions(t, true, "herabox_secondary_srv")
		fmt.Println("Sleeping for 15 seconds")
		time.Sleep(15 * time.Second)
		cnt += 1
	}
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", -1, t)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, afterSourceDBGoesDown, beforeKill-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, afterSourceDBGoesDown, beforeKill-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, afterSourceDBGoesDown, beforeKill-3, 1, 2)

	fmt.Println("Starting down target database")
	util.StartDBService("HERADB_TWO", "herabox_secondary_srv", t)
	fmt.Println("Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 25, t)

}
