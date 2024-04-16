package main

import (
	"fmt"
	"github.com/paypal/hera/tests/util"
	"sync"
	"testing"
	"time"
)

/*
ENABLE CUT-OVER
  1 Stop occ docker
  2 Push tns with two task for cut-over db
  3 Set env variable TWO_TASK_CUTOVER with the proper two task key
  4 Start occ docker
*/

func TestCutOverDisabled(t *testing.T) {
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
	util.ValidateStateLog(t, stateLog)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 0, t)

	util.CT.StopClientTraffic(respChan)
}

/*
TEST:
 1. Before enabling cut-over all traffic should go to primary database
 2. Primary database should have 25 valid connections and cut-over database should have 0
 3. ENABLE CUT-OVER
 4. Shutdown primary service in primary database and make sure cut-over database service is up and running
 5. Create cut-over management table and insert records for cut-over enabled phase
 6. Restart OCC (inside container by issuing SIGHUP)

VALIDATE
 1. No connection for primary database
 2. 12 connection to cut-over database
 3. occ is up and running with ORA error for primary database

OCC should exit - but as of now it is not
TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledPrimaryDBDown(t *testing.T) {
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
	util.ValidateStateLog(t, stateLog)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 0, t)

	util.ShutDownDBService("HERADB_ONE", "herabox_primary_srv", t)

	// enable cut over env and tns changes
	util.EnableCutOver(t, false, false)
	util.MoveCutOverPhase(t, util.CreateTable, "TestCutOverEnabledPrimaryDBDown", true, true)
	util.MoveCutOverPhase(t, util.CutOverEnable, "TestCutOverEnabledPrimaryDBDown", true, true)
	util.RestartOCC(t)

	fmt.Println("Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", -1, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 12, t)

	stateLog["occ.co"] = 12
	util.ValidateStateLog(t, stateLog)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}

	util.CT.StopClientTraffic(respChan)
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase
3. Delete the table only in primary database
4. Restart OCC (SIGHUP)
5. Wait for 30 seconds before starting validation

VALIDATION
1. OCC should not be up and running

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledTableMissingPrimary(t *testing.T) {
	util.InitialSetup(t)

	var wg sync.WaitGroup
	respChan, _ := util.CT.SendClientTraffic(&wg)
	util.EnableCutOver(t, false, false)
	util.MoveCutOverPhase(t, util.CreateTable, "TestCutOverEnabledTableMissingPrimary", true, true)
	util.MoveCutOverPhase(t, util.CutOverEnable, "TestCutOverEnabledTableMissingPrimary", true, true)
	util.MoveCutOverPhase(t, util.DeleteCutOverTable, "TestCutOverEnabledTableMissingPrimary", true, false)
	util.RestartOCC(t)
	fmt.Println("Sleeping for 30 seconds")
	time.Sleep(30 * time.Second)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}

	util.CT.StopClientTraffic(respChan)
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table
3. Insert only one row into both the database (actually code should expect two rows)
4. Restart OCC (SIGHUP)
5. Wait for 30 seconds before starting validation

VALIDATION
1. OCC should not be up and running
TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledInvalidNumOfRows(t *testing.T) {
	util.InitialSetup(t)

	var wg sync.WaitGroup
	respChan, _ := util.CT.SendClientTraffic(&wg)
	util.EnableCutOver(t, false, false)
	util.MoveCutOverPhase(t, util.CreateTable, "TestCutOverEnabledInvalidNumOfRows", true, true)
	util.MoveCutOverPhase(t, util.CutOverEnable, "TestCutOverEnabledInvalidNumOfRows", true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableInvalidNumRows, "TestCutOverEnabledTableMissingPrimary", true, false)
	util.RestartOCC(t)
	fmt.Println("Sleeping for 30 seconds")
	time.Sleep(30 * time.Second)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}

	util.CT.StopClientTraffic(respChan)
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase
3. Enable sharing by making sure management table has right entries and the cdb values are set for sharding
4. Restart OCC (SIGHUP)
5. Wait for 30 seconds before starting validation

VALIDATION
1. OCC should ignore cut-over flags and continue to serve shard traffic
2. Validate state logs for 25 workers in each shard
3. Validate database connections as 25 for each shard

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledShardedDataBase(t *testing.T) {
	util.InitialSetup(t)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, "TestCutOverEnabledShardedDataBase", true, true)
	util.MoveCutOverPhase(t, util.CutOverEnable, "TestCutOverEnabledShardedDataBase", true, true)
	util.EnableSharding(t)
	util.RestartOCC(t)
	fmt.Println("Sleeping for 30 seconds")
	time.Sleep(30 * time.Second)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC should be up and running")
	}

	stateLog := make(map[string]int)
	stateLog["occ.sh0"] = 25
	stateLog["occ.sh1"] = 25
	util.ValidateStateLog(t, stateLog)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 25, t)

}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase with incorrect occ name
3. Restart OCC (SIGHUP)
4. Wait for 40 seconds before starting validation

VALIDATION
1. OCC should not be up and running

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledWrongOCCName(t *testing.T) {
	util.InitialSetup(t)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, "TestCutOverEnabledWrongOCCName", true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableWrongOCC, "TestCutOverEnabledWrongOCCName", true, true)
	util.RestartOCC(t)
	fmt.Println("Sleeping for 40 seconds")
	time.Sleep(40 * time.Second)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase with incorrect db unique name
3. Restart OCC (SIGHUP)
4. Wait for 40 seconds before starting validation

VALIDATION
1. OCC should be up and running
2. Validate state logs for 25 connections to primary database and 1 for cut-over database
3. Validate the same in database connections

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledInvalidUniqueID(t *testing.T) {
	util.InitialSetup(t)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, "TestCutOverEnabledInvalidUniqueID", true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableInvalidUniqueName, "TestCutOverEnabledInvalidUniqueID", true, true)
	util.RestartOCC(t)
	fmt.Println("Sleeping for 40 seconds")
	time.Sleep(40 * time.Second)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC should be up but looks like it is down")
	}
	stateLog := make(map[string]int)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 1
	util.ValidateStateLog(t, stateLog)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 1, t)

}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase with incorrect two task key
3. Restart OCC (SIGHUP)
4. Wait for 40 seconds before starting validation

VALIDATION
1. OCC should not be up and running

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledInvalidTNS(t *testing.T) {
	util.InitialSetup(t)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, "TestCutOverEnabledInvalidTNS", true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableInvalidTNS, "TestCutOverEnabledInvalidTNS", true, true)
	util.RestartOCC(t)
	fmt.Println("Sleeping for 40 seconds")
	time.Sleep(40 * time.Second)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase with incorrect cut-over phase
3. Restart OCC (SIGHUP)
4. Wait for 40 seconds before starting validation

VALIDATION
1. OCC should not be up and running

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledInvalidPhase(t *testing.T) {
	util.InitialSetup(t)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, "TestCutOverEnabledInvalidPhase", true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableInvalidPhase, "TestCutOverEnabledInvalidPhase", true, true)
	util.RestartOCC(t)
	fmt.Println("Sleeping for 40 seconds")
	time.Sleep(40 * time.Second)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase with incorrect read status (other than 'Y'/'N')
3. Restart OCC (SIGHUP)
4. Wait for 40 seconds before starting validation

VALIDATION
1. OCC should not be up and running

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledInvalidRead(t *testing.T) {
	util.InitialSetup(t)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, "TestCutOverEnabledInvalidRead", true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableInvalidRead, "TestCutOverEnabledInvalidRead", true, true)
	util.RestartOCC(t)
	fmt.Println("Sleeping for 40 seconds")
	time.Sleep(40 * time.Second)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase with incorrect write status (other than 'Y'/'N')
3. Restart OCC (SIGHUP)
4. Wait for 40 seconds before starting validation

VALIDATION
1. OCC should not be up and running

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledInvalidWrite(t *testing.T) {
	util.InitialSetup(t)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, "TestCutOverEnabledInvalidWrite", true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableInvalidWrite, "TestCutOverEnabledInvalidWrite", true, true)
	util.RestartOCC(t)
	fmt.Println("Sleeping for 40 seconds")
	time.Sleep(40 * time.Second)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase with write enable but not read
3. Restart OCC (SIGHUP)
4. Wait for 40 seconds before starting validation

VALIDATION
1. OCC should be up and running
2. Write and Transaction Traffic should be success and go to the configured database
3. Read traffic should fail
4. Validate state logs for 25 connections to main database and 1 connection to cut-over database
5. validate the database connections for the same

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledWriteEnabledButNotRead(t *testing.T) {
	util.InitialSetup(t)
	var wg sync.WaitGroup
	respChan, dumpChan := util.CT.SendClientTraffic(&wg)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, "TestCutOverEnabledWriteEnabledButNotRead", true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableWriteNoRead, "TestCutOverEnabledWriteEnabledButNotRead", true, true)
	util.RestartOCC(t)
	fmt.Println("Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)
	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC should be up - which is not expected")
	}
	start := time.Now().Unix() + 2
	fmt.Println("Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)
	trafficStats := util.CT.DumpTrafficStat(dumpChan)
	afterComplete := time.Now().Unix() - 3
	util.CT.StopClientTraffic(respChan)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, start, afterComplete, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, start, afterComplete, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.READ, start, afterComplete)

	stateLog := make(map[string]int)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 1
	util.ValidateStateLog(t, stateLog)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", 1, t)
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase with write enabled for both database
3. Restart OCC (SIGHUP)
4. Wait for 40 seconds before starting validation

VALIDATION
1. OCC should not be up and running

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledDualWrite(t *testing.T) {
	util.InitialSetup(t)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, "TestCutOverEnabledDualWrite", true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableDualWrite, "TestCutOverEnabledDualWrite", true, true)
	util.RestartOCC(t)
	fmt.Println("Sleeping for 40 seconds")
	time.Sleep(40 * time.Second)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}
}

/*
TEST
1. ENABLE CUT-OVER
2. Create cut-over management table and insert records for cut-over enabled phase with read enabled for both database
3. Restart OCC (SIGHUP)
4. Wait for 40 seconds before starting validation

VALIDATION
1. OCC should not be up and running

TODO: Need to add logs and CAL log verification
*/
func TestCutOverEnabledDualRead(t *testing.T) {
	util.InitialSetup(t)

	util.EnableCutOver(t, false, true)
	util.MoveCutOverPhase(t, util.CreateTable, "TestCutOverEnabledDualRead", true, true)
	util.MoveCutOverPhase(t, util.CutOverEnableDualRead, "TestCutOverEnabledDualRead", true, true)
	util.RestartOCC(t)
	fmt.Println("Sleeping for 40 seconds")
	time.Sleep(40 * time.Second)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}
}
