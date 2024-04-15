package main

import (
	"fmt"
	"github.com/paypal/hera/tests/util"
	"sync"
	"testing"
	"time"
)

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

// occ container goes down in 10 seconds
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

}

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
