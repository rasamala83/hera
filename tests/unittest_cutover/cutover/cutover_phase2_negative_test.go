package main

import (
	"github.com/paypal/hera/tests/util"
	logger2 "github.com/paypal/hera/utility/logger"
	"sync"
	"testing"
	"time"
)

func moveToCutOverPhaseI(t *testing.T) (chan map[int64]util.ClientTrafficStats, chan map[int64]util.ClientTrafficStats, chan string) {
	util.InitialSetup(t)

	stateLog := make(map[string]int)
	stateLog["occ"] = 25
	util.ValidateStateLog(t, stateLog, true)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 0, t)

	// enable cut over env and tns changes
	util.EnableCutOver(t, false, false)
	util.MoveCutOverPhase(t, util.CreateTable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnable, true, true)
	util.RestartOCC(t)

	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 1, t)

	stateLog["occ"] = 25
	stateLog["occ.co"] = 1
	util.ValidateStateLog(t, stateLog, true)

	var wg sync.WaitGroup
	respChan, dumpChan, RespMsg := util.CT.SendClientTraffic(&wg)

	beforeStart := time.Now().Unix() + 2
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)
	afterComplete := time.Now().Unix() - 3
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, beforeStart, afterComplete, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, beforeStart, afterComplete, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, beforeStart, afterComplete, 1, 2)

	startClientTraffic := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Pre Cutover state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.CutOverPre, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	cutOverPreState := time.Now().Unix()
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, cutOverPreState-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, cutOverPreState-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, cutOverPreState-3, 1, 2)

	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to cut over phase 1 state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.CutOverPhaseI, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	cutOverPhase1State := time.Now().Unix()
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	cutOverPhase1End := time.Now().Unix()
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, cutOverPhase1State+3, cutOverPhase1End-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, cutOverPhase1State, cutOverPhase1End-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, cutOverPhase1State, cutOverPhase1End-3)

	return dumpChan, respChan, RespMsg
}

/*
TEST:
 1. When entering cutover phase II make sure there is only one row returned from metadata table

VALIDATE
 1. occ should stay in cutover phase
 2. on session kill occ should continue to run
 3. on restart occ should exit

TODO: Need to add logs and CAL log verification
*/
func TestCutOver2InvalidNoOfRow(t *testing.T) {
	dumpChan, respChan, RespChan := moveToCutOverPhaseI(t)
	defer util.CT.TearDown(dumpChan, respChan, RespChan)

	stateLog := make(map[string]int)

	startPhaseII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIIInvalidRowCount, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespChan)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseII, invalidPhaseII-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseII, invalidPhaseII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseII, invalidPhaseII-3)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)

	util.RestartOCC(t)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	afterRestart := time.Now().Unix()
	time.Sleep(25 * time.Second)

	trafficStopped := time.Now().Unix()
	trafficStats = util.CT.StopClientTraffic(respChan, RespChan)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}

	util.ValidateFailureTraffic(t, trafficStats, util.READ, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterRestart+3, trafficStopped-3)
}

/*
TEST:
 1. When entering cutover phase II make sure source DB unique name is wrong/invalid

VALIDATE
 1. occ should stay in cutover phase
 2. on session kill occ should continue to run
 3. on restart occ should exit

TODO: Need to add logs and CAL log verification
*/
func TestCutOver2InvalidDBUniqueName(t *testing.T) {
	dumpChan, respChan, RespMsg := moveToCutOverPhaseI(t)
	defer util.CT.TearDown(dumpChan, respChan, RespMsg)

	stateLog := make(map[string]int)

	startPhaseII := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIIInvalidDBUniqName, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseII, invalidPhaseII-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseII, invalidPhaseII-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseII, invalidPhaseII-3)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)

	util.RestartOCC(t)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	afterRestart := time.Now().Unix()
	time.Sleep(25 * time.Second)

	trafficStopped := time.Now().Unix()
	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus == true {
		t.Fatalf("OCC is up - which is not expected")
	}

	util.ValidateFailureTraffic(t, trafficStats, util.READ, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterRestart+3, trafficStopped-3)
}
