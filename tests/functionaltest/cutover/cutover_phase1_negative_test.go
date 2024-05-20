package main

import (
	"github.com/paypal/hera/tests/util"
	logger2 "github.com/paypal/hera/utility/logger"
	"os"
	"sync"
	"testing"
	"time"
)

func moveToCutOverPhase(t *testing.T) (chan map[int64]util.ClientTrafficStats, chan map[int64]util.ClientTrafficStats, chan string, *os.File) {
	_, logFile := util.Setup(t)

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
	respChan, dumpChan, RunMsg := util.CT.SendClientTraffic(&wg)

	beforeStart := time.Now().Unix() + 2
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)
	afterComplete := time.Now().Unix() - 3
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RunMsg)

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
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RunMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, cutOverPreState-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, cutOverPreState-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, cutOverPreState-3, 1, 2)

	return dumpChan, respChan, RunMsg, logFile
}

/*
TEST:
 1. When entering cutover phase I make sure there is only one row returned from metadata table

VALIDATE
 1. occ should stay in cutover phase
 2. on session kill occ should continue to run
 3. on restart occ should exit

TODO: Need to add logs and CAL log verification
*/
func TestCutOver1InvalidNoOfRow(t *testing.T) {
	dumpChan, respChan, RunMsg, logFile := moveToCutOverPhase(t)
	defer util.TearDown(t, respChan, dumpChan, RunMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIInvalidRowCount, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RunMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseI, invalidPhaseI-3, 1, 2)

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
	trafficStats = util.CT.StopClientTraffic(respChan, RunMsg)

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
 1. When entering cutover phase I make sure one or moe row has invalid uniq name

VALIDATE
 1. occ should stay in cutover phase
 2. on session kill occ should continue to run
 3. on restart occ should exit

TODO: Need to add logs and CAL log verification
*/
func TestCutOver1InvalidUniqName(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhase(t)
	defer util.TearDown(t, respChan, dumpChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIInvalidUniqName, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseI, invalidPhaseI-3, 1, 2)

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

/*
TEST:
 1. When entering cutover phase make sure one or more of the record has invalid occ name

VALIDATE
 1. occ should stay in cutover phase
 2. on session kill occ should continue to run
 3. on restart occ should exit

TODO: Need to add logs and CAL log verification
*/
func TestCutOver1InvalidTwoTask(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhase(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIInvalidTwoTask, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseI, invalidPhaseI-3, 1, 2)

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

/*
TEST:
 1. When entering cutover phase I make sure occ name is invalid

VALIDATE
 1. occ should stay in cutover phase
 2. on session kill occ should continue to run
 3. on restart occ should exit

TODO: Need to add logs and CAL log verification
*/
func TestCutOver1InvalidOCCName(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhase(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIInvalidOCCName, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseI, invalidPhaseI-3, 1, 2)

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

/*
TEST:
 1. When entering cutover phase and update phase with invalid data

VALIDATE
 1. occ should stay in cutover phase
 2. on session kill occ should continue to run
 3. on restart occ should exit

TODO: Need to add logs and CAL log verification
*/
func TestCutOver1InvalidPhase(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhase(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIInvalidPhase, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseI, invalidPhaseI-3, 1, 2)

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

/*
TEST:
 1. When entering cutover phase and update write status as 'x'

VALIDATE
 1. occ should stay in cutover phase
 2. on session kill occ should continue to run
 3. on restart occ should exit

TODO: Need to add logs and CAL log verification
*/
func TestCutOver1InvalidWriteStatus(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhase(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIInvalidWriteStatus, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseI, invalidPhaseI-3, 1, 2)

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

/*
TEST:
 1. When entering cutover phase and update read status as 'x'

VALIDATE
 1. occ should stay in cutover phase
 2. on session kill occ should continue to run
 3. on restart occ should exit

TODO: Need to add logs and CAL log verification
*/
func TestCutOver1InvalidReadStatus(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhase(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIInvalidReadStatus, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseI, invalidPhaseI-3, 1, 2)

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

/*
TEST:
 1. When entering cutover phase and update write status as 'Y' for both the DB

VALIDATE
 1. occ should stay in cutover phase
 2. on session kill occ should continue to run
 3. on restart occ should exit

TODO: Need to add logs and CAL log verification
*/
func TestCutOver1DualWrite(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhase(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIDualWrite, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseI, invalidPhaseI-3, 1, 2)

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

/*
TEST:
 1. When entering cutover phase and update read status as 'Y' for both the DB

VALIDATE
 1. occ should stay in cutover phase
 2. on session kill occ should continue to run
 3. on restart occ should exit

TODO: Need to add logs and CAL log verification
*/
func TestCutOver1DualRead(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhase(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIDualRead, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseI, invalidPhaseI-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseI, invalidPhaseI-3, 1, 2)

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

/*
TEST:
 1. When entering cutover phase and update read status as 'N' for both the DB

VALIDATE
 1. occ should stay in cutover phase
 2. on session kill occ should continue to run
 3. on restart occ should exit

TODO: Need to add logs and CAL log verification
*/
func TestCutOver1ReadOff(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhase(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	startPhaseI := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", startPhaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseIReadOff, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseI := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateFailureTraffic(t, trafficStats, util.READ, startPhaseI+3, invalidPhaseI-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, startPhaseI+3, invalidPhaseI-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, startPhaseI+3, invalidPhaseI-3)

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

/*
TEST:
 1. enter cutover phase and then shutdown target DB, test by killing the sessions and  then restarting occ

VALIDATE
 1. occ should stay in cutover phase
 2. on session kill occ should continue to run
 3. on restart occ should continue to run

TODO: Need to add logs and CAL log verification
*/
func TestCutOver1TargetDBDown(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhase(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	start := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", start)
	util.MoveCutOverPhase(t, util.CutOverPhaseI, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	end := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, start+3, end-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, start+3, end-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, start+3, end-3)

	util.ShutDownDBService("HERADB_TWO", "herabox_secondary_srv", t)
	start = time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	end = time.Now().Unix()
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, start+3, end-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, start+3, end-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, start+3, end-3)

	stateLog["occ"] = 25
	stateLog["occ.co"] = 0

	util.KillSessionAndValidate(t, stateLog, "herabox_secondary_srv", true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	start = time.Now().Unix()
	time.Sleep(25 * time.Second)
	end = time.Now().Unix()
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, start+3, end-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, start+3, end-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, start+3, end-3)

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

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, afterRestart+3, trafficStopped-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterRestart+3, trafficStopped-3)

}

/*
TEST:
 1. enter cutover phase and then shutdown source DB, test by killing the sessions and  then restarting occ

VALIDATE
 1. occ should stay in cutover phase
 2. on session kill occ should continue to run
 3. on restart occ should exit

TODO: Need to add logs and CAL log verification
*/
func TestCutOver1SourceDBDown(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhase(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	start := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", start)
	util.MoveCutOverPhase(t, util.CutOverPhaseI, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	end := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, start+3, end-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, start+3, end-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, start+3, end-3)

	util.ShutDownDBService("HERADB_ONE", "herabox_primary_srv", t)
	start = time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", false, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	end = time.Now().Unix()
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, start+3, end-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, start+3, end-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, start+3, end-3)

	stateLog["occ"] = 0
	stateLog["occ.co"] = 25

	util.KillSessionAndValidate(t, stateLog, "herabox_primary_srv", false)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	start = time.Now().Unix()
	time.Sleep(25 * time.Second)
	end = time.Now().Unix()
	util.ValidateFailureTraffic(t, trafficStats, util.READ, start+3, end-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, start+3, end-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, start+3, end-3)

	util.RestartOCC(t)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	afterRestart := time.Now().Unix()
	time.Sleep(25 * time.Second)

	trafficStopped := time.Now().Unix()
	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}

	util.ValidateFailureTraffic(t, trafficStats, util.READ, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterRestart+3, trafficStopped-3)

}

/*
TEST:
 1. enter cutover phase 1
 2. rollback the whole change

VALIDATE
 1. occ cutover and roll back stages

TODO: Need to add logs and CAL log verification
*/
func TestCutOver1Rollback(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhase(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	start := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", start)
	util.MoveCutOverPhase(t, util.CutOverPhaseI, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	end := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, start+3, end-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, start+3, end-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, start+3, end-3)

	util.MoveCutOverPhase(t, util.CutOverPre, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	start = time.Now().Unix()
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	end = time.Now().Unix()

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, start+3, end-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, start+3, end-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, start+3, end-3, 1, 2)

	util.MoveCutOverPhase(t, util.CutOverEnable, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	start = time.Now().Unix()
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 1
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 1, t)
	end = time.Now().Unix()

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, start+3, end-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, start+3, end-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, start+3, end-3, 1, 2)

	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)

	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}

}
