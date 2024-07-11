package main

import (
	"github.com/paypal/hera/tests/util"
	logger2 "github.com/paypal/hera/utility/logger"
	"os"
	"sync"
	"testing"
	"time"
)

func moveToCutOverPhaseIII(t *testing.T) (chan map[int64]util.ClientTrafficStats, chan map[int64]util.ClientTrafficStats, chan string, *os.File) {
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
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic+3, cutOverPreState-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic+3, cutOverPreState-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic+3, cutOverPreState-3, 1, 2)

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
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, cutOverPhase1State+3, cutOverPhase1End-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, cutOverPhase1State+3, cutOverPhase1End-3)

	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to cut over phase 2 state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.CutOverPhaseII, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	cutOverPhase2State := time.Now().Unix()
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	cutOverPhase2End := time.Now().Unix()
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, cutOverPhase2State+3, cutOverPhase2End-3, 2, 1)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, cutOverPhase2State+3, cutOverPhase2End-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, cutOverPhase2State+3, cutOverPhase2End-3)

	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to cut over phase 3 state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.CutOverPhaseIII, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	cutOverPhase3State := time.Now().Unix()
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	cutOverPhase3End := time.Now().Unix()
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, cutOverPhase3State+3, cutOverPhase3End-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, cutOverPhase3State+3, cutOverPhase3End-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, cutOverPhase3State+3, cutOverPhase3End-3, 2, 1)

	return dumpChan, respChan, RespMsg, logFile
}

/*
PRE-SETUP
-------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | CUTOVER |
-------------------------------------------------------------------------------
*********************************
TestCutOverCompleteInvalidNoOfRow
*********************************
-------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase    |
| Row1 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | COMPLETE |
-------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE    | active |
    | WRITE        |  HERADB_TWO | HERADB_ONE    | active |
    | TXN          |  HERADB_TWO | HERADB_ONE    | active |
    -------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be down
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOverCompleteInvalidNoOfRow(t *testing.T) {
	dumpChan, respChan, RespChan, logFile := moveToCutOverPhaseIII(t)
	defer util.TearDown(t, dumpChan, respChan, RespChan, logFile)

	stateLog := make(map[string]int)

	completePhase := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Complete Phase: ", completePhase)
	util.MoveCutOverPhase(t, util.CutOverCompletePhaseInvalidRowCount, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseIII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespChan)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, completePhase, invalidPhaseIII-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, completePhase, invalidPhaseIII-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, completePhase, invalidPhaseIII-3, 2, 1)

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
PRE-SETUP
-------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | CUTOVER |
-------------------------------------------------------------------------------

**************************************
TestCutOverCompleteInvalidDBUniqueName
**************************************
----------------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname           | r_status | w_status | phase    |
----------------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE_INVALID | N        | N        | COMPLETE |
| Row1 | occ      | CLOC_CUTOVER | HERADB_TWO_INVALID | Y        | Y        | COMPLETE |
----------------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  1             | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  1              | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE    | active |
    | WRITE        |  HERADB_TWO | HERADB_ONE    | active |
    | TXN          |  HERADB_TWO | HERADB_ONE    | active |
    -------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  1             | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be up
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         | HERADB_TWO  | HERADB_ONE | active |
    | WRITE        | HERADB_TWO  | HERADB_ONE | active |
    | TXN          | HERADB_TWO  | HERADB_ONE | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOverCompleteInvalidDBUniqueName(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseIII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	completePhase := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Cutover III to Cutover Complete Phase: ", completePhase)
	util.MoveCutOverPhase(t, util.CutOverCompletePhaseInvalidDBUniqName, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 1
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 1, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseIII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, completePhase, invalidPhaseIII-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, completePhase, invalidPhaseIII-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, completePhase, invalidPhaseIII-3, 2, 1)

	logger2.GetLogger().Log(logger2.Alert, "Killing Sessions ", invalidPhaseIII)
	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	stateLog["occ"] = 1
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)

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

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, afterRestart+5, trafficStopped-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, afterRestart+5, trafficStopped-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, afterRestart+5, trafficStopped-3, 2, 1)
}

/*
PRE-SETUP
-------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | CUTOVER |
-------------------------------------------------------------------------------

**************************************
TestCutOverCompleteInvalidOCCName
**************************************
-----------------------------------------------------------------------------------
| ROWS | occ_name    | occ_two_task | db_uname   | r_status | w_status | phase    |
-----------------------------------------------------------------------------------
| Row1 | occ-invalid | CLOC         | HERADB_ONE | N        | N        | COMPLETE |
| Row1 | occ-invalid | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | COMPLETE |
-----------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE    | active |
    | WRITE        |  HERADB_TWO | HERADB_ONE    | active |
    | TXN          |  HERADB_TWO | HERADB_ONE    | active |
    -------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be down
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOverCompleteInvalidOCCName(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseIII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	completePhase := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Complete Phase : ", completePhase)
	util.MoveCutOverPhase(t, util.CutOverCompletePhaseInvalidOCCName, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhase := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, completePhase, invalidPhase-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, completePhase, invalidPhase-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, completePhase, invalidPhase-3, 2, 1)

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
PRE-SETUP
-------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | CUTOVER |
-------------------------------------------------------------------------------

**************************************
TestCutOverCompleteInvalidTwoTask
**************************************
---------------------------------------------------------------------------------------
| ROWS | occ_name    | occ_two_task     | db_uname   | r_status | w_status | phase    |
---------------------------------------------------------------------------------------
| Row1 | occ         | TWO_TASK_INVALID | HERADB_ONE | N        | N        | COMPLETE |
| Row1 | occ         | TWO_TASK_INVALID | HERADB_TWO | Y        | Y        | COMPLETE |
---------------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE    | active |
    | WRITE        |  HERADB_TWO | HERADB_ONE    | active |
    | TXN          |  HERADB_TWO | HERADB_ONE    | active |
    -------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be down
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOverCompleteInvalidTwoTask(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseIII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	completePhase := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Complete Phase: ", completePhase)
	util.MoveCutOverPhase(t, util.CutOverCompletePhaseInvalidTwoTask, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	invalidPhase := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, completePhase, invalidPhase-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, completePhase, invalidPhase-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, completePhase, invalidPhase-3, 2, 1)

	logger2.GetLogger().Log(logger2.Alert, "Killing Sessions ", invalidPhase)
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
PRE-SETUP
-------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | CUTOVER |
-------------------------------------------------------------------------------

**************************************
TestCutOverCompleteInvalidCutOverPhase
**************************************
-------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | invalid |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | invalid |
-------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE    | active |
    | WRITE        |  HERADB_TWO | HERADB_ONE    | active |
    | TXN          |  HERADB_TWO | HERADB_ONE    | active |
    -------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be down
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOverCompleteInvalidCutOverPhase(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseIII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	completePhase := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Complete Phase: ", completePhase)
	util.MoveCutOverPhase(t, util.CutOverCompletePhaseInvalidPhase, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhase := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, completePhase, invalidPhase-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, completePhase, invalidPhase-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, completePhase, invalidPhase-3, 2, 1)

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
PRE-SETUP
-------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | CUTOVER |
-------------------------------------------------------------------------------

**************************************
TestCutOverCutOverInvalidReadStatus
**************************************
-------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | X        | N        | COMPLETE |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | X        | Y        | COMPLETE |
-------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE    | active |
    | WRITE        |  HERADB_TWO | HERADB_ONE    | active |
    | TXN          |  HERADB_TWO | HERADB_ONE    | active |
    -------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be down
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOverCutOverInvalidReadStatus(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseIII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	completePhase := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to CutOver Complete Phase : ", completePhase)
	util.MoveCutOverPhase(t, util.CutOverCompletePhaseInvalidRead, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhase := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, completePhase+3, invalidPhase-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, completePhase+3, invalidPhase-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, completePhase+3, invalidPhase-3, 2, 1)

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
PRE-SETUP
-------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | CUTOVER |
-------------------------------------------------------------------------------

**************************************
TestCutOverCompleteInvalidWriteStatus
**************************************
--------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | X        | COMPLETE |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | X        | COMPLETE |
--------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE    | active |
    | WRITE        |  HERADB_TWO | HERADB_ONE    | active | TODO FAILING
    | TXN          |  HERADB_TWO | HERADB_ONE    | active |
    -------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be down
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOverCompleteInvalidWriteStatus(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseIII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	completePhase := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover complete Phase: ", completePhase)
	util.MoveCutOverPhase(t, util.CutOverCompletePhaseInvalidWrite, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhaseIII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, completePhase+5, invalidPhaseIII-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, completePhase+5, invalidPhaseIII-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, completePhase+5, invalidPhaseIII-3, 2, 1)

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
PRE-SETUP
-------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | CUTOVER |
-------------------------------------------------------------------------------

**************************************
TestCutOverCompleteDualWriteStatus
**************************************
-------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | Y        | COMPLETE |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | COMPLETE |
-------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE    | active |
    | WRITE        |  HERADB_TWO | HERADB_ONE    | active |
    | TXN          |  HERADB_TWO | HERADB_ONE    | active |
    -------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be down
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOverCompleteDualWriteStatus(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseIII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	completePhase := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover complete Phase : ", completePhase)
	util.MoveCutOverPhase(t, util.CutOverCompletePhaseDualWrite, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhase := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, completePhase, invalidPhase-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, completePhase, invalidPhase-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, completePhase, invalidPhase-3, 2, 1)

	util.KillSessions(t, true, "herabox_secondary_srv")
	util.KillSessions(t, false, "herabox_primary_srv")
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 60 seconds")
	time.Sleep(60 * time.Second)

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
PRE-SETUP
-------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | CUTOVER |
-------------------------------------------------------------------------------

**************************************
TestCutOverCompleteDualReadStatus
**************************************
-------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase    |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | COMPLETE |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | COMPLETE |
-------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE    | active |
    | WRITE        |  HERADB_TWO | HERADB_ONE    | active |
    | TXN          |  HERADB_TWO | HERADB_ONE    | active |
    -------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be down
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOverCompleteDualReadStatus(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseIII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	completePhase := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Complete Phase: ", completePhase)
	util.MoveCutOverPhase(t, util.CutOverCompletePhaseDualRead, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhase := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, completePhase, invalidPhase-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, completePhase, invalidPhase-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, completePhase, invalidPhase-3, 2, 1)

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
PRE-SETUP
-------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | CUTOVER |
-------------------------------------------------------------------------------

**************************************
TestCutOverCompleteReadOffStatus
**************************************
--------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | COMPLETE |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | N        | Y        | COMPLETE |
--------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    -----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB           | state  |
    -----------------------------------------------------------------
    | READ         |             | HERADB_TWO, HERADB_ONE  | active |
    | WRITE        |  HERADB_TWO | HERADB_ONE              | active | TODO FAILING
    | TXN          |  HERADB_TWO | HERADB_ONE              | active |
    -----------------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be down
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        | HERADB_TWO  | HERADB_ONE             | active |
    | TXN          | HERADB_TWO  | HERADB_ONE             | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOverCompleteReadOffStatus(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseIII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	completePhase := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Complete: ", completePhase)
	util.MoveCutOverPhase(t, util.CutOverCompletePhaseReadOff, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhase := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateFailureTraffic(t, trafficStats, util.READ, completePhase+3, invalidPhase-3)
	// We cannot validate only writes as our writes does read to identify the db
	//util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, completePhase+3, invalidPhase-3, 2, 1)
	//util.ValidateSuccessTraffic(t, trafficStats, util.TXN, completePhase+3, invalidPhase-3, 2, 1)

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
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}

	util.ValidateFailureTraffic(t, trafficStats, util.READ, afterRestart+3, trafficStopped-3)
}

/*
PRE-SETUP
-------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | CUTOVER |
-------------------------------------------------------------------------------

**************************************
TestCutOverCompleteSourceDBDown
**************************************
--------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | COMPLETE |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | COMPLETE |
--------------------------------------------------------------------------------

* Shutdown source DB - CLOC/HERADB_ONE

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  1             | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  1              | herabox_primary_srv   | down   |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    --------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB  | state  |
    --------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE     | active |
    | WRITE        |  HERADB_TWO | HERADB_ONE     | active |
    | TXN          |  HERADB_TWO | HERADB_ONE     | active |
    --------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  0             | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be down
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOverCompleteSourceDBDown(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseIII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	util.ShutDownDBService("HERADB_ONE", "herabox_primary_srv", t)
	stateLog := make(map[string]int)

	completePhase := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Complete Phase: ", completePhase)
	util.MoveCutOverPhase(t, util.CutOverComplete, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 1
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", false, 1, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	invalidPhase := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, completePhase+3, invalidPhase-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, completePhase+3, invalidPhase-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, completePhase+3, invalidPhase-3, 2, 1)

	stateLog["occ"] = 0
	stateLog["occ.co"] = 25
	util.KillSessionAndValidate(t, stateLog, "herabox_primary_srv", true)

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
PRE-SETUP
-------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | CUTOVER |
-------------------------------------------------------------------------------

**************************************
TestCutOverCompleteTargetDBDown
**************************************
--------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | COMPLETE |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | COMPLETE |
--------------------------------------------------------------------------------

* Taget source DB - CLOC/HERADB_ONE

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  1             | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  1              | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | down   |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    --------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB  | state  |
    --------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE     | down   |
    | WRITE        |  HERADB_TWO | HERADB_ONE     | down   |
    | TXN          |  HERADB_TWO | HERADB_ONE     | down   |
    --------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  1             | accept+wait+busy |
    | CLOC_CUTOVER |  0             | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be down
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | down  |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | down  |
    | TXN          |             | HERADB_ONE, HERADB_TWO | down  |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOverCompleteTargetDBDown(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseIII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	util.ShutDownDBService("HERADB_TWO", "herabox_secondary_srv", t)
	stateLog := make(map[string]int)

	completePhase := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Cutover  to Cutover Complete Phase: ", completePhase)
	util.MoveCutOverPhase(t, util.CutOverComplete, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 1
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 1, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", false, 25, t)
	invalidPhase := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, completePhase+3, invalidPhase-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, completePhase+3, invalidPhase-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, completePhase+3, invalidPhase-3, 2, 1)

	stateLog["occ"] = 1
	stateLog["occ.co"] = 0

	util.KillSessionAndValidate(t, stateLog, "herabox_secondary_srv", true)

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
PRE-SETUP
-------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase   |
-------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | CUTOVER |
-------------------------------------------------------------------------------

**************************************
TestCutOverCompleteRollback
**************************************
--------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | COMPLETE |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | COMPLETE |
--------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  1             | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  1              | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    --------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB  | state  |
    --------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE     | active |
    | WRITE        |  HERADB_TWO | HERADB_ONE     | active |
    | TXN          |  HERADB_TWO | HERADB_ONE     | active |
    --------------------------------------------------------

# ROLLBACK TO CUTOVER PHASE III

--------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | Y        | CUTOVER  |
--------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    --------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB  | state  |
    --------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE     | active |
    | WRITE        |  HERADB_TWO | HERADB_ONE     | active |
    | TXN          |  HERADB_TWO | HERADB_ONE     | active |
    --------------------------------------------------------

# ROLLBACK TO CUTOVER PHASE II

--------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | N        | N        | CUTOVER  |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | Y        | N        | CUTOVER  |
--------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    --------------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB              | state  |
    --------------------------------------------------------------------
    | READ         |  HERADB_TWO | HERADB_ONE                 | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO     | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO     | active |
    --------------------------------------------------------------------

# ROLLBACK TO CUTOVER PHASE I

--------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | N        | CUTOVER  |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | N        | N        | CUTOVER  |
--------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    --------------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB              | state  |
    --------------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO                 | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO     | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO     | active |
    --------------------------------------------------------------------

# ROLLBACK TO PRE

--------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | PRE      |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | N        | N        | PRE      |
--------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    --------------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB  | state  |
    --------------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO     | active |
    | WRITE        |  HERADB_ONE | HERADB_TWO     | active |
    | TXN          |  HERADB_ONE | HERADB_TWO     | active |
    --------------------------------------------------------------------

# ROLLBACK TO ENABLE

--------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase    |
--------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | ENABLE   |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | N        | N        | ENABLE   |
--------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  1             | accept+wait+busy |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  1              | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    --------------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB  | state  |
    --------------------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO     | active |
    | WRITE        |  HERADB_ONE | HERADB_TWO     | active |
    | TXN          |  HERADB_ONE | HERADB_TWO     | active |
    --------------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOverCompleteRollback(t *testing.T) {
	dumpChan, respChan, RespMsg, logFile := moveToCutOverPhaseIII(t)
	defer util.TearDown(t, dumpChan, respChan, RespMsg, logFile)

	stateLog := make(map[string]int)

	completePhase := time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Moving from Cutover to Cutover Complete Phase: ", completePhase)
	util.MoveCutOverPhase(t, util.CutOverComplete, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 1
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 1, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	startPhaseIII := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, completePhase+3, startPhaseIII-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, completePhase+3, startPhaseIII-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, completePhase+3, startPhaseIII-3, 2, 1)

	logger2.GetLogger().Log(logger2.Alert, "Moving from Cutover to Cutover phase III: ", startPhaseIII)
	util.MoveCutOverPhase(t, util.CutOverPhaseIII, true, true)
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	phaseII := time.Now().Unix()
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startPhaseIII+3, phaseII-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startPhaseIII+3, phaseII-3, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startPhaseIII+3, phaseII-3, 2, 1)

	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase II: ", phaseII)
	util.MoveCutOverPhase(t, util.CutOverPhaseII, true, true)
	phaseII = time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	phaseI := time.Now().Unix()

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, phaseII+3, phaseI-3, 2, 1)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, phaseII+3, phaseI-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, phaseII+3, phaseI-3)

	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Phase I: ", phaseI)
	util.MoveCutOverPhase(t, util.CutOverPhaseI, true, true)
	phaseI = time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	preMode := time.Now().Unix()

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, phaseI+3, preMode-3, 1, 2)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, phaseI+3, preMode-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, phaseI+3, preMode-3)

	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Pre Mode: ", phaseI)
	util.MoveCutOverPhase(t, util.CutOverPre, true, true)
	preMode = time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	enableMode := time.Now().Unix()

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, preMode+3, enableMode-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, preMode+3, enableMode-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, preMode+3, enableMode-3, 1, 2)

	logger2.GetLogger().Log(logger2.Alert, "Moving from Enable to Cutover Enable Mode: ", enableMode)
	util.MoveCutOverPhase(t, util.CutOverEnable, true, true)
	enableMode = time.Now().Unix()
	logger2.GetLogger().Log(logger2.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 1
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 1, t)
	trafficStopped := time.Now().Unix()
	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, enableMode+3, trafficStopped-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, enableMode+3, trafficStopped-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, enableMode+3, trafficStopped-3, 1, 2)

}
