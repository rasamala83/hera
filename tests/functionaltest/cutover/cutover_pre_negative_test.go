package main

import (
	"github.com/paypal/hera/tests/util"
	"github.com/paypal/hera/utility/logger"
	"sync"
	"testing"
	"time"
)

/*
PRE-SETUP
------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase  |
------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | ENABLE |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | N        | N        | ENABLE |
------------------------------------------------------------------------------

**************************************
TestCutOverPreSourceDBDown
**************************************
-----------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | PRE   |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | N        | N        | PRE   |
-----------------------------------------------------------------------------

* shutdown source DB

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
    | HERADB_ONE     |  25             | herabox_primary_srv   | down   |
    | HERADB_TWO     |  25             | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO    | down   |
    | WRITE        |  HERADB_ONE | HERADB_TWO    | down   |
    | TXN          |  HERADB_ONE | HERADB_TWO    | down   |
    -------------------------------------------------------
 4. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be up and failing to connect
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active |
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active |
    | TXN          |             | HERADB_ONE, HERADB_TWO | active |
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOverPreSourceDBDown(t *testing.T) {
	_, logFile := util.Setup(t)

	var wg sync.WaitGroup
	respChan, dumpChan, RespMsg := util.CT.SendClientTraffic(&wg)
	defer util.TearDown(t, respChan, dumpChan, RespMsg, logFile)

	beforeStart := time.Now().Unix() + 2
	logger.GetLogger().Log(logger.Alert, "Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)
	afterComplete := time.Now().Unix() - 3
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, beforeStart, afterComplete, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, beforeStart, afterComplete, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, beforeStart, afterComplete, 1, 2)

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

	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 1, t)

	stateLog["occ"] = 25
	stateLog["occ.co"] = 1
	util.ValidateStateLog(t, stateLog, true)

	startClientTraffic := time.Now().Unix()
	logger.GetLogger().Log(logger.Alert, "Moving from Enable to Pre Cutover state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.CutOverPre, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	beforeSourceDBGoesDown := time.Now().Unix()
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, beforeSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, beforeSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, beforeSourceDBGoesDown-3, 1, 2)

	logger.GetLogger().Log(logger.Alert, "Shutting down source database")
	util.ShutDownDBService("HERADB_ONE", "herabox_primary_srv", t)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 20 seconds")
	shutdownPrimary := time.Now().Unix()
	time.Sleep(20 * time.Second)
	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}
	afterSourceDBGoesDown := time.Now().Unix()

	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, shutdownPrimary, afterSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, shutdownPrimary, afterSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, shutdownPrimary, afterSourceDBGoesDown-3, 1, 2)

	util.KillSessions(t, false, "herabox_primary_srv")
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	afterKill := time.Now().Unix()
	time.Sleep(15 * time.Second)

	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)

	util.ValidateFailureTraffic(t, trafficStats, util.READ, afterKill+3, afterSourceDBGoesDown-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterKill+3, afterSourceDBGoesDown-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterKill+3, afterSourceDBGoesDown-3)

	util.RestartOCC(t)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)

	occStatus = util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}

}

/*
PRE-SETUP
------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase  |
------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | ENABLE |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | N        | N        | ENABLE |
------------------------------------------------------------------------------

**************************************
TestCutOverPreTargetDBDown
**************************************
-----------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | PRE   |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | N        | N        | PRE   |
-----------------------------------------------------------------------------

* shutdown target DB

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
    | HERADB_TWO     |  25             | herabox_secondary_srv | down   |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO    | active |
    | WRITE        |  HERADB_ONE | HERADB_TWO    | active |
    | TXN          |  HERADB_ONE | HERADB_TWO    | active |
    -------------------------------------------------------
 4. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be up
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO    | active |
    | WRITE        |  HERADB_ONE | HERADB_TWO    | active |
    | TXN          |  HERADB_ONE | HERADB_TWO    | active |
    -------------------------------------------------------

* start target DB

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
    | HERADB_TWO     |  25             | herabox_secondary_srv | down   |
    ---------------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOverPreTargetDBDown(t *testing.T) {
	_, logFile := util.Setup(t)

	var wg sync.WaitGroup

	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

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

	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 1, t)

	stateLog["occ"] = 25
	stateLog["occ.co"] = 1
	util.ValidateStateLog(t, stateLog, true)
	respChan, dumpChan, RespMsg := util.CT.SendClientTraffic(&wg)
	defer util.TearDown(t, respChan, dumpChan, RespMsg, logFile)

	logger.GetLogger().Log(logger.Alert, "Sleeping for 5 seconds")
	time.Sleep(5 * time.Second)
	startClientTraffic := time.Now().Unix()
	logger.GetLogger().Log(logger.Alert, "Moving from Enable to Pre Cutover state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.CutOverPre, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	beforeSourceDBGoesDown := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, beforeSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, beforeSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, beforeSourceDBGoesDown-3, 1, 2)

	logger.GetLogger().Log(logger.Alert, "Shutting down target database")
	util.ShutDownDBService("HERADB_TWO", "herabox_secondary_srv", t)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 20 seconds")
	shutdownPrimary := time.Now().Unix()
	time.Sleep(20 * time.Second)
	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}
	afterSourceDBGoesDown := time.Now().Unix()

	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, shutdownPrimary, afterSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, shutdownPrimary, afterSourceDBGoesDown-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, shutdownPrimary, afterSourceDBGoesDown-3, 1, 2)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)

	beforeKill := time.Now().Unix()

	util.KillSessions(t, true, "herabox_secondary_srv")
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)

	stateLog["occ"] = 25
	stateLog["occ.co"] = 0
	util.KillSessionAndValidate(t, stateLog, "herabox_secondary_srv", true)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, -1, t)

	util.ValidateSuccessTraffic(t, trafficStats, util.READ, afterSourceDBGoesDown, beforeKill-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, afterSourceDBGoesDown, beforeKill-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, afterSourceDBGoesDown, beforeKill-3, 1, 2)

	logger.GetLogger().Log(logger.Alert, "Starting down target database")
	util.StartDBService("HERADB_TWO", "herabox_secondary_srv", t)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 20 seconds")
	time.Sleep(20 * time.Second)

	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)

}

/*
PRE-SETUP
------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase  |
------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | ENABLE |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | N        | N        | ENABLE |
------------------------------------------------------------------------------

**************************************
TestCutOverPreUniqNameInCorrect
**************************************
-------------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname           | r_status | w_status | phase |
-------------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE_INVALID | Y        | Y        | PRE   |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO_INVALID | N        | N        | PRE   |
-------------------------------------------------------------------------------------

Validation:
 1. Worker validation after 15 seconds
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | init+schd        |
    ----------------------------------------------------
 2. DB validation after 15 seconds
    ---------------------------------------------------------------------
    | db unique name | num of sessions | service name          | state  |
    ---------------------------------------------------------------------
    | HERADB_ONE     |  25             | herabox_primary_srv   | active |
    | HERADB_TWO     |  0              | herabox_secondary_srv | active |
    ---------------------------------------------------------------------
 3. Traffic Validation for the whole 15 seconds
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO    | active |
    | WRITE        |  HERADB_ONE | HERADB_TWO    | active |
    | TXN          |  HERADB_ONE | HERADB_TWO    | active |
    -------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | init+schd        |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be up and failing to connect
    ----------------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB          | state  |
    ----------------------------------------------------------------
    | READ         |   			 | HERADB_ONE, HERADB_TWO | active | TODO Failing
    | WRITE        |             | HERADB_ONE, HERADB_TWO | active | TODO Failing
    | TXN          |             | HERADB_ONE, HERADB_TWO | active | TODO Failing
    ----------------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOverPreUniqNameInCorrect(t *testing.T) {
	_, logFile := util.Setup(t)

	var wg sync.WaitGroup

	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

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

	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 1, t)

	stateLog["occ"] = 25
	stateLog["occ.co"] = 1
	util.ValidateStateLog(t, stateLog, true)
	respChan, dumpChan, RespMsg := util.CT.SendClientTraffic(&wg)
	defer util.TearDown(t, respChan, dumpChan, RespMsg, logFile)

	logger.GetLogger().Log(logger.Alert, "Sleeping for 5 seconds")
	time.Sleep(5 * time.Second)
	startClientTraffic := time.Now().Unix()
	logger.GetLogger().Log(logger.Alert, "Moving from Enable to Pre Cutover state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.CutOverPreInValidUniqName, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co.not_connected"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 0, t)
	beforeSessionKill := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, beforeSessionKill-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, beforeSessionKill-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, beforeSessionKill-3, 1, 2)

	stateLog["occ"] = 25
	stateLog["occ.co.not_connected"] = 25
	util.RestartOCC(t)
	afterRestart := time.Now().Unix()
	logger.GetLogger().Log(logger.Alert, "Sleeping for 25 seconds")
	time.Sleep(25 * time.Second)
	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}
	trafficStopped := time.Now().Unix()
	trafficStats = util.CT.StopClientTraffic(respChan, RespMsg)
	util.ValidateFailureTraffic(t, trafficStats, util.READ, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterRestart+3, trafficStopped-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterRestart+3, trafficStopped-3)
}

/*
PRE-SETUP
------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase  |
------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | ENABLE |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | N        | N        | ENABLE |
------------------------------------------------------------------------------

**************************************
TestCutOverPreUniqNameInCorrectDestDB
**************************************
only in secondary table
--------------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname           | r_status | w_status | phase  |
--------------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE_INVALID | Y        | Y        | ENABLE |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO_INVALID | N        | N        | ENABLE |
--------------------------------------------------------------------------------------

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
    | READ         |  HERADB_ONE | HERADB_TWO    | active |
    | WRITE        |  HERADB_ONE | HERADB_TWO    | active |
    | TXN          |  HERADB_ONE | HERADB_TWO    | active |
    -------------------------------------------------------
 4. Validate after forcing workers restart (by killing sessions from db's end)
    ----------------------------------------------------
    | two task     | num of workers | state            |
    ----------------------------------------------------
    | CLOC         |  25            | accept+wait+busy |
    | CLOC_CUTOVER |  25            | accept+wait+busy |
    ----------------------------------------------------
 5. Validate after forcing occ restart (including mux) - wait for 25 seconds before validation
    5.1 OCC Container should be up and failing to connect
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         | HERADB_ONE	 | HERADB_TWO    | active |
    | WRITE        | HERADB_ONE  | HERADB_TWO    | active |
    | TXN          | HERADB_ONE  | HERADB_TWO    | active |
    -------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOverPreUniqNameInCorrectDestDB(t *testing.T) {
	_, logFile := util.Setup(t)

	var wg sync.WaitGroup

	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

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

	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 1, t)

	stateLog["occ"] = 25
	stateLog["occ.co"] = 1
	util.ValidateStateLog(t, stateLog, true)
	respChan, dumpChan, RespMsg := util.CT.SendClientTraffic(&wg)
	defer util.TearDown(t, respChan, dumpChan, RespMsg, logFile)

	logger.GetLogger().Log(logger.Alert, "Sleeping for 5 seconds")
	time.Sleep(5 * time.Second)
	startClientTraffic := time.Now().Unix()
	logger.GetLogger().Log(logger.Alert, "Moving from Enable to Pre Cutover state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.CutOverPreInValidUniqName, false, true)
	util.MoveCutOverPhase(t, util.CutOverPre, true, false)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	beforeSessionKill := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, beforeSessionKill-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, beforeSessionKill-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, beforeSessionKill-3, 1, 2)

	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.KillSessionAndValidate(t, stateLog, "herabox_primary_srv", false)
	occStatus := util.IsContainerUp(t, "occ")
	if occStatus != true {
		t.Fatalf("OCC is down - which is not expected")
	}

	util.CT.StopClientTraffic(respChan, RespMsg)
}

/*
PRE-SETUP
------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase  |
------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | ENABLE |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | N        | N        | ENABLE |
------------------------------------------------------------------------------

**************************************
TestCutOverPreUniqNameInCorrectDestDB
**************************************
-----------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase |
-----------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | PRE   |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | N        | N        | PRE   |
-----------------------------------------------------------------------------

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
    | READ         |  HERADB_ONE | HERADB_TWO    | active |
    | WRITE        |  HERADB_ONE | HERADB_TWO    | active |
    | TXN          |  HERADB_ONE | HERADB_TWO    | active |
    -------------------------------------------------------

Rollback to Enable mode
------------------------------------------------------------------------------
| ROWS | occ_name | occ_two_task | db_uname   | r_status | w_status | phase  |
------------------------------------------------------------------------------
| Row1 | occ      | CLOC         | HERADB_ONE | Y        | Y        | ENABLE |
| Row2 | occ      | CLOC_CUTOVER | HERADB_TWO | N        | N        | ENABLE |
-----------------------------------------------------------------------------

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
    -------------------------------------------------------
    | Traffic Type | Success DB  | No Traffic DB | state  |
    -------------------------------------------------------
    | READ         |  HERADB_ONE | HERADB_TWO    | active |
    | WRITE        |  HERADB_ONE | HERADB_TWO    | active |
    | TXN          |  HERADB_ONE | HERADB_TWO    | active |
    -------------------------------------------------------

TODO: Need to add logs and CAL log verification
*/
func TestCutOverPreRollback(t *testing.T) {
	_, logFile := util.Setup(t)

	var wg sync.WaitGroup

	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

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

	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 1, t)

	stateLog["occ"] = 25
	stateLog["occ.co"] = 1
	util.ValidateStateLog(t, stateLog, true)

	respChan, dumpChan, RespMsg := util.CT.SendClientTraffic(&wg)
	defer util.TearDown(t, respChan, dumpChan, RespMsg, logFile)

	logger.GetLogger().Log(logger.Alert, "Sleeping for 5 seconds")
	time.Sleep(5 * time.Second)
	startClientTraffic := time.Now().Unix()
	logger.GetLogger().Log(logger.Alert, "Moving from Enable to Pre Cutover state: ", startClientTraffic)
	util.MoveCutOverPhase(t, util.CutOverPre, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	beforeRollback := time.Now().Unix()
	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, beforeRollback-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, beforeRollback-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, beforeRollback-3, 1, 2)

	util.MoveCutOverPhase(t, util.CutOverEnable, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 1, t)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 1
	util.ValidateStateLog(t, stateLog, true)

	util.ResetOCCDocker(t)
	util.OCCConfig(t, "readonly_children_pct", "0", "/x/web/LIVE/occ/occ.cdb")
	util.RestartOCC(t)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	stateLog = make(map[string]int)
	stateLog["occ"] = 25
	util.ValidateStateLog(t, stateLog, true)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 0, t)

	util.CT.StopClientTraffic(respChan, RespMsg)
}
