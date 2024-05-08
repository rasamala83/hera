package main

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/paypal/hera/client/gosqldriver"
	"github.com/paypal/hera/tests/util"
	"github.com/paypal/hera/utility/logger"
	"sync"
	"testing"
	"time"
)

func TestCutOverPositive(t *testing.T) {

	// bring the setup to initial state
	//OCC running with only one db in TNS, no env set for cut over, cut over table is empty
	_, logFile := util.Setup(t)

	// enable cut over env and tns changes
	util.EnableCutOver(t, false, false)
	util.MoveCutOverPhase(t, util.CreateTable, true, true)
	util.MoveCutOverPhase(t, util.CutOverEnable, true, true)
	util.RestartOCC(t)

	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	// send client traffic
	var wg sync.WaitGroup
	respChan, dumpChan, RespMsg := util.CT.SendClientTraffic(&wg)
	defer util.TearDown(t, respChan, dumpChan, RespMsg, logFile)

	stateLog := make(map[string]int)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 1
	util.ValidateStateLog(t, stateLog, true)

	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 1, t)

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

	beforeCutOverStart := time.Now().Unix()

	trafficStats := util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, startClientTraffic, beforeCutOverStart-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, startClientTraffic, beforeCutOverStart-3, 1, 2)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, startClientTraffic, beforeCutOverStart-3, 1, 2)

	logger.GetLogger().Log(logger.Alert, "Moving from Pre to Cutover state(stopping write in main DB): ", beforeCutOverStart)
	util.MoveCutOverPhase(t, util.CutOverPhaseI, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	afterServiceStop := time.Now().Unix()
	logger.GetLogger().Log(logger.Alert, "Moved to Cutover state(stopped write in main db): ", afterServiceStop)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, beforeCutOverStart, afterServiceStop-3, 1, 2)

	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)

	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterServiceStop, time.Now().Unix()-3)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterServiceStop, time.Now().Unix()-3)

	logger.GetLogger().Log(logger.Alert, "Moving Read to CutOver: ", time.Now().Unix())
	util.MoveCutOverPhase(t, util.CutOverPhaseII, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	afterReadCutOver := time.Now().Unix()
	logger.GetLogger().Log(logger.Alert, "Moved Read to Cutover database: ", afterReadCutOver)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	readCutOverValidation := time.Now().Unix() - 3
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, afterReadCutOver, readCutOverValidation, 2, 1)
	util.ValidateFailureTraffic(t, trafficStats, util.WRITE, afterServiceStop, readCutOverValidation)
	util.ValidateFailureTraffic(t, trafficStats, util.TXN, afterServiceStop, readCutOverValidation)

	logger.GetLogger().Log(logger.Alert, "Moving Write to CutOver: ", time.Now().Unix())
	util.MoveCutOverPhase(t, util.CutOverPhaseIII, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	afterWriteCutOver := time.Now().Unix()
	logger.GetLogger().Log(logger.Alert, "Moved Write to Cutover database: ", afterWriteCutOver)
	stateLog["occ"] = 25
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	writeCutOverValidation := time.Now().Unix() - 3
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, readCutOverValidation, writeCutOverValidation, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, afterWriteCutOver, writeCutOverValidation, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, afterWriteCutOver, writeCutOverValidation, 2, 1)

	logger.GetLogger().Log(logger.Alert, "Moving to Complete State: ", time.Now().Unix())
	util.MoveCutOverPhase(t, util.CutOverComplete, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	afterComplete := time.Now().Unix() - 3
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, writeCutOverValidation, afterComplete, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, writeCutOverValidation, afterComplete, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, writeCutOverValidation, afterComplete, 2, 1)
	stateLog["occ"] = 1
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 1, t)

	logger.GetLogger().Log(logger.Alert, "Moving to Broom State: ", time.Now().Unix())
	util.MoveCutOverPhase(t, util.CutOverBroom, true, true)
	logger.GetLogger().Log(logger.Alert, "Sleeping for 15 seconds")
	time.Sleep(15 * time.Second)
	trafficStats = util.CT.DumpTrafficStat(dumpChan, RespMsg)
	afterBroom := time.Now().Unix() - 3
	util.ValidateSuccessTraffic(t, trafficStats, util.READ, afterComplete, afterBroom, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.WRITE, afterComplete, afterBroom, 2, 1)
	util.ValidateSuccessTraffic(t, trafficStats, util.TXN, afterComplete, afterBroom, 2, 1)
	stateLog["occ"] = 1
	stateLog["occ.co"] = 25
	util.ValidateStateLog(t, stateLog, true)
	util.ValidateWorkerCountFromDatabase("HERADB_TWO", "herabox_secondary_srv", true, 25, t)
	util.ValidateWorkerCountFromDatabase("HERADB_ONE", "herabox_primary_srv", true, 1, t)

	util.CT.StopClientTraffic(respChan, RespMsg)
}
