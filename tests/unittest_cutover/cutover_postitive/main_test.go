package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/paypal/hera/client/gosqldriver"
	"github.com/paypal/hera/tests/util"
	"github.com/paypal/hera/utility/logger"
	"sync"
	"testing"
	"time"
)

func init() {
	logger.CreateLogger("log.txt", "UT", logger.Info)
}

func initialSetupStats(t *testing.T) []util.DBStatus {
	util.StartDBService("HERADB_PRIMARY", "herabox_primary", t)
	util.ShutDownDBService("HERADB_PRIMARY", "herabox_cutover", t)
	util.ShutDownDBService("HERADB_CUTOVER", "herabox_cutover", t)
	util.StartDBService("HERADB_CUTOVER", "herabox_primary", t)
	util.LockUnlockUser(t, "unlock", false)
	return util.LockUnlockUser(t, "lock", true)
}

//func TestDB_cut_over_set_up(t *testing.T) {
//	pwd, _ := os.Getwd()
//	host, driverName := util.SetUpHeraConnection(pwd + "/../../certs/client_test.cert")
//	db, err := sql.Open(driverName, host)
//	if err != nil {
//		t.Fatalf("open err %s\n", err.Error())
//	}
//
//	ctx := context.Background()
//
//	conn, err := db.Conn(ctx)
//	if err != nil {
//		t.Fatalf("conn err %s\n", err.Error())
//	}
//	defer conn.Close()
//	stmt, err := conn.PrepareContext(ctx, "select * from db_id_test")
//	if err != nil {
//		t.Fatalf("prep err %s\n", err.Error())
//	}
//	rows, err := stmt.QueryContext(ctx)
//	if err != nil {
//		t.Fatalf("query err %s\n", err.Error())
//	}
//	defer rows.Close()
//
//	for rows.Next() {
//		var name string
//		var id int
//		if err := rows.Scan(&id, &name); err != nil {
//			t.Fatal(err)
//		}
//		fmt.Println("Validating if we are connecting to DB1")
//		if id != 1 || name != "db_1" {
//			t.Fatalf("Wrong DB ID present")
//		}
//	}
//	util.ValidateWorkerCount("HERADB_PRIMARY", "herabox_primary", 25, 3,
//		util.GetDBStatus(), t)
//	fmt.Printf("Sucessfully read from db_id_test\n")
//}
//
//func TestCutOverInitSetup(t *testing.T) {
//	var wg sync.WaitGroup
//
//	initialSetupStats(t)
//	respChan := util.CT.SendClientTraffic(&wg)
//	util.ShutDownDBService("HERADB_CUTOVER", "herabox_primary", t)
//	util.ShutDownDBService("HERADB_PRIMARY", "herabox_cutover", t)
//
//	dbStatus := util.GetDBStatus()
//	util.ValidateWorkerCount("HERADB_PRIMARY", "herabox_primary", 25, 0,
//		dbStatus, t)
//	util.ValidateWorkerCount("HERADB_PRIMARY", "herabox_cutover", -1, 0,
//		dbStatus, t)
//	util.ValidateWorkerCount("HERADB_CUTOVER", "herabox_primary", -1, 0,
//		dbStatus, t)
//	util.ValidateWorkerCount("HERADB_CUTOVER", "herabox_cutover", -1, 0,
//		dbStatus, t)
//
//	util.StartDBService("HERADB_CUTOVER", "herabox_primary", t)
//
//	util.ValidateWorkerCount("HERADB_CUTOVER", "herabox_primary", 0, 0,
//		util.GetDBStatus(), t)
//	trafficStats := util.CT.StopClientTraffic(respChan)
//	util.ValidateTraffic(t, trafficStats, -1, -1, -1, 3)
//
//	util.ValidateConnectionIntegrity(13, 12, 3, 1, "db_1")
//
//	wg.Wait()
//}

func TestLegacyCutOver(t *testing.T) {
	var wg sync.WaitGroup

	dbStatus := initialSetupStats(t)
	respChan := util.CT.SendClientTraffic(&wg)
	//
	util.ValidateWorkerCount("HERADB_PRIMARY", "herabox_cutover", -1, 0,
		dbStatus, t)
	util.ValidateWorkerCount("HERADB_CUTOVER", "herabox_cutover", -1, 0,
		dbStatus, t)
	util.ValidateWorkerCount("HERADB_PRIMARY", "herabox_primary", 25, 0,
		dbStatus, t)
	util.ValidateWorkerCount("HERADB_CUTOVER", "herabox_primary", 0, 0,
		dbStatus, t)
	time.Sleep(20 * time.Second)
	util.LockUnlockUser(t, "lock", true)

	util.EnableTNSForCutOver(t)
	//fmt.Println("sleeping for 20 to pick up new tns")
	time.Sleep(20 * time.Second)

	util.ShutDownDBService("HERADB_PRIMARY", "herabox_primary", t)
	util.StartDBService("HERADB_PRIMARY", "herabox_cutover", t)
	util.StartDBService("HERADB_CUTOVER", "herabox_cutover", t)

	serviceStopTime := time.Now().Unix()
	status := util.KillSessions(t, false, "herabox_primary")
	util.ValidateWorkerCount("HERADB_PRIMARY", "herabox_primary", -1, 0,
		status, t)
	//util.ValidateWorkerCount("HERADB_CUTOVER", "herabox_primary", 0, 0,
	//	status, t)
	fmt.Println("sleeping for 60 seconds before unlocking")
	time.Sleep(60 * time.Second)
	util.LockUnlockUser(t, "unlock", true)
	serviceUpTime := time.Now().Unix()
	fmt.Printf("sleeping for 60 seconds to give time for worker to reconnect")
	time.Sleep(60 * time.Second)
	util.ValidateWorkerCount("HERADB_CUTOVER", "herabox_primary", 25, 0,
		status, t)
	trafficStats := util.CT.StopClientTraffic(respChan)
	util.ValidateTraffic(t, trafficStats, serviceStopTime, serviceStopTime, serviceUpTime, 10)
}
