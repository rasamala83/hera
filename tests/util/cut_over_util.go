package util

import (
	"context"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/paypal/hera/client/gosqldriver/tcp"
	"github.com/paypal/hera/client/gosqldriver/tls"
	"github.com/paypal/hera/utility/logger"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
)

var timeMutex sync.Mutex
var readMutex sync.Mutex
var writeMutex sync.Mutex
var txnMutex sync.Mutex
var dbMutex sync.Mutex

var READ = "ReadType"
var WRITE = "WriteType"
var TXN = "TXNType"

type DatabaseServices struct {
	ServiceName string
	WorkerCount int
	Active      bool
}

type DBStatus struct {
	Database         string
	DBUniqueName     string
	ClocAppStatus    string
	DatabaseServices []DatabaseServices
}

type DBTxn struct {
	DBConnection  *sql.Conn
	DBContext     context.Context
	DBTransaction *sql.Tx
}

func SetUpHeraConnection(certPath string) (string, string) {
	var tlsEnv = os.Getenv("TLS")
	host := "1:localhost:10101"
	driverName := "heratls"

	if len(tlsEnv) > 0 && tlsEnv == "1" {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "tls enabled")
		}
		tls.HeraTLSDrv.TLSCfg.InsecureSkipVerify = true
		dat, err := os.ReadFile(certPath)
		if err != nil {
			panic(err)
		}

		rootPEM := string(dat)
		roots := x509.NewCertPool()
		ok := roots.AppendCertsFromPEM([]byte(rootPEM))
		if !ok {
			panic("failed to parse root certificate")
		}
		tls.HeraTLSDrv.TLSCfg.RootCAs = roots
		tls.HeraTLSDrv.Ssl = true
		key := []byte{166, 35, 129, 232, 126, 80, 214, 71, 152, 247, 2, 185, 25, 128, 2, 174, 145,
			38, 48, 107, 60, 129, 228, 137, 87, 72, 176, 144, 194, 163, 237, 11}
		tls.HeraTLSDrv.EncryptedAuthKey = key
	} else {
		host = "localhost:10101"
		driverName = "hera"
		tcp.RegisterHeraDriver()
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "tls disabled")
		}
	}
	return host, driverName
}

func dbService(dbUniqueName string, serviceName string, action string) []DBStatus {
	var status []DBStatus

	response, err := http.Get("http://localhost:8000/occ/db_service?action=" + action +
		"&service_name=" + serviceName + "&db_unique_name=" + dbUniqueName)
	if err != nil {
		panic(err)
	}
	responseData, err := io.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}
	err = json.Unmarshal(responseData, &status)
	if err != nil {
		panic(err)
	}
	return status
}

func QueryService(dbUniqueName string, query string) string {
	response, err := http.Get("http://localhost:8000/occ/run_query?query=" + query + "&db_unique_name=" + dbUniqueName)
	if err != nil {
		panic(err)
	}
	responseData, err := io.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		panic(err)
	}
	return string(responseData)
}

func validateServiceStatus(dbStatus []DBStatus, dbUniqueName string, serviceName string, status string) bool {
	foundActiveService := false
	for _, db := range dbStatus {
		for _, service := range db.DatabaseServices {
			if strings.TrimSpace(db.DBUniqueName) == dbUniqueName &&
				strings.TrimSpace(service.ServiceName) == serviceName && service.Active == true {
				foundActiveService = true
				break
			}
		}
	}
	if foundActiveService && status == "stop_service" {
		return false
	}
	if !foundActiveService && status == "start_service" {
		return false
	}
	return true
}

func LockUnlockUser(t *testing.T, action string, cutOver bool) []DBStatus {
	var status []DBStatus
	co := "False"
	if cutOver {
		co = "True"
		fmt.Printf("%s user on cutover db\n", action)
	} else {
		fmt.Printf("%s user on main db\n", action)
	}
	url := "http://localhost:8000/occ/lock_user?cut_over=" + co
	if action == "unlock" {
		url = "http://localhost:8000/occ/unlock_user?cut_over=" + co
	}
	response, err := http.Get(url)
	if err != nil {
		t.Fatalf(err.Error())
	}
	responseData, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = json.Unmarshal(responseData, &status)
	if err != nil {
		t.Fatalf(err.Error())
	}
	return status
}

func EnableTNSForCutOver(t *testing.T) {
	fmt.Println("EnableTNSForCutOver")
	response, err := http.Get("http://localhost:8000/enable_tns_for_cut_over")
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func KillSessions(t *testing.T, cutOver bool, serviceName string) []DBStatus {
	var status []DBStatus

	co := "False"
	if cutOver {
		fmt.Printf("Kill Session for cutover db\n")
		co = "True"
	} else {
		fmt.Printf("Kill Session for main db\n")
	}
	response, err := http.Get("http://localhost:8000/occ/kill_session?cut_over=" + co + "&service_name=" + serviceName)
	if err != nil {
		t.Fatalf(err.Error())
	}
	responseData, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = json.Unmarshal(responseData, &status)
	if err != nil {
		t.Fatalf(err.Error())
	}
	return status
}

func ShutDownDBService(dbUniqueName string, serviceName string, t *testing.T) {
	_, activeResponse := GetDBStatus()
	if !activeResponse[dbUniqueName][serviceName] {
		fmt.Printf("%s:%s is already in down state\n", dbUniqueName, serviceName)
		return
	}
	action := "stop_service"
	fmt.Printf("shutting down %s:%s\n", dbUniqueName, serviceName)
	dbStatus := dbService(dbUniqueName, serviceName, action)

	if validateServiceStatus(dbStatus, dbUniqueName, serviceName, action) != true {
		t.Fatalf("Failed " + action + ":" + dbUniqueName + ":" + serviceName)
	}
}

func StartDBService(dbUniqueName string, serviceName string, t *testing.T) {
	_, activeResponse := GetDBStatus()
	if activeResponse[dbUniqueName][serviceName] {
		fmt.Printf("%s:%s is already in up state\n", dbUniqueName, serviceName)
		return
	}
	action := "start_service"
	fmt.Printf("starting %s:%s\n", dbUniqueName, serviceName)
	dbStatus := dbService(dbUniqueName, serviceName, action)

	if validateServiceStatus(dbStatus, dbUniqueName, serviceName, action) != true {
		t.Fatalf("Failed " + action + ":" + dbUniqueName + ":" + serviceName)
	}
}

func GetDBStatus() ([]DBStatus, map[string]map[string]bool) {
	var status []DBStatus

	activeResponse := make(map[string]map[string]bool)
	response, err := http.Get("http://localhost:8000/occ/status_from_db")
	if err != nil {
		panic(err)
	}
	responseData, err := io.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}
	err = json.Unmarshal(responseData, &status)
	if err != nil {
		panic(err)
	}

	for _, db := range status {
		for _, service := range db.DatabaseServices {
			un := strings.TrimSpace(db.DBUniqueName)
			sn := strings.TrimSpace(service.ServiceName)
			_, ok := activeResponse[un]
			if !ok {
				activeResponse[un] = make(map[string]bool)
			}
			activeResponse[un][sn] = service.Active
		}
	}

	return status, activeResponse
}

func rollbackTxn(txn *DBTxn) {
	defer txn.DBConnection.Close()
	err := txn.DBTransaction.Rollback()
	if err != nil {
		panic(err)
	}
}

func commitTxn(txn *DBTxn) error {
	defer txn.DBConnection.Close()
	return txn.DBTransaction.Commit()
}

func exec(conn *sql.Conn, ctx context.Context, query string) *sql.Rows {
	stmt, err := conn.PrepareContext(ctx, query)
	if err != nil {
		panic(err)
	}
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		panic(err)
	}
	return rows
}

func slowQuery(txn *sql.Tx, expectedId int, sec int, wg *sync.WaitGroup) {
	defer wg.Done()
	query := fmt.Sprintf("select SLOW_QUERY(%d) from dual", sec)
	rows, err := txn.Query(query)

	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			panic(err)
		}
		if expectedId != id {
			msg := fmt.Sprintf("Wrong DB connected expected %d but got %d", expectedId, id)
			panic(msg)
		}
	}
}

func GetConnection() (*sql.Conn, context.Context, error) {
	pwd, _ := os.Getwd()
	host, driverName := SetUpHeraConnection(pwd + "/../../certs/client_test.cert")
	db, err := sql.Open(driverName, host)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	return conn, ctx, nil
}

func ValidateReadWorkerID(ReadWaitTime int, expectedId int, wg *sync.WaitGroup) {
	conn, ctx, err := GetConnection()
	if err != nil {
		panic(err)
	}

	txn, err := conn.BeginTx(ctx, nil)
	if err != nil {
		panic(err)
	}
	wg.Add(1)
	go slowQuery(txn, expectedId, ReadWaitTime, wg)
}

func writeBeginTxn() *DBTxn {
	conn, ctx, err := GetConnection()
	if err != nil {
		panic(err)
	}
	txn, err := conn.BeginTx(ctx, nil)
	if err != nil {
		panic(err)
	}
	insertQuery := "insert into occ_test values(id_seq.NEXTVAL, 'hold-record', 1)"
	_, err = txn.ExecContext(ctx, insertQuery)
	if err != nil {
		txn.Rollback()
		panic(err)
	}

	return &DBTxn{DBConnection: conn, DBContext: ctx, DBTransaction: txn}
}

func validateDBID(dbTxn *DBTxn, dbId int, dbName string) {

	query := "select * from db_id_test"
	stmt, err := dbTxn.DBTransaction.PrepareContext(dbTxn.DBContext, query)
	if err != nil {
		panic(err)
	}
	rows, err := stmt.QueryContext(dbTxn.DBContext)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var id int
		if err := rows.Scan(&id, &name); err != nil {
			panic(err)
		}
		if id != dbId || name != dbName {
			panic("Wrong DB ID present")
		}
	}
}

func ValidateBeforeCutOver(t *testing.T, queryType string, cts ClientTrafficStats) {
	if cts.stats[queryType][2].successCount > 0 || cts.stats[queryType][2].failureCount > 0 {
		t.Fatalf("%s Traffic moved to second DB before cut-over", queryType)
	}

	if cts.stats[queryType][1].successCount == 0 {
		t.Fatalf("%s Traffic missing in db1 before cutover", queryType)
	}

	if cts.stats[queryType][1].failureCount > 0 {
		t.Fatalf("%s Traffic failing in db1 before cutover %d", queryType, cts.stats[queryType][1].failureCount)
	}

	if cts.stats[queryType][0].failureCount > 0 {
		t.Fatalf("%s Traffic failing in db before cutover", queryType)
	}
}

func ValidateAfterCutOverBeforeGrace(t *testing.T, queryType string, cts ClientTrafficStats) {
	if cts.stats[queryType][2].successCount == 0 {
		t.Fatalf("%s Traffic did not move to second DB", queryType)
	}

	if cts.stats[queryType][2].failureCount > 0 || cts.stats[queryType][1].failureCount > 0 {
		t.Fatalf("%s Traffic failing in db1/2 before cutover", queryType)
	}
}

func ValidateAfterCutOverAfterGracePeriod(t *testing.T, queryType string, cts ClientTrafficStats) {
	if cts.stats[queryType][1].successCount > 0 {
		t.Fatalf("%s Traffic did not move to second DB after grace period", queryType)
	}

	if cts.stats[queryType][2].failureCount > 0 || cts.stats[queryType][1].failureCount > 0 {
		t.Fatalf("%s Traffic failing in db1/2 before cutover", queryType)
	}

	if cts.stats[queryType][2].successCount == 0 {
		t.Fatalf("%s no traffic found in db2 after cutover", queryType)
	}

	if cts.stats[queryType][0].failureCount > 0 {
		t.Fatalf("%s Traffic failing in in both DB's", queryType)
	}
}

func validateReadTraffic(t *testing.T, cts ClientTrafficStats, cutOverTime int64, utc int64, graceTime int64) {
	if cutOverTime > 0 {
		// if current stat is after read cut-over
		if utc > cutOverTime {
			ValidateAfterCutOverBeforeGrace(t, READ, cts)

		} else {
			ValidateBeforeCutOver(t, READ, cts)
		}

		// if current stat is after read cut-over and gracePeriodInSeconds - all read should have stopped in database 1
		if utc > cutOverTime+graceTime {
			ValidateAfterCutOverAfterGracePeriod(t, READ, cts)
		}
	} else {
		ValidateBeforeCutOver(t, READ, cts)
	}

}

func ValidateTraffic(t *testing.T, trafficStats map[int64]ClientTrafficStats,
	readCutOverTime int64, writeStopTime int64, writeCutOverTime int64, gracePeriodInSeconds int64) {
	keys := make([]int64, 0)
	for k, _ := range trafficStats {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	fmt.Println("Validating if Read/Write Traffic moved to second db")
	for _, utc := range keys {
		cts := trafficStats[utc]

		validateReadTraffic(t, cts, readCutOverTime, utc, gracePeriodInSeconds)

		if utc < writeStopTime {
			ValidateBeforeCutOver(t, WRITE, cts)
			ValidateBeforeCutOver(t, TXN, cts)
		}

		if writeCutOverTime > 0 && utc > writeCutOverTime+gracePeriodInSeconds {
			ValidateAfterCutOverAfterGracePeriod(t, WRITE, cts)
			ValidateAfterCutOverAfterGracePeriod(t, TXN, cts)
		}

		if writeStopTime > 0 && writeCutOverTime > 0 && utc > writeStopTime && utc < writeCutOverTime {
			if cts.stats[WRITE][0].failureCount == 0 || cts.stats[WRITE][1].successCount > 0 ||
				cts.stats[WRITE][2].successCount > 0 {
				t.Fatalf("Write is not failing during cutover")
			}
			if cts.stats[TXN][0].failureCount == 0 || cts.stats[TXN][1].successCount > 0 ||
				cts.stats[TXN][2].successCount > 0 {
				t.Fatalf("Write is not failing during cutover")
			}
		}

		if cts.stats[READ][0].failureCount > 0 {
			t.Fatalf("Read Traffic failing in in both DB's")
		}

	}

}

func ValidateConnectionIntegrity(WriteWorkerCount int, ReadWorkerCount int, ReadWaitSeconds int,
	expectedId int, expectedDBString string) {
	var dbWriteTrans []*DBTxn
	var wg sync.WaitGroup

	fmt.Println(" and as of now it is connected to db_1")

	fmt.Printf("Validating occ connection integrity: Validating WriteWorker:%d, Validating ReadWorker:%d\n", WriteWorkerCount, ReadWorkerCount)

	for i := 0; i < WriteWorkerCount; i++ {
		txn := writeBeginTxn()
		dbWriteTrans = append(dbWriteTrans, txn)
	}

	for i := 0; i < ReadWorkerCount; i++ {
		ValidateReadWorkerID(ReadWaitSeconds, expectedId, &wg)
	}

	for _, dbTxn := range dbWriteTrans {
		validateDBID(dbTxn, expectedId, expectedDBString)
	}

	// wait for readers as they are blocked by procedure
	wg.Wait()

	for _, dbTxn := range dbWriteTrans {
		rollbackTxn(dbTxn)
	}

}

func ValidateWorkerCount(dbUniqueName string, serviceName string,
	expectedWorkerCount int, variance int,
	databaseServices []DBStatus, t *testing.T) {
	fmt.Printf("Validating %s:%s has atleast %d workers connected to db\n",
		dbUniqueName, serviceName, expectedWorkerCount+variance)
	foundWorker := false
	for _, db := range databaseServices {
		for _, service := range db.DatabaseServices {
			if strings.TrimSpace(db.DBUniqueName) == dbUniqueName &&
				strings.TrimSpace(service.ServiceName) == serviceName && service.Active {
				foundWorker = true
				if service.WorkerCount > expectedWorkerCount+variance {
					t.Fatalf("Failed for worker count in %s:%s - expected/actual %d/%d",
						strings.TrimSpace(db.DBUniqueName),
						strings.TrimSpace(service.ServiceName),
						service.WorkerCount, expectedWorkerCount)
				}
			}
		}
	}
	if expectedWorkerCount >= 0 && !foundWorker {
		t.Fatalf("Failed for worker count in %s:%s", strings.TrimSpace(dbUniqueName),
			strings.TrimSpace(serviceName))
	}

	if expectedWorkerCount == -1 && foundWorker {
		t.Fatalf("Service should not be up and running %s:%s", strings.TrimSpace(dbUniqueName),
			strings.TrimSpace(serviceName))
	}
}
