package util

import (
	"bytes"
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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var timeMutex sync.Mutex
var statMutex sync.Mutex
var readMutex sync.Mutex
var writeMutex sync.Mutex
var txnMutex sync.Mutex

var READ = "ReadType"
var WRITE = "WriteType"
var TXN = "TXNType"

var occHost = "10.183.162.56"
var occPort = "10101"
var oracleHost = "10.183.162.56"

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

func SetUpHeraConnection(certPath string) (string, string, error) {
	var tlsEnv = os.Getenv("TLS")
	host := "1:" + occHost + ":" + occPort
	driverName := "heratls"

	if len(tlsEnv) > 0 && tlsEnv == "1" {
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "tls enabled")
		}
		tls.HeraTLSDrv.TLSCfg.InsecureSkipVerify = true
		dat, err := os.ReadFile(certPath)
		if err != nil {
			return "", "", err
		}

		rootPEM := string(dat)
		roots := x509.NewCertPool()
		ok := roots.AppendCertsFromPEM([]byte(rootPEM))
		if !ok {
			return "", "", fmt.Errorf("failed to parse root certificate")
		}
		tls.HeraTLSDrv.TLSCfg.RootCAs = roots
		tls.HeraTLSDrv.Ssl = true
		key := []byte{166, 35, 129, 232, 126, 80, 214, 71, 152, 247, 2, 185, 25, 128, 2, 174, 145,
			38, 48, 107, 60, 129, 228, 137, 87, 72, 176, 144, 194, 163, 237, 11}
		tls.HeraTLSDrv.EncryptedAuthKey = key
	} else {
		host = occHost + ":" + occPort
		driverName = "hera"
		tcp.RegisterHeraDriver()
		if logger.GetLogger().V(logger.Warning) {
			logger.GetLogger().Log(logger.Warning, "tls disabled")
		}
	}
	return host, driverName, nil
}

func dbService(dbUniqueName string, serviceName string, action string) []DBStatus {
	var status []DBStatus

	response, err := http.Get("http://" + oracleHost + ":8000/occ/db_service?action=" + action +
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

func CleanCutOverTable(t *testing.T) {
	query := "delete from pypl_occ_cutover"
	QueryOracle(t, query, "False", "False")
	QueryOracle(t, query, "True", "False")
}

func MoveCutOverPhase(t *testing.T, phase string, comment string) {
	switch phase {
	case "pre":
		query := "update pypl_occ_cutover set cutover_phase='pre', remarks='" + comment +
			"' where dbuname='HERADB_ONE' and occ_name='occ'"
		QueryOracle(t, query, "False", "False")
		QueryOracle(t, query, "True", "False")
		query = "update pypl_occ_cutover set cutover_phase='pre', remarks='" + comment +
			"' where dbuname='HERADB_TWO' and occ_name='occ'"
		QueryOracle(t, query, "False", "False")
		QueryOracle(t, query, "True", "False")
		break
	case "enable":
		CleanCutOverTable(t)
		query := "insert into pypl_occ_cutover values ('HERADB_ONE', 'occ', 'CLOC', 'Y', 'Y', 'enable', '" +
			comment + "')"
		QueryOracle(t, query, "False", "False")
		QueryOracle(t, query, "True", "False")
		query = "insert into pypl_occ_cutover values ('HERADB_TWO', 'occ', 'CLOC_CUTOVER', 'N', 'N', 'enable', '" +
			comment + "')"
		QueryOracle(t, query, "False", "False")
		QueryOracle(t, query, "True", "False")
		break
	default:
		t.Fatalf("Unknown cutover phase %s.\n", phase)
	}
}

func OCCConfig(t *testing.T, key string, value string, filename string) {
	fmt.Printf("Changing occ config in file %s: key: %s, value: %s\n", filename, key, value)
	url := "http://" + oracleHost + ":8000/occ/occ_config?key=" + key + "&value=" + value + "&filename=" + filename
	response, err := http.Get(url)
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func OCCBinarySetup(t *testing.T, filename string) string {
	fmt.Printf("Pushing mux binary from %s\n", filename)
	client := &http.Client{
		Timeout: time.Second * 10,
	}

	url := "http://" + oracleHost + ":8000?filename=mux"
	b, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf(err.Error())
	}

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(b))
	if err != nil {
		t.Fatalf(err.Error())
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	resp, _ := client.Do(req)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf(strconv.Itoa(resp.StatusCode))
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed while reading response %s", err)
	}
	if strings.Contains(string(body), "ORA-") {
		t.Fatalf("Failed while reading response %s", body)
	}
	return string(body)
}

func StopOCCDocker(t *testing.T) {
	fmt.Println("restarting occ")
	url := "http://" + oracleHost + ":8000/docker_support?container=occ&action=stop"
	response, err := http.Get(url)
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func StartOCCDocker(t *testing.T) {
	fmt.Println("restarting occ")
	url := "http://" + oracleHost + ":8000/docker_support?container=occ&action=start"
	response, err := http.Get(url)
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func RestartOCC(t *testing.T) {
	fmt.Println("restarting occ")
	url := "http://" + oracleHost + ":8000/occ/restart_occ"
	response, err := http.Get(url)
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func QueryOracle(t *testing.T, query string, cutOver string, dbaUser string) string {
	url := "http://" + oracleHost + ":8000/run_query?cut_over=" + cutOver + "&dba_user=" + dbaUser
	jsonStr := "{\"query\":\"" + strings.Replace(query, strconv.Itoa(int('"')), "'", -1) + "\"" +
		", \"dba_user\": \"" + dbaUser + "\",\"cut_over\":\"" + cutOver + "\"}"

	fmt.Printf("Running SQL: %s, cutOver=%s\n", strings.Replace(query, strconv.Itoa(int('"')), "'", -1), cutOver)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(jsonStr)))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Failed while calling %s", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed while reading response %s", err)
	}
	if strings.Contains(string(body), "ORA-") {
		t.Fatalf("Failed while reading response %s", body)
	}
	return string(body)
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
	url := "http://" + oracleHost + ":8000/occ/lock_user?cut_over=" + co
	if action == "unlock" {
		url = "http://" + oracleHost + ":8000/occ/unlock_user?cut_over=" + co
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

func DefaultTns(t *testing.T) {
	fmt.Println("Move TNS to Default Value")
	response, err := http.Get("http://" + oracleHost + ":8000/\"default_tns\"")
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func EnableCutOver(t *testing.T, enableRWSplit bool) {
	fmt.Println("EnableCutOver")
	response, err := http.Get("http://" + oracleHost + ":8000/enable_cut_over")
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
	OCCBinarySetup(t, os.Getenv("GOPATH")+"/src/bin/mux")
	if !enableRWSplit {
		OCCConfig(t, "readonly_children_pct", "0", "/x/web/LIVE/occ/occ.cdb")
	}

	MoveCutOverPhase(t, "enable", "TestCutOverEnable")
	RestartOCC(t)

}

func ResetOCCDocker(t *testing.T) {
	fmt.Println("ResetOCCDocker")
	response, err := http.Get("http://" + oracleHost + ":8000/reset")
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
	response, err := http.Get("http://" + oracleHost + ":8000/occ/kill_session?cut_over=" + co + "&service_name=" + serviceName)
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
	response, err := http.Get("http://" + oracleHost + ":8000/occ/status_from_db")
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
	host, driverName, err := SetUpHeraConnection(pwd + "/../../certs/client_test.cert")
	if err != nil {
		return nil, nil, err
	}

	db, err := sql.Open(driverName, host)
	if err != nil {
		return nil, nil, err
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

func ValidateBeforeCutOver(t *testing.T, queryType string, cts ClientTrafficStats, utc int64, trafficStopTime int64) {
	if cts.stats[queryType][2].successCount > 0 || cts.stats[queryType][2].failureCount > 0 {
		t.Fatalf("UTC: %d, Traffic Stop Time: %d - %s Traffic moved to second DB before cut-over", utc, trafficStopTime, queryType)
	}

	if cts.stats[queryType][1].successCount == 0 {
		t.Fatalf("UTC: %d, Traffic Stop Time: %d - %s Traffic missing in db1 before cutover", utc, trafficStopTime, queryType)
	}

	if cts.stats[queryType][1].failureCount > 0 {
		t.Fatalf("UTC: %d, Traffic Stop Time: %d - %s Traffic failing in db1 before cutover %d", utc, trafficStopTime, queryType, cts.stats[queryType][1].failureCount)
	}

	if cts.stats[queryType][0].failureCount > 0 {
		t.Fatalf("UTC: %d, Traffic Stop Time: %d - %s Traffic failing in db before cutover", utc, trafficStopTime, queryType)
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

func ValidateAfterCutOverAfterGracePeriod(t *testing.T, queryType string, cts ClientTrafficStats,
	utc int64, cutOverTime int64) {
	if cts.stats[queryType][1].successCount > 0 {
		t.Fatalf("UTC: %d, CutOverTime: %d, %s Traffic did not move to second DB after grace period",
			utc, cutOverTime, queryType)
	}

	if cts.stats[queryType][2].failureCount > 0 || cts.stats[queryType][1].failureCount > 0 {
		t.Fatalf("UTC: %d, CutOverTime: %d, %s Traffic failing in db1/2 before cutover",
			utc, cutOverTime, queryType)
	}

	if cts.stats[queryType][2].successCount == 0 {
		t.Fatalf("UTC: %d, CutOverTime: %d, %s no traffic found in db2 after cutover",
			utc, cutOverTime, queryType)
	}

	if cts.stats[queryType][0].failureCount > 0 {
		t.Fatalf("UTC: %d, CutOverTime: %d, %s Traffic failing in both DB's",
			utc, cutOverTime, queryType)
	}
}

func validateReadTraffic(t *testing.T, cts ClientTrafficStats, lastGoodKnownTraffic int64,
	cutOverTime int64, utc int64) {
	if cutOverTime > 0 {
		if utc < lastGoodKnownTraffic {
			ValidateBeforeCutOver(t, READ, cts, utc, lastGoodKnownTraffic)
		}

		// if current stat is after read cut-over and gracePeriodInSeconds - all read should have stopped in database 1
		if utc > cutOverTime {
			ValidateAfterCutOverAfterGracePeriod(t, READ, cts, utc, cutOverTime)
		}
	} else {
		ValidateBeforeCutOver(t, READ, cts, utc, lastGoodKnownTraffic)
	}

}

// ValidateTraffic /*
/*
   lastGoodKnownTraffic - time before touching any of the service
   readCutOverTime - time at which read is moved to new DB
   writeStopTime - time at which write is stopped in old DB
   writeCutOverTime - time at which write is completely moved to new DB
   recoverTime - time at which write started moving to new DB
*/

func ValidateTraffic(t *testing.T, trafficStats map[int64]ClientTrafficStats, lastGoodKnownTraffic int64,
	readCutOverTime int64, writeStopTime int64, writeCutOverTime int64, recoverTime int64) {
	keys := make([]int64, 0)
	for k, _ := range trafficStats {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	lastElement := keys[len(keys)-1:][0]
	lastElement = lastElement - 5

	validatedStopTime := false
	validatedCutOverTime := false
	validatedPostCutover := false

	fmt.Println("Validating if Read/Write Traffic moved to second db")
	for _, utc := range keys {
		// ignore last 5 seconds data
		if utc >= lastElement {
			break
		}
		cts := trafficStats[utc]

		validateReadTraffic(t, cts, lastGoodKnownTraffic, readCutOverTime, utc)

		if utc < lastGoodKnownTraffic {
			ValidateBeforeCutOver(t, WRITE, cts, utc, lastGoodKnownTraffic)
			ValidateBeforeCutOver(t, TXN, cts, utc, lastGoodKnownTraffic)
			validatedStopTime = true
		}

		if writeCutOverTime > 0 && utc > writeCutOverTime {
			ValidateAfterCutOverAfterGracePeriod(t, WRITE, cts, utc, writeCutOverTime)
			ValidateAfterCutOverAfterGracePeriod(t, TXN, cts, utc, writeCutOverTime)
			validatedPostCutover = true
		}

		/*
		 if in cut-over time
		*/
		if writeStopTime > 0 && recoverTime > 0 && utc > writeStopTime && utc < recoverTime {
			if cts.stats[WRITE][0].failureCount == 0 || cts.stats[WRITE][1].successCount > 0 ||
				cts.stats[WRITE][2].successCount > 0 {
				t.Fatalf("Write is not failing during cutover")
			}

			failure := cts.stats[READ][0].failureCount + cts.stats[WRITE][0].failureCount + cts.stats[TXN][0].failureCount
			db1Success := cts.stats[READ][1].successCount + cts.stats[WRITE][1].successCount + cts.stats[TXN][1].successCount
			db2Success := cts.stats[READ][2].successCount + cts.stats[WRITE][2].successCount + cts.stats[TXN][2].successCount

			if failure == 0 || cts.stats[TXN][1].successCount > 0 ||
				db1Success+db2Success > 0 {
				t.Fatalf("UTC: %d, writeStopTime:%d, writeCutOverTime:%d "+
					"Write is not failing during cutover Failure : %d, "+
					"db1Success: %d, db2Success: %d", utc, writeStopTime, writeCutOverTime,
					failure, db1Success, db2Success)
			}
			validatedCutOverTime = true
		}
	}
	if !validatedCutOverTime {
		t.Fatalf("did not do validatedCutOverTime")
	}
	if !validatedStopTime {
		t.Fatalf("did not do validatedStopTime")
	}
	if !validatedPostCutover {
		t.Fatalf("did not do validatedPostCutover")
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
		dbUniqueName, serviceName, expectedWorkerCount-variance)
	foundWorker := false
	for _, db := range databaseServices {
		for _, service := range db.DatabaseServices {
			if strings.TrimSpace(db.DBUniqueName) == dbUniqueName &&
				strings.TrimSpace(service.ServiceName) == serviceName && service.Active {
				foundWorker = true
				if service.WorkerCount+variance < expectedWorkerCount ||
					service.WorkerCount-variance > expectedWorkerCount {
					t.Fatalf("Failed for worker count in %s:%s - expected/actual %d/%d",
						strings.TrimSpace(db.DBUniqueName),
						strings.TrimSpace(service.ServiceName),
						expectedWorkerCount, service.WorkerCount)
				} else {
					fmt.Printf("Validation for %s:%s - Success:\n ExpectedWorker Count: %d\n Actual WorkerCount(allowed Variance): "+
						"%d(%d)", strings.TrimSpace(db.DBUniqueName), strings.TrimSpace(service.ServiceName),
						expectedWorkerCount, service.WorkerCount, variance)
				}
			}
		}
	}
	if expectedWorkerCount >= 0 && !foundWorker {
		t.Fatalf("Failed for worker count in %s:%s, expectedWorkerCount: %d", strings.TrimSpace(dbUniqueName),
			strings.TrimSpace(serviceName), expectedWorkerCount)
	}

	if expectedWorkerCount == -1 && foundWorker {
		t.Fatalf("Service should not be up and running %s:%s", strings.TrimSpace(dbUniqueName),
			strings.TrimSpace(serviceName))
	}
}
