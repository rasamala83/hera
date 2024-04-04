package util

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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

var maxRetryCount = 5
var binaryPushDone = false
var READ = "ReadType"
var WRITE = "WriteType"
var TXN = "TXNType"
var STOP = "stop"
var DumpLogs = "dumpLogs"
var CutOverEnable = "CUT_OVER_ENABLE"
var CutOverPre = "CUT_OVER_PRE"
var CutOverPhaseI = "CUT_OVER_PHASE_1"
var CutOverPhaseII = "CUT_OVER_PHASE_2"
var CutOverPhaseIII = "CUT_OVER_PHASE_3"
var CutOverComplete = "CUT_OVER_COMPLETE"

var heraBoxHost = "10.183.162.56"

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
	DBConnection  TestConnection
	DBTransaction *sql.Tx
}

func dbService(dbUniqueName string, serviceName string, action string) []DBStatus {
	var status []DBStatus

	response, err := http.Get("http://" + heraBoxHost + ":8000/occ/db_service?action=" + action +
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

func splitBySpace(input string) []string {
	words := strings.Split(input, " ")
	var fields []string

	for _, word := range words {
		if word != "" {
			fields = append(fields, word)
		}
	}
	return fields
}

func ValidateStateLog(t *testing.T, expected map[string]int) {
	fmt.Println("Validating State Logs")
	retryCount := 0
	for {
		url := "http://" + heraBoxHost + ":8000/occ/logs?path=/x/web/LIVE/occ/state-logs/current"
		response, err := http.Get(url)
		if err != nil {
			t.Fatalf(err.Error())
		}
		byteArray, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatalf(err.Error())
		}
		str := string(byteArray)
		buf := bytes.NewBufferString(str)
		scanner := bufio.NewScanner(buf)
		for scanner.Scan() {
			line := scanner.Text()
			words := splitBySpace(line)
			expectedWorkerCount, exists := expected[words[2]]
			if exists {
				accept, _ := strconv.Atoi(words[4])
				wait, _ := strconv.Atoi(words[5])
				if expectedWorkerCount != accept+wait && retryCount == maxRetryCount {
					t.Fatalf("State Log Validation failed for %s at %s %s - Expected Worker Count: %d vs Actual %d",
						words[2], words[0], words[1], expectedWorkerCount, accept+wait)
				} else if expectedWorkerCount == accept+wait {
					delete(expected, words[2])
				} else if expectedWorkerCount != accept+wait && retryCount < maxRetryCount {
					fmt.Printf("State Log Validation failed for %s at %s %s - "+
						"Expected Worker Count: %d vs Actual %d - retrying after 5 seconds\n",
						words[2], words[0], words[1], expectedWorkerCount, accept+wait)
					break
				}
			}
		}
		if retryCount > maxRetryCount || len(expected) == 0 {
			break
		}
		retryCount += 1
		time.Sleep(5 * time.Second)
		fmt.Println("Retry - Validating State Logs")
	}
	for key := range expected {
		t.Fatalf("Unable to find state log for %s", key)
	}
}

func MoveCutOverPhase(t *testing.T, phase string, comment string) {
	switch phase {

	case CutOverEnable:
		CleanCutOverTable(t)
		query := "insert into pypl_occ_cutover values ('HERADB_ONE', 'occ', 'CLOC', 'Y', 'Y', 'ENABLE', '" +
			comment + "');\\n" +
			"insert into pypl_occ_cutover values ('HERADB_TWO', 'occ', 'CLOC_CUTOVER', 'N', 'N', 'ENABLE', '" +
			comment + "')"
		QueryOracle(t, query, "False", "False")
		QueryOracle(t, query, "True", "False")
		break

	case CutOverPre:
		query := "update pypl_occ_cutover set cutover_phase='PRE', remarks='" + comment +
			"' where dbuname='HERADB_ONE' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='PRE', remarks='" + comment +
			"' where dbuname='HERADB_TWO' and occ_name='occ'\\n"
		QueryOracle(t, query, "False", "False")
		QueryOracle(t, query, "True", "False")
		break

	case CutOverPhaseI:
		query := "update pypl_occ_cutover set cutover_phase='CUTOVER', write_status='N', remarks='" + comment +
			"' where dbuname='HERADB_ONE' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set cutover_phase='CUTOVER', write_status='N', remarks='" + comment +
			"' where dbuname='HERADB_TWO' and occ_name='occ'"
		QueryOracle(t, query, "False", "False")
		QueryOracle(t, query, "True", "False")
		break

	case CutOverPhaseII:
		query := "update pypl_occ_cutover set read_status='N', remarks='" + comment +
			"' where dbuname='HERADB_ONE' and occ_name='occ';\\n" +
			"update pypl_occ_cutover set read_status='Y', remarks='" + comment +
			"' where dbuname='HERADB_TWO' and occ_name='occ'"
		QueryOracle(t, query, "False", "False")
		QueryOracle(t, query, "True", "False")
		break

	case CutOverPhaseIII:
		query := "update pypl_occ_cutover set write_status='Y', remarks='" + comment +
			"' where dbuname='HERADB_TWO' and occ_name='occ'"
		QueryOracle(t, query, "False", "False")
		QueryOracle(t, query, "True", "False")
		break

	case CutOverComplete:
		query := "update pypl_occ_cutover set cutover_phase='COMPLETE', dbuname='HERADB_TWO', remarks='" + comment +
			"' where occ_name='occ';"
		QueryOracle(t, query, "False", "False")
		QueryOracle(t, query, "True", "False")
		break

	default:
		t.Fatalf("Unknown cutover phase %s.\n", phase)
	}
}

func OCCConfig(t *testing.T, key string, value string, filename string) {
	fmt.Printf("Changing occ config in file %s: key: %s, value: %s\n", filename, key, value)
	url := "http://" + heraBoxHost + ":8000/occ/occ_config?key=" + key + "&value=" + value + "&filename=" + filename
	response, err := http.Get(url)
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func OCCBinarySetup(t *testing.T, filename string) {
	if binaryPushDone {
		return
	}
	binaryPushDone = true
	fmt.Printf("Pushing mux binary from %s\n", filename)
	client := &http.Client{
		Timeout: time.Second * 60,
	}

	url := "http://" + heraBoxHost + ":8000?filename=mux"
	b, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf(err.Error())
	}

	retryCount := 0

	for {
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(b))
		if err != nil {
			t.Fatalf(err.Error())
		}

		req.Header.Set("Content-Type", "application/octet-stream")
		resp, _ := client.Do(req)
		if resp == nil {
			if retryCount >= maxRetryCount {
				t.Fatalf("unable to push the binary to Herabox setup")
			}
			retryCount += 1
			fmt.Println("sleeping 5 sec before retrying")
			time.Sleep(5 * time.Second)
			continue
		}
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
		break
	}
}

func StopOCCDocker(t *testing.T) {
	fmt.Println("Stopping occ docker")
	url := "http://" + heraBoxHost + ":8000/docker_support?container=occ&action=stop"
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
	fmt.Println("Starting occ docker")
	url := "http://" + heraBoxHost + ":8000/docker_support?container=occ&action=start"
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
	url := "http://" + heraBoxHost + ":8000/occ/restart_occ"
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
	url := "http://" + heraBoxHost + ":8000/run_query?cut_over=" + cutOver + "&dba_user=" + dbaUser
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
	url := "http://" + heraBoxHost + ":8000/occ/lock_user?cut_over=" + co
	if action == "unlock" {
		url = "http://" + heraBoxHost + ":8000/occ/unlock_user?cut_over=" + co
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
	response, err := http.Get("http://" + heraBoxHost + ":8000/\"default_tns\"")
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

// EnableCutOver /*
/*
 1. stop occ docker
 2. push tns with cut over details
 3. enable cut over env variable for occ
 4. start occ docker
 5. copy the new mux binary
 6. disable/enable read/write split
 7. restart occ with sig hup command
*/
func EnableCutOver(t *testing.T, enableRWSplit bool) {
	fmt.Println("EnableCutOver")
	response, err := http.Get("http://" + heraBoxHost + ":8000/enable_cut_over")
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if !enableRWSplit {
		OCCConfig(t, "readonly_children_pct", "0", "/x/web/LIVE/occ/occ.cdb")
	}
	EnableDebugLog(t)
	MoveCutOverPhase(t, CutOverEnable, "TestCutOverEnable")
	RestartOCC(t)
}

func EnableDebugLog(t *testing.T) {
	OCCConfig(t, "log_level", "4", "/x/web/LIVE/occ/occ.cdb")
	OCCConfig(t, "log_level", "4", "/x/web/LIVE/occ/hera.txt")
	OCCConfig(t, "opscfg.occ.server.log_level", "4", "/x/web/LIVE/opscfg/occ.cdb")
}

func PushTNSForComplete(t *testing.T) {
	fmt.Println("PushTNSForComplete")
	response, err := http.Get("http://" + heraBoxHost + ":8000/enable_cut_over?always_secondary=True")
	if err != nil {
		t.Fatalf(err.Error())
	}
	_, err = io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

// ResetOCCDocker /**
/*
1. Stop OCC Docker container
2. Push default TNS File (without cutover)
3. Disable CLOC_CUTOVER env variable to disable cutover
4. Start OCC Docker
*/
func ResetOCCDocker(t *testing.T) {
	fmt.Println("ResetOCCDocker")
	response, err := http.Get("http://" + heraBoxHost + ":8000/reset")
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
	response, err := http.Get("http://" + heraBoxHost + ":8000/occ/kill_session?cut_over=" + co + "&service_name=" + serviceName)
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
	response, err := http.Get("http://" + heraBoxHost + ":8000/occ/status_from_db")
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

func ValidateReadWorkerID(ReadWaitTime int, expectedId int, wg *sync.WaitGroup) {
	c := TestConnection{}
	c.GetConnection()
	if c.Err != nil {
		panic(c.conn)
	}
	defer c.Close()
	txn, err := c.conn.BeginTx(c.context, nil)
	if err != nil {
		panic(err)
	}
	wg.Add(1)
	go slowQuery(txn, expectedId, ReadWaitTime, wg)
}

func writeBeginTxn() *DBTxn {
	c := TestConnection{}
	c.GetConnection()
	if c.Err != nil {
		panic(c.Err)
	}
	defer c.Close()
	txn, err := c.conn.BeginTx(c.context, nil)
	if err != nil {
		panic(err)
	}
	insertQuery := "insert into occ_test values(id_seq.NEXTVAL, 'hold-record', 1)"
	_, err = txn.ExecContext(c.context, insertQuery)
	if err != nil {
		txn.Rollback()
		panic(err)
	}

	return &DBTxn{DBConnection: c, DBTransaction: txn}
}

func validateDBID(dbTxn *DBTxn, dbId int, dbName string) {

	query := "select * from db_id_test"
	stmt, err := dbTxn.DBTransaction.PrepareContext(dbTxn.DBConnection.context, query)
	if err != nil {
		panic(err)
	}
	rows, err := stmt.QueryContext(dbTxn.DBConnection.context)
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

func sortStats(trafficStats map[int64]ClientTrafficStats) []int64 {
	keys := make([]int64, 0)
	for k, _ := range trafficStats {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

func ValidateFailureTraffic(t *testing.T, trafficStats map[int64]ClientTrafficStats, queryType string,
	startTime int64, endTime int64) {
	fmt.Printf("Validating Failure traffic for %s from %d to %d\n", queryType, startTime, endTime)
	for _, utc := range sortStats(trafficStats) {

		if utc < startTime || utc > endTime {
			continue
		}

		cts := trafficStats[utc]

		for dbId := 1; dbId < 3; dbId++ {
			if cts.stats[queryType][dbId].successCount != 0 {
				t.Fatalf("UTC: %d, Expected %s Query to fail but found in db %d", utc, queryType, dbId)
			}
		}

		if cts.stats[queryType][0].failureCount == 0 {
			t.Fatalf("UTC: %d, Expected failure for %s - but no failure found", utc, queryType)
		}

	}
}

func ValidateSuccessTraffic(t *testing.T, trafficStats map[int64]ClientTrafficStats, queryType string,
	startTime int64, endTime int64, dbIdWithTraffic int, dbIdNoTraffic int) {

	fmt.Printf("Validating Success traffic for %s in DB:%d from %d to %d\n", queryType, dbIdWithTraffic, startTime, endTime)
	for _, utc := range sortStats(trafficStats) {

		if utc < startTime || utc > endTime {
			continue
		}

		cts := trafficStats[utc]
		if cts.stats[queryType][dbIdWithTraffic].successCount == 0 {
			t.Fatalf("UTC: %d, Expected %s Query in DB: %d - but not found", utc, queryType, dbIdWithTraffic)
		}

		if cts.stats[queryType][dbIdWithTraffic].failureCount != 0 {
			t.Fatalf("UTC: %d, Expected %s Query to succeed in DB: %d - but failed", utc, queryType, dbIdWithTraffic)
		}

		if cts.stats[queryType][dbIdNoTraffic].successCount != 0 {
			t.Fatalf("UTC: %d, Not expecting %s Query in DB: %d - but found", utc, queryType, dbIdWithTraffic)
		}

		if cts.stats[queryType][dbIdNoTraffic].failureCount != 0 {
			t.Fatalf("UTC: %d, Not expecting %s Query in DB: %d - but found as failures", utc, queryType, dbIdWithTraffic)
		}

		if cts.stats[queryType][0].failureCount > 0 || cts.stats[queryType][1].failureCount > 0 {
			t.Fatalf("UTC: %d, Expected No %s failure. DB1: %d failed, DB2: %d failed", utc, queryType,
				cts.stats[queryType][0].failureCount, cts.stats[queryType][1].failureCount)
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

func ValidateWorkerCountFromDatabase(dbUniqueName string, serviceName string,
	expectedWorkerCount int, t *testing.T) {
	fmt.Printf("Validating %s:%s has %d workers connected to db\n",
		dbUniqueName, serviceName, expectedWorkerCount)
	retryCount := 0
	retry := false
	validationSuccess := false
	workerFound := false

	// retry in case of temp failures
	for {
		validationSuccess = false
		workerFound = false
		// load status from database
		databaseServices, _ := GetDBStatus()

		// for each database
		for _, db := range databaseServices {

			// for each database service
			for _, service := range db.DatabaseServices {
				if strings.TrimSpace(db.DBUniqueName) == dbUniqueName &&
					strings.TrimSpace(service.ServiceName) == serviceName && service.Active {
					workerFound = true
					if service.WorkerCount != expectedWorkerCount {
						if retryCount >= maxRetryCount {
							t.Fatalf("Failed for worker count in %s:%s - expected/actual %d/%d",
								strings.TrimSpace(db.DBUniqueName),
								strings.TrimSpace(service.ServiceName),
								expectedWorkerCount, service.WorkerCount)
						} else {
							retry = true
							fmt.Printf("Retry: Failed for worker count in %s:%s - expected/actual %d/%d\n",
								strings.TrimSpace(db.DBUniqueName),
								strings.TrimSpace(service.ServiceName),
								expectedWorkerCount, service.WorkerCount)
							break
						}
					} else {
						fmt.Printf("Validation for %s:%s - Success:\n ExpectedWorker Count: %d\n Actual WorkerCount: "+
							"%d\n", strings.TrimSpace(db.DBUniqueName), strings.TrimSpace(service.ServiceName),
							expectedWorkerCount, service.WorkerCount)
						validationSuccess = true
						break
					}
				}
			}
			if retry || validationSuccess {
				break
			}
		}

		if retryCount >= maxRetryCount || validationSuccess {
			break
		}
		retryCount += 1
		fmt.Println("Sleeping for 5 seconds and retrying validation")
		time.Sleep(5 * time.Second)
	}
	if expectedWorkerCount >= 0 && !validationSuccess {
		t.Fatalf("Failed for worker count in %s:%s, expectedWorkerCount: %d", strings.TrimSpace(dbUniqueName),
			strings.TrimSpace(serviceName), expectedWorkerCount)
	}

	if expectedWorkerCount == -1 && workerFound {
		t.Fatalf("Service should not be up and running %s:%s", strings.TrimSpace(dbUniqueName),
			strings.TrimSpace(serviceName))
	}
}
