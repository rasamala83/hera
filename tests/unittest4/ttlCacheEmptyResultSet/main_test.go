package main

import (
	
	"os"
	"testing"
	"database/sql"
	"context"
	"math/rand"
	"time"
	"fmt"
	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
)

var mx testutil.Mux
var tableName string
const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randSeq(n int) string {
	rand.Seed(time.Now().UnixNano())
    b := make([]byte, n)
    for i := range b {
        b[i] = letters[rand.Intn(len(letters))]
    }
    return string(b)
}

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// appcfg["x-mysql"] = "manual" // disable test framework spawning mysql server
	// best to chose an "unique" port in case golang runs tests in paralel
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["db_heartbeat_interval"] = "10"
	appcfg["enable_caching"] = "true"
	appcfg["caching_cfg_reload_interval"] = "60"
	appcfg["cache_endpoint"] = "10.176.9.146:5080"
	appcfg["cache_by_corrid"] = "false"
	appcfg["cache_response_timeout_ms"] = "10000"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"
	opscfg["opscfg.default.server.max_lifespan_per_child"]="5"

	appcfg["child.executable"] = "mysqlworker"

	if os.Getenv("WORKER") == "postgres" {
		return appcfg, opscfg, testutil.PostgresWorker
	}

	return appcfg, opscfg, testutil.MySQLWorker
}

func before() error {
	testutil.RunDML("drop table testEmptyResultSet")
	testutil.RunDML("create table testEmptyResultSet (id INT, val MEDIUMTEXT)")
	return nil
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, before))
}
// GET (Cache MISS), Response from DB, SET
func TestTTLCacheEmptyResultSet(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestTTLCacheEmptyResultSet begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML("DELETE from hera_sql_caching")
	testutil.RunDML("INSERT into hera_sql_caching (query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, remarks, hera_module) VALUES  ('1', '619648305', 'SelectQueryWithEmptyResultSet', 'abc=123', 30, 'N', 'MyTestTable', '', 'Y', '', 'hera-test')")

	time.Sleep(1*time.Second)
	

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	conn, err := db.Conn(ctx);
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}

	time.Sleep(5*time.Second)
	
	rows, _ := conn.QueryContext(ctx, "select val from testEmptyResultSet")
	
	if rows.Next() {
		t.Fatalf("Expected empty result")
	}
	rows.Close()

	if testutil.RegexCountFile(".*GET\t619648305\t2.*error: no key", "cal.log") < 1 {
		t.Fatalf("Error: GET should fail with key not found")
	}

	if testutil.RegexCountFile(".*EXEC\t619648305\t0", "cal.log") < 1 {
		t.Fatalf("Error: query should be sent to the database")
	}

	if testutil.RegexCountFile(".*SET\t619648305\t0", "cal.log") < 1 {
		t.Fatalf("Error: SET should succeed")
	}


	cancel()
	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestTTLCacheEmptyResultSet done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}


func TestTTLCacheHitEmptyResultSet(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestTTLCacheHitEmptyResultSet begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	conn, err := db.Conn(ctx);
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	
	rows, _ := conn.QueryContext(ctx, "select val from testEmptyResultSet")
	
	if rows.Next() {
		t.Fatalf("Expected empty result")
	}
	rows.Close()

	time.Sleep(2*time.Second)

	// GET should fail with key not found
	if testutil.RegexCountFile(".*GET\t619648305\t0.*", "cal.log") < 1 {
		t.Fatalf("Error: GET should be a cache hit")
	}

	if testutil.RegexCountFile(".*EXEC\t619648305\t0", "cal.log") != 1 {
		t.Fatalf("Error: query should not be sent to the database")
	}

	
	cancel()
	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestTTLCacheHitEmptyResultSet done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

}