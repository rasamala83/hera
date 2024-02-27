package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
	"os"
	"strings"
	"testing"
	"time"
)

var mx testutil.Mux
var tableName string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appcfg := make(map[string]string)
	// appcfg["x-mysql"] = "manual" // disable test framework spawning mysql server
	// best to chose an "unique" port in case golang runs tests in paralel
	cacheHost, ok := os.LookupEnv("CACHE_HOST")
	if !ok {
		cacheHost = "localhost"
	}
	cacheCertsPath, ok := os.LookupEnv("CACHE_CERTS_PATH")
	if !ok {
		cacheCertsPath, _ = os.Getwd()
	}
	appcfg["cache_cert_file_path"] = cacheCertsPath
	appcfg["cache_endpoint"] = fmt.Sprintf("%s:5080", cacheHost)
	appcfg["bind_port"] = "31002"
	appcfg["log_level"] = "5"
	appcfg["log_file"] = "hera.log"
	appcfg["sharding_cfg_reload_interval"] = "0"
	appcfg["rac_sql_interval"] = "0"
	appcfg["db_heartbeat_interval"] = "10"
	appcfg["enable_caching"] = "true"
	appcfg["caching_cfg_reload_interval"] = "300"
	appcfg["cache_response_timeout_ms"] = "3000"
	appcfg["cache_by_corrid"] = "false"
	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "5"
	opscfg["opscfg.default.server.log_level"] = "5"
	opscfg["opscfg.default.server.max_lifespan_per_child"] = "500"

	appcfg["child.executable"] = "mysqlworker"

	if os.Getenv("WORKER") == "postgres" {
		return appcfg, opscfg, testutil.PostgresWorker
	}

	return appcfg, opscfg, testutil.MySQLWorker
}

func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func before() error {
	tableName = "jdbc_hera_cache_txn_test"

	if strings.HasPrefix(os.Getenv("TWO_TASK"), "tcp") {
		testutil.DBDirect("create table hera_sql_caching(query_id varchar(30),sqlhash varchar(40),sqltext varchar(4000),"+
			"bind_variables varchar(1000),TTL_sec BIGINT,enable_shadow_test varchar(1),tableName varchar(30),"+
			"invalidation_clause varchar(1000),caching_enabled varchar(1),remarks varchar(4000),hera_module varchar(100))", os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL)
		testutil.DBDirect("create table jdbc_hera_cache_txn_test ( ID BIGINT, INT_VAL BIGINT, STR_VAL VARCHAR(500))", os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL)

		testutil.DBDirect("INSERT into hera_sql_caching (query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, remarks, hera_module) VALUES  ('1', '2733177372', 'select id, int_val from jdbc_hera_cache_txn_test where id=?', 'id=1', 30, 'N', 'jdbc_hera_cache_taf_test', '', 'Y', '', 'hera-test')", os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL)
	}
	return nil
}

// GET (Cache MISS) + SET
func TestTTLCacheWithTxnStartFlow(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestTTLCacheWithTxnStartFlow begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML("/*cmd*/insert into " + tableName + "(id, int_val, str_val) VALUES(1," + fmt.Sprint(time.Now().Unix()) + ", \"val 1\")")
	time.Sleep(5 * time.Second)

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := db.Conn(ctx)
	defer conn.Close()
	if err != nil {
		t.Fatalf("error in preparing test %v", err)
	}
	tx, _ := conn.BeginTx(ctx, nil)
	stmt, _ := conn.PrepareContext(ctx, "/*cmd*/select id, int_val from "+tableName+" where id=?")
	//rows, err := conn.QueryContext(ctx, "SELECT VERSION()")
	rows, err := stmt.QueryContext(ctx, 1)

	if err != nil {
		t.Fatalf("extected one row but received error %v", err)
	}
	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("during commit, received error %v", err)
	}
	rows.Close()
	//stmt.Close()
	time.Sleep(5 * time.Second)
	if testutil.RegexCountFile("coordinator doCacheRequest: starting", "hera.log") < 1 {
		t.Fatalf("Error: should have entered doCacheRequest when caching is enabled")
	}

	if testutil.RegexCountFile("Trying GET with key", "hera.log") < 1 {
		t.Fatalf("Error: should have entered getRecordFromCache when caching is enabled")
	}

	if testutil.RegexCountFile("coordinator DispatchCachingSession for GET returned: error: no key", "hera.log") < 1 {
		t.Fatalf("Error: should be a cache miss for the first read")
	}

	if testutil.RegexCountFile("2733177372 CachingEnabled for  SET : true", "hera.log") < 1 {
		t.Fatalf("Error: should have entered this block")
	}

	if testutil.RegexCountFile("Trying SET with key", "hera.log") < 1 {
		t.Fatalf("Error: should have entered setRecordToCache when caching is enabled")
	}

	if testutil.RegexCountFile(".*SET.*2733177372", "cal.log") < 1 {
		t.Fatalf("Error: should see SET when cacheCfgRecord is enabled for caching")
	}

	logger.GetLogger().Log(logger.Debug, "TestTTLCacheWithTxnStartFlow done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}
