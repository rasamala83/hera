package main

import (
	
	"os"
	"testing"
	"database/sql"
	"context"
	"time"
	"fmt"
	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
)

var mx testutil.Mux
var tableName string

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
	appcfg["caching_cfg_reload_interval"] = "1"
	appcfg["cache_endpoint"] = "10.176.9.146:5080"
	appcfg["cache_by_corrid"] = "false"

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


func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, nil))
}

func TestTTLCacheUpdateTTLCfg(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestTTLCacheUpdateTTLCfg begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML("DELETE from hera_sql_caching")
	testutil.RunDML("INSERT into hera_sql_caching (query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, remarks, hera_module) VALUES  ('1', '2904134799', 'MyTestQuery', 'abc=123', 3, 'N', 'MyTestTable', '', 'Y', '', 'hera-test')")

	time.Sleep(5*time.Second)

	if testutil.RegexCountFile("Loaded 1 sqlhashes, 1 cacheCfg entries", "hera.log") < 1 {
		t.Fatalf("Error: should have loaded the cacheCfg entry...")
	}

	if testutil.RegexCountFile("cacheCfgRecord size inside routine: 1", "hera.log") < 1 {
		t.Fatalf("Error: should have loaded the cacheCfg entry...")
	}

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
	
	rows, _ := conn.QueryContext(ctx, "SELECT version()")
	
	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}
	rows.Close()

	if testutil.RegexCountFile("Trying SET with key", "hera.log") < 1 {
		t.Fatalf("Error: should have entered setRecordToCache when caching is enabled")
	}

	if testutil.RegexCountFile("creationTime.*ttl=3", "hera.log") < 1 {
		t.Fatalf("Error: should have set the record with ttl=3 seconds")
	}


	cancel()
	conn.Close()


	testutil.RunDML("UPDATE hera_sql_caching SET TTL_sec=120 where sqlhash='2904134799'")

	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	conn, err = db.Conn(ctx);
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}

	time.Sleep(5*time.Second)
	
	rows, _ = conn.QueryContext(ctx, "SELECT version()")
	
	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}	

	if testutil.RegexCountFile("creationTime.*ttl=120", "hera.log") < 1 {
		t.Fatalf("Error: should have set the record with ttl=120 seconds")
	}

	cancel()
	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestTTLCacheUpdateTTLCfg done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}