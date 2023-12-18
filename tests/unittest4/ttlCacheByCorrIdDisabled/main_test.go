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
	"github.com/paypal/hera/client/gosqldriver"
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
	appcfg["cache_by_corrid"] = "false"
	appcfg["caching_cfg_reload_interval"] = "60"
	appcfg["cache_response_timeout_ms"] = "3000"
	appcfg["cache_endpoint"] = "10.176.9.146:5080"
	

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

func populateCache() error {

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		return err
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	conn, err := db.Conn(ctx);
	if err != nil {
		return err
	}
	mux := gosqldriver.InnerConn(conn)
	mux.SetCalCorrID("5af5e4a2758e")

	rows, _ := conn.QueryContext(ctx, "SELECT version()")
	
	if !rows.Next() {
		return fmt.Errorf("Expected 1 row")
	}
	rows.Close()
	cancel()
	conn.Close()
	return err
}


func TestMain(m *testing.M) {
	os.Exit(testutil.UtilMain(m, cfg, nil))
}
// GET (Cache MISS) + SET
func TestTTLCacheByCorridDisabled(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestTTLCacheByCorridDisabled begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML("DELETE from hera_sql_caching")
	testutil.RunDML("INSERT into hera_sql_caching (query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, remarks, hera_module) VALUES  ('1', '2904134799', 'MyTestQuery', 'abc=123', 60, 'N', 'MyTestTable', '', 'Y', '', 'hera-test')")

	time.Sleep(5*time.Second)

	if testutil.RegexCountFile("Loaded 1 sqlhashes, 1 cacheCfg entries", "hera.log") < 1 {
		t.Fatalf("Error: should have loaded the cacheCfg entry...")
	}

	if testutil.RegexCountFile("cacheCfgRecord size inside routine: 1", "hera.log") < 1 {
		t.Fatalf("Error: should have loaded the cacheCfg entry...")
	}

	err := populateCache()

	if err != nil {
		t.Fatal("Error populating cache:", err)
		return
	}

	if testutil.RegexCountFile("2904134799 CachingEnabled for  GET : true", "hera.log") < 1 {
		t.Fatalf("Error: should have entered this block")
	}

	if testutil.RegexCountFile("coordinator DispatchCachingSession for GET returned: error: no key", "hera.log") < 1 {
		t.Fatalf("Error: should be a cache miss for the first read")
	}

	if testutil.RegexCountFile("T.*CACHE_SESSION.*", "cal.log") < 1 {
		t.Fatalf("Error: should see CACHE_SESSION when cacheCfgRecord is enabled for caching")
	}

	if testutil.RegexCountFile("coordinator dispatchrequest", "hera.log") < 4 {
		t.Fatalf("Error: should have dispatched the request to database")
	}

	if testutil.RegexCountFile("2904134799 CachingEnabled for  SET : true", "hera.log") < 1 {
		t.Fatalf("Error: should have entered this block")
	}

	if testutil.RegexCountFile("Trying SET with key", "hera.log") < 1 {
		t.Fatalf("Error: should have entered setRecordToCache when caching is enabled")
	}

	// GET should fail with no key
	if testutil.RegexCountFile(".*GET\t2904134799\t2.*", "cal.log") < 1 { 
		t.Fatalf("Error: should see GET when cacheCfgRecord is enabled for caching")
	}

	if testutil.RegexCountFile(".*SET\t2904134799\t0.*", "cal.log") < 1 { 
		t.Fatalf("Error: should see SET when cacheCfgRecord is enabled for caching")
	}

	if testutil.RegexCountFile(".*EXEC\t2904134799\t0.*", "cal.log") < 1 { 
		t.Fatalf("Error: query should be sent to the database")
	}

	time.Sleep(2*time.Second)

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

	time.Sleep(3*time.Second)

	if testutil.RegexCountFile("2904134799 CachingEnabled for  GET : true", "hera.log") < 2 {
		t.Fatalf("Error: should have entered this block")
	}

	if testutil.RegexCountFile("coordinator doCacheRequest: starting", "hera.log") < 2 {
		t.Fatalf("Error: should have entered doCacheRequest when caching is enabled")
	}

	if testutil.RegexCountFile("Trying GET with key", "hera.log") < 2 {
		t.Fatalf("Error: should have entered getRecordFromCache when caching is enabled")
	}

	if testutil.RegexCountFile(".*GET\t2904134799\t0.*", "cal.log") < 1 { 
		t.Fatalf("Error: should be a cache HIT")
	}

	if testutil.RegexCountFile(".*EXEC\t2904134799\t0.*", "cal.log") < 1 { 
		t.Fatalf("Error: query should not be sent to the database")
	}

	cancel()
	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestTTLCacheByCorridDisabled done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}