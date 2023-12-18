package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

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
	appcfg["caching_cfg_reload_interval"] = "60"
	appcfg["cache_endpoint"] = "10.176.9.146:5080"
	// appcfg["idle_timeout_ms"] = "1000"
	appcfg["cache_connection_pool_size"] = "10"
	appcfg["cache_response_timeout_ms"] = "10000"
	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.idle_timeout_ms"] = "100"
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"
	opscfg["opscfg.default.server.max_lifespan_per_child"] = "5"

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

	rows, _ := conn.QueryContext(ctx, "SELECT 'xyz' from dual")
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
func TestCacheSessionClientDisconnect(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestCacheSessionClientDisconnect begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML("DELETE from hera_sql_caching")
	testutil.RunDML("INSERT into hera_sql_caching (query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, remarks, hera_module) VALUES  ('1', '1397234510', 'MyTestQuery', 'abc=123', 10, 'N', 'MyTestTable', '', 'Y', '', 'hera-test')")

	time.Sleep(5 * time.Second)

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

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	mux := gosqldriver.InnerConn(conn)
	mux.SetCalCorrID("5af5e4a2758e")

	conn.QueryContext(ctx, "SELECT 'xyz' from dual")
	// if err != nil {
	// 	t.Fatalf("Error QueryContext %s\n", err.Error())
	// 	return
	// }

	cancel()
	conn.Close()

	if testutil.RegexCountFile("1397234510 CachingEnabled for  GET : true", "hera.log") < 2 {
		t.Fatalf("Error: should have entered this block")
	}

	if testutil.RegexCountFile("coordinator doCacheRequest: starting", "hera.log") < 2 {
		t.Fatalf("Error: should have entered doCacheRequest when caching is enabled")
	}

	if testutil.RegexCountFile("Trying GET with key", "hera.log") < 2 {
		t.Fatalf("Error: should have entered getRecordFromCache when caching is enabled")
	}

	if testutil.RegexCountFile("doCacheRequest: request canceled", "hera.log") < 1 {
		t.Fatalf("Error: client request should be canceled... Should be captured by cache session")
	}

	if testutil.RegexCountFile(".*doCacheRequest\tclient_req_canceled.*", "cal.log") < 1 {
		t.Fatalf("Error: should see client_req_canceled event when request is canceled")
	}

	// cancel()
	// conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestCacheSessionClientDisconnect done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}
