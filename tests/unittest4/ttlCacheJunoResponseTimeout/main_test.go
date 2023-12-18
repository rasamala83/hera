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
	appcfg["caching_cfg_reload_interval"] = "60"
	appcfg["cache_endpoint"] = "10.176.9.146:5080"
	appcfg["cache_by_corrid"] = "false"
	appcfg["cache_response_timeout_ms"] = "1"

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

func TestTTLCacheJunoResponseTimeout(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestTTLCacheJunoResponseTimeout begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	testutil.RunDML("DELETE from hera_sql_caching")
	testutil.RunDML("INSERT into hera_sql_caching (query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, remarks, hera_module) VALUES  ('1', '2580005598', 'MyTestQuery', 'abc=123', 30, 'N', 'MyTestTable', '', 'Y', '', 'hera-test')")

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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	conn, err := db.Conn(ctx);
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}
	
	rows, _ := conn.QueryContext(ctx, "SELECT 'abc' from dual")
	
	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}
	rows.Close()

	if testutil.RegexCountFile("2580005598 CachingEnabled for  GET : true", "hera.log") < 1 {
		t.Fatalf("Error: should have entered this block")
	}

	if testutil.RegexCountFile("coordinator dorequest: starting", "hera.log") < 1 {
		t.Fatalf("Error: should have dispatched the request to database")
	}

	if testutil.RegexCountFile("2580005598 CachingEnabled for  SET : true", "hera.log") < 1 {
		t.Fatalf("Error: should have entered this block")
	}

	if testutil.RegexCountFile("coordinator DispatchCachingSession for SET returned:.*response timeout", "hera.log") < 1 {
		t.Fatalf("Error: should have exited from CachingSession for SET with response timeout")
	}

	// INSERT + UPDATE + CacheCfg query and the select query should be sent to the database)
	if testutil.RegexCountFile("T.*CLIENT_SESSION.*", "cal.log") < 4 {
		t.Fatalf("Error: all the requests should have been sent to the database")
	}

	if testutil.RegexCountFile(".*EXEC\t2580005598.*", "cal.log") < 1 {
		t.Fatalf("Error: query should be sent to the database")
	}

	if testutil.RegexCountFile("T.*SET\t2580005598\t2.*response timeout", "cal.log") < 1 {
		t.Fatalf("Error: SET should have failed")
	}

	cancel()
	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestTTLCacheJunoResponseTimeout done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}