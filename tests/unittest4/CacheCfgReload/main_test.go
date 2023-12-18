package main

import (
	
	"os"
	"testing"
	"time"
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
	appcfg["caching_cfg_reload_interval"] = "2"

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

func TestCacheCfgReload(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestCacheCfgReload begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	
	testutil.RunDML("DELETE from hera_sql_caching")

	time.Sleep(5*time.Second)

	if testutil.RegexCountFile("Loaded 0 sqlhashes, 0 cacheCfg entries", "hera.log") < 1 {
		t.Fatalf("Error: should not have cacheCfg entries...table is empty")
	}

	if testutil.RegexCountFile("cacheCfgRecord size inside routine: 0", "hera.log") < 1 {
		t.Fatalf("Error: should not have cacheCfg entries...table is empty")
	}

	testutil.RunDML("INSERT into hera_sql_caching (query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, remarks, hera_module) VALUES  ('1', '1774480566', 'MyTestQuery', 'abc=123', 30, 'N', 'MyTestTable', '', 'Y', '', 'hera-test')")
	testutil.RunDML("INSERT into hera_sql_caching (query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, remarks, hera_module) VALUES  ('2', '1774480567', 'MyTestQuery2', 'xyz=123', 30, 'N', 'MyTestTable', '', 'Y', '', 'hera-test')")

	time.Sleep(5*time.Second)

	if testutil.RegexCountFile("Loaded 2 sqlhashes, 2 cacheCfg entries", "hera.log") < 1 {
		t.Fatalf("Error: should have two cacheCfg entries...")
	}

	if testutil.RegexCountFile("cacheCfgRecord size inside routine: 2", "hera.log") < 1 {
		t.Fatalf("Error: should have two cacheCfg entries...")
	}

	testutil.RunDML("DELETE from hera_sql_caching WHERE sqlhash = '1774480567'")

	time.Sleep(5*time.Second)

	if testutil.RegexCountFile("Loaded 1 sqlhashes, 1 cacheCfg entries", "hera.log") < 1 {
		t.Fatalf("Error: should have reloaded and have just one cacheCfg entry...")
	}

	if testutil.RegexCountFile("cacheCfgRecord size inside routine: 1", "hera.log") < 1 {
		t.Fatalf("Error: should have reloaded and have just one cacheCfg entry...")
	}

	logger.GetLogger().Log(logger.Debug, "TestCacheCfgReload done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}