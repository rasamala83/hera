package main

import (
	"context"
	"database/sql"
	"fmt"
	speedbump "github.com/kffl/speedbump/lib"
	"github.com/paypal/hera/tests/unittest/testutil"
	"github.com/paypal/hera/utility/logger"
	"os"
	"strings"
	"testing"
	"time"
)

var mx testutil.Mux
var tableName string
var appConfig map[string]string
var sb *speedbump.Speedbump
var destAddr string

func cfg() (map[string]string, map[string]string, testutil.WorkerType) {

	appConfig = make(map[string]string)
	// appConfig["x-mysql"] = "manual" // disable test framework spawning mysql server
	// best to chose an "unique" port in case golang runs tests in paralel
	cacheCertsPath, ok := os.LookupEnv("CACHE_CERTS_PATH")
	if !ok {
		cacheCertsPath, _ = os.Getwd()
	}
	fmt.Println(cacheCertsPath)
	appConfig["bind_port"] = "31002"
	appConfig["log_level"] = "5"
	appConfig["log_file"] = "hera.log"
	appConfig["sharding_cfg_reload_interval"] = "0"
	appConfig["rac_sql_interval"] = "0"
	appConfig["db_heartbeat_interval"] = "10"
	appConfig["enable_caching"] = "true"
	appConfig["caching_cfg_reload_interval"] = "60"
	appConfig["cache_response_timeout_ms"] = "3000"
	appConfig["cache_by_corrid"] = "false"
	appConfig["cache_endpoint"] = fmt.Sprintf("%s:%d", "localhost", 9090)
	appConfig["cache_ssl_enabled"] = "false"

	opscfg := make(map[string]string)
	opscfg["opscfg.default.server.max_connections"] = "3"
	opscfg["opscfg.default.server.log_level"] = "5"
	opscfg["opscfg.default.server.max_lifespan_per_child"] = "50"

	appConfig["child.executable"] = "mysqlworker"

	if os.Getenv("WORKER") == "postgres" {
		return appConfig, opscfg, testutil.PostgresWorker
	}

	return appConfig, opscfg, testutil.MySQLWorker
}

func TestMain(m *testing.M) {
	cacheHost, ok := os.LookupEnv("CACHE_HOST")
	if !ok {
		cacheHost = "localhost"
	}
	destAddr = fmt.Sprintf("%s:%d", cacheHost, 8080)
	sb, _ = testutil.StartSpeedBumpProxy(9090, destAddr, 3200, 3100, 5)
	sb.Start()
	//Accept incoming connection
	os.Exit(testutil.UtilMain(m, cfg, before))
}

func before() error {
	tableName = os.Getenv("TABLE_NAME")
	if tableName == "" {
		tableName = "jdbc_hera_cache_test"
	}
	if strings.HasPrefix(os.Getenv("TWO_TASK"), "tcp") {
		testutil.DBDirect("create table hera_sql_caching(query_id varchar(30),sqlhash varchar(40),sqltext varchar(4000),"+
			"bind_variables varchar(1000),TTL_sec BIGINT,enable_shadow_test varchar(1),tableName varchar(30),"+
			"invalidation_clause varchar(1000),caching_enabled varchar(1),remarks varchar(4000),hera_module varchar(100))", os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL)
		testutil.DBDirect("create table jdbc_hera_cache_test ( ID BIGINT, INT_VAL BIGINT, STR_VAL VARCHAR(500))", os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL)

		testutil.DBDirect("INSERT into hera_sql_caching (query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, remarks, hera_module) VALUES  ('1', '658232417', 'select id, int_val from jdbc_hera_cache_test where id=?', 'id=1', 30, 'N', 'jdbc_hera_cache_test', '', 'Y', '', 'hera-test')", os.Getenv("MYSQL_IP"), "heratestdb", testutil.MySQL)
	}
	return nil
}

func TestTTLCacheJunoTimeoutInitiallyDuringStartup(t *testing.T) {
	logger.GetLogger().Log(logger.Debug, "TestTTLCacheJunoTimeoutInitiallyDuringStartup begin +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	testutil.RunDML("/*cmd*/insert into " + tableName + "(id, int_val, str_val) VALUES(1," + fmt.Sprint(time.Now().Unix()) + ", \"val 1\")")
	time.Sleep(5 * time.Second)

	if testutil.RegexCountFile("Loaded 1 sqlhashes, 1 cacheCfg entries", "hera.log") < 1 {
		t.Fatalf("Error: should have loaded the cacheCfg entry...")
	}

	if testutil.RegexCountFile("cacheCfgRecord size inside routine: 1", "hera.log") < 1 {
		t.Fatalf("Error: should have loaded the cacheCfg entry...")
	}

	time.Sleep(10 * time.Second)

	shard := 0
	db, err := sql.Open("heraloop", fmt.Sprintf("%d:0:0", shard))
	if err != nil {
		t.Fatal("Error starting Mux:", err)
		return
	}
	db.SetMaxIdleConns(0)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}

	stmt, _ := conn.PrepareContext(ctx, "/*cmd*/select id, int_val from "+tableName+" where id=?")
	rows, err := stmt.QueryContext(ctx, 1)

	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}
	cancel()
	rows.Close()
	stmt.Close()
	conn.Close()
	time.Sleep(5 * time.Second)
	if testutil.RegexCountFile("658232417 CachingEnabled for  GET : true", "hera.log") < 1 {
		t.Fatalf("Error: should have entered this block")
	}

	if testutil.RegexCountFile("658232417 CachingEnabled for  SET : true", "hera.log") < 1 {
		t.Fatalf("Error: should have entered this block")
	}

	if testutil.RegexCountFile("coordinator DispatchCachingSession for GET returned: error:.*response timeout", "hera.log") < 1 {
		t.Fatalf("Error: should have exited from CachingSession for SET with response timeout")
	}

	if testutil.RegexCountFile("coordinator DispatchCachingSession for SET returned:.*response timeout", "hera.log") < 1 {
		t.Fatalf("Error: should have exited from CachingSession for SET with response timeout")
	}

	if testutil.RegexCountFile("coordinator dispatchrequest", "hera.log") < 1 {
		t.Fatalf("Error: should have dispatched the request to database")
	}

	sb.Stop()
	time.Sleep(5 * time.Second)
	sb, err = testutil.StartSpeedBumpProxy(9090, destAddr, 10, 20, 5)

	if err != nil {
		t.Fatalf("Failed to start speedbump with new config, err %v", err)
	}
	sb.Start()
	defer sb.Stop()

	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err = db.Conn(ctx)
	if err != nil {
		t.Fatalf("Error getting connection %s\n", err.Error())
	}

	stmt, _ = conn.PrepareContext(ctx, "/*cmd*/select id, int_val from "+tableName+" where id=?")
	rows, err = stmt.QueryContext(ctx, 1)

	if !rows.Next() {
		t.Fatalf("Expected 1 row")
	}
	rows.Close()
	stmt.Close()
	time.Sleep(5 * time.Second)

	if testutil.RegexCountFile("T.*SET\t658232417\t0", "cal.log") < 1 {
		t.Fatalf("Error: Second SET should have success")
	}

	if testutil.RegexCountFile("T.*CLIENT_SESSION.*", "cal.log") < 2 {
		t.Fatalf("Error: all the requests should have been sent to the database")
	}

	if testutil.RegexCountFile(".*EXEC\t658232417.*", "cal.log") < 2 {
		t.Fatalf("Error: query should be sent to the database")
	}

	if testutil.RegexCountFile("T.*GET\t658232417\t2.*error:.*response timeout", "cal.log") < 1 {
		t.Fatalf("Error: One Get should failed connection timeout")
	}

	if testutil.RegexCountFile("T.*GET\t658232417\t2.*err=error: no key", "cal.log") < 1 {
		t.Fatalf("Error: One Get should failed with no key error")
	}
	conn.Close()

	logger.GetLogger().Log(logger.Debug, "TestTTLCacheJunoTimeoutInitiallyDuringStartup done +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
}
