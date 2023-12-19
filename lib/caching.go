// Copyright 2023 PayPal Inc.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lib

import (
	"context"
	"database/sql"
	"fmt"
	"errors"
	"time"
	"sync/atomic"
	"github.com/paypal/hera/utility/logger"
	"sync"
)
// Cache config record to store the hera_sql_caching entries
type CacheRecord struct {
	query_id string
	sqlHash uint32
	sqlText string
	binds string
	ttl uint32
	enableShadowTest string
	tableName string
	invalidationClause string
	cachingEnabled string
	remarks string
	module string
}

type CacheCfg struct {
	cacheCfgRecords map[uint32]*CacheRecord
	lock *sync.Mutex
}

var moduleName string
var gCacheCfg atomic.Value

func getCacheCfgSQL() string {
	return fmt.Sprintf("SELECT query_id, sqlhash, sqltext, bind_variables, TTL_sec, enable_shadow_test, tableName, invalidation_clause, caching_enabled, remarks, %s_module FROM %s_sql_caching WHERE %s_module ='%s'", GetConfig().StateLogPrefix, "hera", GetConfig().StateLogPrefix, moduleName)
}

func getCacheCfg() *CacheCfg {
	cfg := gCacheCfg.Load()
	if cfg == nil {
		out := &CacheCfg{cacheCfgRecords:make(map[uint32]*CacheRecord), lock: &sync.Mutex{}}
		gCacheCfg.Store(out)
		return out
	}
	return cfg.(*CacheCfg)
}

func loadCacheCfg(ctx context.Context, db *sql.DB) error {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "Begin loading CacheCfg")
	}
	if logger.GetLogger().V(logger.Verbose) {
		defer func() {
			logger.GetLogger().Log(logger.Verbose, "Done loading cache config")
		}()
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Error (conn) loading cache config", err)
		return fmt.Errorf("Error (conn) loading cache config: %s", err.Error())
	}
	defer conn.Close()
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "CacheCfgSQL:", getCacheCfgSQL())
	}
	stmt, err := conn.PrepareContext(ctx, getCacheCfgSQL())
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Error (stmt) loading cache config", err)
		return fmt.Errorf("Error (stmt) loading cache config: %s", err.Error())
	}
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		logger.GetLogger().Log(logger.Alert, "Error (query) loading cache config", err)
		return fmt.Errorf("Error (query) loading cache config: %s", err.Error())
	}
	defer rows.Close()

	cfgLoad := &CacheCfg{cacheCfgRecords:make(map[uint32]*CacheRecord), lock: &sync.Mutex{}}
	logger.GetLogger().Log(logger.Verbose, "No errors. Begin loading rows...")
	rowCount := 0
	for rows.Next() {
		var rec CacheRecord
		var bindVariables sql.NullString
		var invalidationClause sql.NullString
		err = rows.Scan(&(rec.query_id), &(rec.sqlHash), &(rec.sqlText), &bindVariables, &(rec.ttl), &(rec.enableShadowTest), &(rec.tableName), &invalidationClause, &(rec.cachingEnabled), &(rec.remarks), &(rec.module))
		if err != nil {
			logger.GetLogger().Log(logger.Alert, "Error (rows scan) loading cache config", err)
			return fmt.Errorf("Error (rows scan) loading cache config: %s", err.Error())
		}
		
		if bindVariables.Valid {
			rec.binds = bindVariables.String
		}

		if invalidationClause.Valid {
			rec.invalidationClause = invalidationClause.String
		}

		rowCount++
		// To-Do: Any pre-validation checks if required
		cfgLoad.cacheCfgRecords[rec.sqlHash] = &rec
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, fmt.Sprintf("cacheCfgRecords entry: queryId:%s, sqlHash:%d, sqlText:%s, Binds:%s, TTL: %d, enableShadowTest:%s, tableName:%s, invClause:%s, cachingEnabled:%s, remarks:%s, module:%s", rec.query_id, rec.sqlHash, rec.sqlText, rec.binds, rec.ttl, rec.enableShadowTest, rec.tableName, rec.invalidationClause, rec.cachingEnabled, rec.remarks, rec.module))
		}
	}
	logger.GetLogger().Log(logger.Warning, fmt.Sprintf("Loaded %d sqlhashes, %d cacheCfg entries", len(cfgLoad.cacheCfgRecords), rowCount))
	
	gCacheCfg.Store(cfgLoad)
	return err
}

func InitCachingCfg(modName string) error {
	logger.GetLogger().Log(logger.Verbose, "InitCachingCfg for module:", modName)
	moduleName = modName
	ctx := context.Background()
	var db *sql.DB
	var err error
	i := 0
	for ; i < 3; i++ {
		time.Sleep(time.Millisecond * 2000)
		if db != nil {
			db.Close()
		}
		db, err = sql.Open("heraloop", fmt.Sprintf("0:0:0"))
		if err != nil {
			logger.GetLogger().Log(logger.Alert, "Error (db) InitCaching - conn error ", err)
			return err
		}
		db.SetMaxIdleConns(0)
		err = loadCacheCfg(ctx, db)
		if err == nil {
			break
		}
	}
	if i == 3 {
		logger.GetLogger().Log(logger.Verbose, "Reached max retries (3)")
		return errors.New("Failed to load caching config, no more retry..." + err.Error())
	}
	go func() {
		for {
			logger.GetLogger().Log(logger.Verbose, "Inside Routine to periodically load CacheConfig")
			// temp := getCacheCfg()
			// logger.GetLogger().Log(logger.Info, fmt.Sprintf("cacheCfgRecord size inside routine: %d", len(temp.cacheCfgRecords)))
			// for k, v := range temp.cacheCfgRecords {
			// 	logger.GetLogger().Log(logger.Info, fmt.Sprintf("Key SQLHash:%d", k))
			// 	logger.GetLogger().Log(logger.Info, fmt.Sprintf("Value:%s", v.sqlText))
			// }
			time.Sleep(time.Second * time.Duration(GetConfig().CachingCfgReloadInterval))
			if db != nil {
				db.Close()
			}
			db, err = sql.Open("heraloop", fmt.Sprintf("0:0:0"))
			db.SetMaxIdleConns(0)
			if err == nil {
				err = loadCacheCfg(ctx, db)
				if err != nil {
					logger.GetLogger().Log(logger.Alert, "Error loading CacheCfg ", err)
				}
			} else {
				logger.GetLogger().Log(logger.Alert, "Error (db) InitCaching - conn error ", err)
			}
		}
	}()
	
	return nil

}