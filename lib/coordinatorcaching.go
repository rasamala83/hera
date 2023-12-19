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
	// "time"
	"context"
	"fmt"
	"strings"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility"
	"github.com/paypal/hera/utility/encoding/netstring"
	"github.com/paypal/hera/utility/logger"
)

func (crd *Coordinator) getKey(request *netstring.Netstring) ([]byte, string, error) {
	var key string
	if GetConfig().CacheByCorrId {
		// Return err if corrid is NotSet
		if crd.extractedcorrId == "NotSet" || crd.extractedcorrId == "" || crd.extractedcorrId == "unset" {
			return nil, "", ErrCacheCorridNotSet
		}
		key += crd.extractedcorrId + "|"
	}
	reqAfterCorrId := ""
	if request != nil {
		str := string(request.Payload)
		pos := strings.Index(str, ",")
		if pos != -1 {
			reqAfterCorrId = str[pos+1:]
		}
		logger.GetLogger().Log(logger.Verbose, "reqAfterCorrId:", reqAfterCorrId)
		if len(reqAfterCorrId) > 0 {
			key += reqAfterCorrId + "|"
		}
	}
	sqlhashStr := fmt.Sprintf("%d", uint32(crd.sqlhash))
	logger.GetLogger().Log(logger.Verbose, "SQLHash:", sqlhashStr)
	key += sqlhashStr + "|"
	binds := parseBinds(request)
	var concatKey string
	for bindName, bindValue := range binds {
		concatKey += fmt.Sprintf("%s^%s|", bindName, bindValue)
	}
	logger.GetLogger().Log(logger.Verbose, "Binds after parsing:", concatKey)
	key += concatKey
	poolName := cal.GetCalClientInstance().GetPoolName()
	key += poolName
	logger.GetLogger().Log(logger.Verbose, "key inside getKey:", key)
	keyHash := utility.GetFNV128a(key)
	return keyHash, key, nil
}

func (crd *Coordinator) setRecordToCache(request *netstring.Netstring, crdResponse string, ttl uint32) error {
	cli, _ := GetJunoClient()
	logger.GetLogger().Log(logger.Verbose, "SET junoClientReady:", cli.junoClientReady)
	if cli.junoClientReady && cli != nil {
		txn := cal.NewCalTransaction("API", "CACHE_SESSION", "0", "", cal.DefaultTGName)
		txn.AddDataStr("corrid", crd.extractedcorrId)
		keyHash, key, keyerr := crd.getKey(request)
		if keyerr != nil {
			evt := cal.NewCalEvent("setRecordToCache", "getKeyErr", cal.TransWarning, "")
			evt.AddDataStr("corrid", crd.extractedcorrId)
			evt.AddDataStr("err", keyerr.Error())
			evt.Completed()
			txn.SetStatus("3")
			txn.Completed()
			return keyerr
		}
		keyHashStr := fmt.Sprintf("%x", keyHash)
		logger.GetLogger().Log(logger.Verbose, "Trying SET with key:", keyHashStr, "value:", crdResponse)
		caltxn := cal.NewCalTransaction("SET", fmt.Sprintf("%d", uint32(crd.sqlhash)), "0", "", cal.DefaultTGName)
		caltxn.AddDataStr("corrid", crd.extractedcorrId)
		logger.GetLogger().Log(logger.Verbose, "junoKeyHash:", keyHashStr, "junoKey:", key)
		err := cli.Set([]byte(keyHashStr), []byte(crdResponse), ttl)
		caltxn.AddDataStr("junoKeyHash", keyHashStr)
		caltxn.AddDataInt("keySize:", int64(len([]byte(keyHashStr))))
		caltxn.AddDataInt("crdResponseSize", int64(len([]byte(crdResponse))))
		if err != nil {
			logger.GetLogger().Log(logger.Alert, "Error in setRecordToCache:", err)
			caltxn.SetStatus("2")
			caltxn.AddDataStr("err", err.Error())
			caltxn.Completed()
			txn.Completed()
			return err
		}
		caltxn.Completed()
		txn.Completed()
		return err
	} else {
		return fmt.Errorf("GetJunoClient returned nil...")
	}
}

func (crd *Coordinator) getRecordFromCache(request *netstring.Netstring, respExit <-chan error, shadowTest bool) error {
	cli, _ := GetJunoClient()
	logger.GetLogger().Log(logger.Verbose, "GET junoClientReady:", cli.junoClientReady)
	if cli.junoClientReady && cli != nil {
		txn := cal.NewCalTransaction("API", "CACHE_SESSION", "0", "", cal.DefaultTGName)
		txn.AddDataStr("corrid", crd.extractedcorrId)
		keyHash, key, keyerr := crd.getKey(request)
		if keyerr != nil {
			evt := cal.NewCalEvent("getRecordFromCache", "getKeyErr", cal.TransWarning, "")
			evt.AddDataStr("corrid", crd.extractedcorrId)
			evt.AddDataStr("err", keyerr.Error())
			evt.Completed()
			txn.Completed()
			return keyerr
		}
		caltxn := cal.NewCalTransaction("GET", fmt.Sprintf("%d", uint32(crd.sqlhash)), "0", "", cal.DefaultTGName)
		caltxn.AddDataStr("corrid", crd.extractedcorrId)
		keyHashStr := fmt.Sprintf("%x", keyHash)
		logger.GetLogger().Log(logger.Verbose, "Trying GET with key:", keyHashStr)
		logger.GetLogger().Log(logger.Verbose, "junoKeyHash:", keyHashStr, "junoKey:", key)
		resp, err := cli.Get([]byte(keyHashStr))
		caltxn.AddDataStr("junoKeyHash:", keyHashStr)
		caltxn.AddDataInt("keySize:", int64(len([]byte(keyHashStr))))
		if err != nil {
			logger.GetLogger().Log(logger.Alert, "Error in getRecordFromCache:", err)
			caltxn.SetStatus("2")
			caltxn.AddDataStr("err", err.Error())
			caltxn.Completed()
			txn.Completed()
			return err
		}
		logger.GetLogger().Log(logger.Verbose, "Response from cache...", string(resp))
		caltxn.AddDataInt("cacheResponseSize", int64(len(resp)))
		caltxn.Completed()
		select {
		case err := <-respExit:
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, crd.id, "getRecordFromCache: received err:", err)
			}
			txn.SetStatus("2")
			txn.AddDataStr("err", err.Error())
			txn.Completed()
			return err
		default:
			if len(string(resp)) > 0 {
				if shadowTest {
					logger.GetLogger().Log(logger.Debug, crd.id, "Not responding to the client from cache...ShadowTestEnabled:", shadowTest)
					evt := cal.NewCalEvent("getRecordFromCache", "shadowTest", cal.TransOK, "")
					evt.AddDataStr("resp", ErrCacheShadowTest.Error())
					evt.Completed()
					txn.Completed()
					return ErrCacheShadowTest
				}
				splits := strings.Split(string(resp), "|")
				for idx, split := range splits {
					if len(split) > 0 {
						logger.GetLogger().Log(logger.Debug, crd.id, "Responding to client...")
						err := crd.respond([]byte(split))
						// _, err = crd.conn.Write([]byte(split))
						txn.AddDataInt(fmt.Sprintf("%d", idx), int64(len([]byte(split))))
						if err != nil {
							logger.GetLogger().Log(logger.Debug, crd.id, "Failed to reply to client")
							txn.SetStatus("2")
							txn.AddDataStr("err", err.Error())
							txn.Completed()
							return ErrCacheClientWriteFailed
						}
					}
				}
			}
			txn.Completed()
			return err
		}
	} else {
		return fmt.Errorf("GetJunoClient returned nil...")
	}
}

func (crd *Coordinator) doCacheRequest(ctx context.Context, request *netstring.Netstring, enableShadowTest bool) error {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, crd.id, "coordinator doCacheRequest: starting")
	}

	defer func() {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, crd.id, "coordinator doCacheRequest: exiting")
		}
	}()

	errors := make(chan error, 1)
	quit := make(chan bool, 1)
	safeExit := make(chan bool, 1)
	respExit := make(chan error, 1)
	clientChannel := crd.clientchannel
	done := ctx.Done()

	go func() {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, crd.id, "doCacheRequest: routine starting")
		}
		defer func() {
			if logger.GetLogger().V(logger.Verbose) {
				logger.GetLogger().Log(logger.Verbose, crd.id, "doCacheRequest: routine exiting")
			}
		}()
		for {
			select {
			case ns, ok := <-clientChannel:
				if !ok {
					if logger.GetLogger().V(logger.Verbose) {
						logger.GetLogger().Log(logger.Verbose, crd.id, "doCacheRequest: client channel closed")
					}
					evt := cal.NewCalEvent("doCacheRequest", "client_closed", cal.TransOK, "")
					evt.Completed()
					quit <- true
					respExit <- ErrCacheClientClosed
					errors <- ErrCacheClientClosed
					return
				} else {
					if ns != nil {
						// abort the client conn?
						// This is a protocol/internal error. Cannot support multiple netstrings
						if logger.GetLogger().V(logger.Warning) {
							logger.GetLogger().Log(logger.Warning, crd.id, "doCacheRequest: multiple client req", DebugString(ns.Serialized))
						}
						evt := cal.NewCalEvent("doCacheRequest", "multiple_client_req", cal.TransOK, fmt.Sprintf("cmd=%s", DebugString(ns.Serialized)))
						evt.Completed()
					}
					quit <- true
					respExit <- ErrCacheMultipleClientReq
					errors <- ErrCacheMultipleClientReq
					return
				}
			case <-done:
				if logger.GetLogger().V(logger.Verbose) {
					logger.GetLogger().Log(logger.Verbose, crd.id, "doCacheRequest: request canceled")
				}
				evt := cal.NewCalEvent("doCacheRequest", "client_req_canceled", cal.TransOK, "")
				evt.Completed()
				quit <- true
				respExit <- ErrCacheClientReqCanceled
				errors <- ErrCacheClientReqCanceled
				return
			case <-safeExit:
				if logger.GetLogger().V(logger.Verbose) {
					logger.GetLogger().Log(logger.Verbose, crd.id, "doCacheRequest: request completed")
				}
				evt := cal.NewCalEvent("doCacheRequest", "client_req_completed", cal.TransOK, "")
				evt.Completed()
				return
			}
		}
	}()

	err := crd.getRecordFromCache(request, respExit, enableShadowTest)

	select {
	case <-quit:
		return <-errors
	default:
		safeExit <- true
		return err
	}

}

func (crd *Coordinator) DispatchCachingSession(request *netstring.Netstring, reqType string) error {
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, crd.id, "coordinator DispatchCachingSession for", reqType, ": starting")
	}

	defer func() {
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, crd.id, "coordinator DispatchCachingSession for", reqType, ": exiting")
		}
	}()

	// var key string
	logger.GetLogger().Log(logger.Verbose, "Incoming request.Serialized:", string(request.Serialized))
	cacheCfg := getCacheCfg()
	cacheCfg.lock.Lock()
	defer cacheCfg.lock.Unlock()
	rec, ok := cacheCfg.cacheCfgRecords[uint32(crd.sqlhash)]
	logger.GetLogger().Log(logger.Verbose, uint32(crd.sqlhash), "CachingEnabled for ", reqType, ":", ok)
	if ok {
		logger.GetLogger().Log(logger.Verbose, "cacheRecord:", "sqlHash", rec.sqlHash, "sqlText", rec.sqlText, "ttl", rec.ttl, "cache enabled", rec.cachingEnabled)
		if rec.cachingEnabled == "Y" {
			if reqType == "GET" {
				if rec.enableShadowTest == "Y" {
					err := crd.doCacheRequest(crd.ctx, request, true)
					return err
				} else {
					err := crd.doCacheRequest(crd.ctx, request, false)
					return err
				}
			} else if reqType == "SET" {
				err := crd.setRecordToCache(request, crd.response, rec.ttl)
				return err
			} else {
				err := fmt.Errorf("Unsupported reqType...It must be GET/SET")
				return err
			}
		} else {
			logger.GetLogger().Log(logger.Verbose, "sqlHash is disabled for caching:", rec.sqlHash, rec.cachingEnabled)
			return ErrCacheDisabled
		}
	}
	return ErrCacheNotEnabled
}
