// Copyright 2019 PayPal Inc.
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
	"errors"
	"net"

	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"
)

type tcpListener struct {
	lsn net.Listener
}

// NewTCPListener creates a Listener attached to the address "service". It is a wrapper over net.Listener
func NewTCPListener(service string) *tcpListener {
	var err error
	lsn := &tcpListener{}
	lsn.lsn, err = net.Listen("tcp", service)
	if err != nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "Cannot create listener: ", err.Error())
		}

		// do a full shutdown and kill the parent watchdog
		FullShutdown()
	}

	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, "server: listening on", service, " for https, connects to worker through socket")
	}

	return lsn
}

// Accept is used to accept a connected. It simply calls net.Listenr.Accept()
func (lsn *tcpListener) Accept() (ClientConn, error) {
	clientConn := ClientConn{}
	var err error
	clientConn.tcpConn, err = lsn.lsn.Accept()
	return clientConn, err
}

// Close closes the connection
func (lsn *tcpListener) Close() error {
	return lsn.lsn.Close()
}

// Called after the connection is accepted and before it is handled. This function can be enhanced to
// handle some type of authentication for example
func (lsn *tcpListener) Init(conn ClientConn) error {
	if conn.tcpConn == nil {
		return errors.New("Nil connection")
	}

	e := cal.NewCalEvent("ACCEPT", IPAddrStr(conn.RemoteAddr()), cal.TransOK, "")
	e.AddDataStr("fwk", "muxtcp")
	e.AddDataStr("raddr", conn.RemoteAddr().String())
	e.AddDataStr("laddr", conn.LocalAddr().String())
	e.Completed()

	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "Authenticated OK")
	}
	return nil
}
