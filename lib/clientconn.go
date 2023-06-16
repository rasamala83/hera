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
	"crypto/tls"
	"errors"
	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"
	"github.com/quic-go/quic-go"
	"net"
)

//ClientConn Represents either TCP based or QUIC based connection
type ClientConn struct {
	quicConn quic.Connection
	tcpConn  net.Conn
}

// Init It initializes client connection based on user configuration
func (clientConn *ClientConn) Init() error {
	if GetConfig().UseQUIC {
		if clientConn.quicConn == nil {
			return errors.New("QUIC Nil connection")
		}
		e := cal.NewCalEvent("ACCEPT", IPAddrStr(clientConn.RemoteAddr()), cal.TransOK, "")
		e.AddDataStr("fwk", "muxtls_quic")
		e.AddDataStr("raddr", clientConn.RemoteAddr().String())
		e.AddDataStr("laddr", clientConn.LocalAddr().String())
		e.Completed()

		connState := clientConn.quicConn.ConnectionState()
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, "QUIC Handshake OK. QUIC version=", connState.Version)
		}
		return nil
	} else {
		if clientConn.tcpConn == nil {
			return errors.New("Nil connection")
		}
		tlsconn := clientConn.tcpConn.(*tls.Conn)
		if logger.GetLogger().V(logger.Verbose) {
			logger.GetLogger().Log(logger.Verbose, "Processing connection. Start handshake")
		}

		err := tlsconn.Handshake()

		if err != nil {
			if logger.GetLogger().V(logger.Info) {
				logger.GetLogger().Log(logger.Info, "Handshake error: ", err.Error())
			}
			tlsconn.Close()
			return err
		}
		e := cal.NewCalEvent("ACCEPT", IPAddrStr(clientConn.RemoteAddr()), cal.TransOK, "")
		e.AddDataStr("fwk", "muxtls")
		e.AddDataStr("raddr", clientConn.RemoteAddr().String())
		e.AddDataStr("laddr", clientConn.LocalAddr().String())
		e.Completed()

		connState := tlsconn.ConnectionState()
		if logger.GetLogger().V(logger.Debug) {
			logger.GetLogger().Log(logger.Debug, "Handshake OK. connState.SessionReused=", connState.DidResume)
		}
		return nil
	}
}

//Implement few common operation which performed on connection to abstart internal connection details
//Based on isQUICConn flag it uses respective connection from client.

// RemoteAddr return remote address for respective type connection
func (clientConn *ClientConn) RemoteAddr() net.Addr {
	if GetConfig().UseQUIC {
		return clientConn.quicConn.RemoteAddr()
	}
	return clientConn.tcpConn.RemoteAddr()
}

// LocalAddr return remote address for respective type connection
func (clientConn *ClientConn) LocalAddr() net.Addr {
	if GetConfig().UseQUIC {
		return clientConn.quicConn.LocalAddr()
	}
	return clientConn.tcpConn.LocalAddr()
}

//Close Release connection from server
func (clientConn *ClientConn) Close() error {
	if GetConfig().UseQUIC {
		return clientConn.quicConn.CloseWithError(quic.ApplicationErrorCode(quic.NoError), "Sever closed the connection")
	}
	return clientConn.tcpConn.Close()
}

//Read data from connection and write to clientchannel
func (clientConn *ClientConn) Read(data []byte) (n int, err error) {
	if GetConfig().UseQUIC {
		dataStream, _ := clientConn.quicConn.OpenStream()
		defer dataStream.Close()
		return dataStream.Read(data)
	}
	return clientConn.tcpConn.Read(data)
}

//Write data bytes to connection from server
func (clientConn *ClientConn) Write(data []byte) (int, error) {
	if GetConfig().UseQUIC {
		dataStream, _ := clientConn.quicConn.OpenStream()
		defer dataStream.Close()
		return dataStream.Write(data)
	}
	return clientConn.tcpConn.Write(data)
}

func (clientConn *ClientConn) bounce() {
	e := cal.NewCalEvent(cal.EventTypeWarning, "Bounce", cal.TxnStatus(cal.TransWarning, "SERVER", "BOUNCE", "-1"), "")
	if GetConfig().UseQUIC {
		if clientConn.quicConn != nil {
			e.AddDataStr("raddr", clientConn.RemoteAddr().String())
		}
	} else {
		if clientConn.tcpConn != nil {
			e.AddDataStr("raddr", clientConn.RemoteAddr().String())
		}
	}
	clientConn.Close()
	e.Completed()
}
