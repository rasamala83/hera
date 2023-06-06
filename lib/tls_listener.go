package lib

import (
	//"sync"
	"encoding/pem"
	"io/ioutil"
	"net"
	"os"

	//"os"
	//"time"
	//. "utility/logger"
	//cal "calclient"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"github.com/paypal/hera/cal"
	"github.com/paypal/hera/utility/logger"
	"github.com/quic-go/quic-go"
)

type tlsListener struct {
	tcpListener  net.Listener
	tlsListener  net.Listener
	quicListener *quic.Listener
	cfg          *tls.Config
}

// CheckErrAndShutdown if error then it logs it and starts the shutdown
func CheckErrAndShutdown(err error, msg string) bool {
	if err == nil {
		return false
	}
	if logger.GetLogger().V(logger.Alert) {
		logger.GetLogger().Log(logger.Alert, err.Error()+" CAUSED FAILURE IN "+msg)
	}
	FullShutdown()
	return true
}

// NewTLSListener creates the TLS listener
func NewTLSListener(service string) Listener {
	var err error
	lsn := &tlsListener{}

	pemData, err := ioutil.ReadFile(GetConfig().KeyFile)
	if CheckErrAndShutdown(err, "load key") {
		return nil
	}

	block, _ := pem.Decode(pemData)
	if block == nil {
		CheckErrAndShutdown(errors.New("pem.Decode"), "pem decode")
		return nil
	}

	TLSKeyPasswd := os.Getenv("TLS_KEY_PASSWD")
	if TLSKeyPasswd != "" {
		decBlock, err := x509.DecryptPEMBlock(block, []byte(TLSKeyPasswd))
		if CheckErrAndShutdown(err, "decrypt key") {
			return nil
		}
		block.Bytes = decBlock
	}

	certChain, err := ioutil.ReadFile(GetConfig().CertChainFile)
	if CheckErrAndShutdown(err, "load certChain") {
		return nil
	}
	cert, err := tls.X509KeyPair(certChain, pem.EncodeToMemory(block))
	if CheckErrAndShutdown(err, "load cert with key") {
		return nil
	}

	lsn.cfg = &tls.Config{Certificates: []tls.Certificate{cert}, DynamicRecordSizingDisabled: true}

	if GetConfig().UseQUIC {
		lsn.quicListener, err = quic.ListenAddr(service, lsn.cfg, nil)
	} else {
		lsn.tcpListener, err = net.Listen("tcp", service)
	}

	if err != nil {
		if logger.GetLogger().V(logger.Alert) {
			logger.GetLogger().Log(logger.Alert, "Cannot create listener: ", err.Error())
		}

		// do a full shutdown and kill the parent watchdog
		FullShutdown()
	}

	if !GetConfig().UseQUIC {
		lsn.tlsListener = tls.NewListener(lsn.tcpListener, lsn.cfg)
	}

	if logger.GetLogger().V(logger.Info) {
		logger.GetLogger().Log(logger.Info, "server: listening on", service, " for https, connects to worker through uds")
	}

	return lsn
}

func (lsn *tlsListener) Accept() (net.Conn, error) {
	return lsn.tlsListener.Accept()
}

func (lsn *tlsListener) Close() error {
	return lsn.tlsListener.Close()
}

func (lsn *tlsListener) Init(conn net.Conn) (net.Conn, error) {
	if conn == nil {
		return nil, errors.New("Nil connection")
	}

	tlsconn := conn.(*tls.Conn)
	if logger.GetLogger().V(logger.Verbose) {
		logger.GetLogger().Log(logger.Verbose, "Processing connection. Start handshake")
	}

	err := tlsconn.Handshake()

	if err != nil {
		if logger.GetLogger().V(logger.Info) {
			logger.GetLogger().Log(logger.Info, "Handshake error: ", err.Error())
		}
		tlsconn.Close()
		return nil, err
	}

	e := cal.NewCalEvent("ACCEPT", IPAddrStr(conn.RemoteAddr()), cal.TransOK, "")
	e.AddDataStr("fwk", "muxtls")
	e.AddDataStr("raddr", conn.RemoteAddr().String())
	e.AddDataStr("laddr", conn.LocalAddr().String())
	e.Completed()

	connState := tlsconn.ConnectionState()
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "Handshake OK. connState.SessionReused=", connState.DidResume)
	}
	return tlsconn, nil
}

func (lsn *tlsListener) QUICAccept() (quic.Connection, error) {
	return lsn.quicListener.Accept(nil)
}

func (lsn *tlsListener) QUICClose() error {
	return lsn.quicListener.Close()
}

func (lsn *tlsListener) InitQUICConn(quicConn quic.Connection) (quic.Connection, error) {
	if quicConn == nil {
		return nil, errors.New("nil QUIC connection")
	}

	e := cal.NewCalEvent("ACCEPT", IPAddrStr(quicConn.RemoteAddr()), cal.TransOK, "")
	e.AddDataStr("fwk", "muxtls")
	e.AddDataStr("raddr", quicConn.RemoteAddr().String())
	e.AddDataStr("laddr", quicConn.LocalAddr().String())
	e.Completed()

	connState := quicConn.ConnectionState()
	if logger.GetLogger().V(logger.Debug) {
		logger.GetLogger().Log(logger.Debug, "Handshake OK. connState.TLS version=", connState.TLS)
	}
	return quicConn, nil
}
