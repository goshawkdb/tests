package tests

import (
	"encoding/binary"
	"flag"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/common"
	"log"
	"runtime"
	"time"
)

const (
	clusterCertPEM = `-----BEGIN CERTIFICATE-----
MIIBxzCCAW2gAwIBAgIIQqu37k6KPOIwCgYIKoZIzj0EAwIwOjESMBAGA1UEChMJ
R29zaGF3a0RCMSQwIgYDVQQDExtDbHVzdGVyIENBIFJvb3QgQ2VydGlmaWNhdGUw
IBcNMTYwMTAzMDkwODE2WhgPMjIxNjAxMDMwOTA4MTZaMDoxEjAQBgNVBAoTCUdv
c2hhd2tEQjEkMCIGA1UEAxMbQ2x1c3RlciBDQSBSb290IENlcnRpZmljYXRlMFkw
EwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEjHBXt+0n477zVZHTsGgu9rLYzNz/WMLm
l7/KC5v2nx+RC9yfkyfBKq8jJk3KYoB/YJ7s8BH0T456/+nRQIUo7qNbMFkwDgYD
VR0PAQH/BAQDAgIEMA8GA1UdEwEB/wQFMAMBAf8wGQYDVR0OBBIEEL9sxrcr6QTw
wk5csm2ZcfgwGwYDVR0jBBQwEoAQv2zGtyvpBPDCTlyybZlx+DAKBggqhkjOPQQD
AgNIADBFAiAy9NW3zE1ACYDWcp+qeTjQOfEtED3c/LKIXhrbzg2N/QIhANLb4crz
9ENxIifhZcJ/S2lqf49xZZS91dLF4x5ApKci
-----END CERTIFICATE-----`
	clientCertAndKeyPEM = `-----BEGIN CERTIFICATE-----
MIIBszCCAVmgAwIBAgIIfOmxD9dF8ZMwCgYIKoZIzj0EAwIwOjESMBAGA1UEChMJ
R29zaGF3a0RCMSQwIgYDVQQDExtDbHVzdGVyIENBIFJvb3QgQ2VydGlmaWNhdGUw
IBcNMTYwMTAzMDkwODUwWhgPMjIxNjAxMDMwOTA4NTBaMBQxEjAQBgNVBAoTCUdv
c2hhd2tEQjBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABFrAPcdlw5DWQmS9mCFX
FlD6R8ABaBf4LA821wVmPa9tiM6n8vRJvbmHuSjy8LwJJRRjo9GJq7KD6ZmsK9P9
sXijbTBrMA4GA1UdDwEB/wQEAwIHgDATBgNVHSUEDDAKBggrBgEFBQcDAjAMBgNV
HRMBAf8EAjAAMBkGA1UdDgQSBBBX9qcbG4ofUoUTHGwOgGvFMBsGA1UdIwQUMBKA
EL9sxrcr6QTwwk5csm2ZcfgwCgYIKoZIzj0EAwIDSAAwRQIgOK9PVJt7KdvDU/9v
z9gQI8JnVLZm+6gsh6ro9WnaZ8YCIQDXhjfQAWaUmJNTgKq3rLHiEbPS4Mxl7h7S
kbkX/2GIjg==
-----END CERTIFICATE-----
-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIN9Mf6CzDgCs1EbzJqDK3+12wcr7Ua3Huz6qNhyXCrS1oAoGCCqGSM49
AwEHoUQDQgAEWsA9x2XDkNZCZL2YIVcWUPpHwAFoF/gsDzbXBWY9r22Izqfy9Em9
uYe5KPLwvAklFGOj0YmrsoPpmawr0/2xeA==
-----END EC PRIVATE KEY-----`
)

type TestInterface interface {
	Fatal(...interface{})
	Fatalf(string, ...interface{})
	Log(...interface{})
	Logf(string, ...interface{})
}

type TestHelper struct {
	TestInterface
	Connections []*Connection
	hosts       []string
}

type TestHelperTxnResult uint8

const Abort TestHelperTxnResult = iota

func (self TestHelperTxnResult) Error() string {
	return "Abort"
}

func NewTestHelper(t TestInterface, hosts ...string) *TestHelper {
	if t == nil {
		t = defaultTestInterface
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	if len(hosts) == 0 {
		flag.Parse()
		hosts = flag.Args()
	}
	if len(hosts) == 0 {
		t.Fatal("No hosts provided")
	}
	return &TestHelper{
		TestInterface: t,
		hosts:         hosts,
	}
}

func (th *TestHelper) MaybeFatal(err error) {
	if err != nil {
		th.Fatal(err)
	}
}

func (th *TestHelper) CreateConnections(num int) []*Connection {
	th.Connections = th.createConnections(num, th.hosts)
	return th.Connections
}

func (th *TestHelper) createConnections(num int, hosts []string) []*Connection {
	results := make([]*Connection, num)
	for i := 0; i < num; i++ {
		host := hosts[i%len(hosts)]
		conn, err := client.NewConnection(host, []byte(clientCertAndKeyPEM), []byte(clusterCertPEM))
		if err != nil {
			th.Fatal(err)
		}
		results[i] = &Connection{
			Connection:              conn,
			connIdx:                 i,
			submissionCount:         0,
			totalSubmissionDuration: time.Duration(0),
		}
	}
	return results
}

func (th *TestHelper) SetRootToZeroUInt64() (*common.TxnId, error) {
	buf := make([]byte, 8)
	txnId, _, err := th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
		rootObj, err := txn.GetRootObject()
		if err != nil {
			return nil, err
		}
		binary.BigEndian.PutUint64(buf, 0)
		rootObj.Set(buf)
		return rootObj.Version()
	})
	if err != nil {
		return nil, err
	}
	return txnId.(*common.TxnId), nil
}

func (th *TestHelper) SetRootToNZeroObjs(n int) (*common.TxnId, error) {
	txnId, _, err := th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
		rootObj, err := txn.GetRootObject()
		if err != nil {
			return nil, err
		}
		objs := make([]*client.Object, n)
		for idx := range objs {
			zeroBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(zeroBuf, 0)
			objs[idx], err = txn.CreateObject(zeroBuf)
			if err != nil {
				return nil, err
			}
		}
		if err := rootObj.Set([]byte{}, objs...); err != nil {
			return nil, err
		}
		return rootObj.Version()
	})
	if err != nil {
		return nil, err
	}
	return txnId.(*common.TxnId), nil
}

func (th *TestHelper) AwaitRootVersionChange(connNum int, vsn *common.TxnId) error {
	_, _, err := th.RunTransaction(connNum, func(txn *client.Txn) (interface{}, error) {
		rootObj, err := txn.GetRootObject()
		if err != nil {
			return nil, err
		}
		if rootVsn, err := rootObj.Version(); err != nil {
			return nil, err
		} else if vsn.Compare(rootVsn) == common.EQ {
			return client.Retry, nil
		}
		return nil, nil
	})
	return err
}

func (th *TestHelper) RunTransaction(connNum int, fun func(*client.Txn) (interface{}, error)) (interface{}, *common.TxnId, error) {
	conn := th.Connections[connNum]
	result, txnId, err := conn.RunTransaction(fun)
	if err == Abort || err == client.Restart {
		return nil, nil, nil
	} else if err != nil {
		return nil, txnId, err
	}
	return result, txnId, nil
}

func (th *TestHelper) Shutdown() {
	for _, c := range th.Connections {
		th.Logf("Average submission time: %v (%v submissions)", c.AvgSubmissionTime(), c.submissionCount)
		c.Shutdown()
	}
}

type Connection struct {
	*client.Connection
	connIdx                 int
	submissionCount         int
	totalSubmissionDuration time.Duration
}

func (conn *Connection) RunTransaction(fun func(*client.Txn) (interface{}, error)) (interface{}, *common.TxnId, error) {
	fmt.Printf("%x", conn.connIdx)
	result, stats, err := conn.Connection.RunTransaction(fun)
	conn.submissionCount += len(stats.Submissions)
	for _, s := range stats.Submissions {
		conn.totalSubmissionDuration += s
	}
	return result, stats.TxnId, err
}

func (conn *Connection) AvgSubmissionTime() time.Duration {
	if conn.submissionCount == 0 {
		return time.Duration(0)
	} else {
		return conn.totalSubmissionDuration / time.Duration(conn.submissionCount)
	}
}

var defaultTestInterface = &mainTest{}

type mainTest struct{}

func (mt *mainTest) Fatal(args ...interface{}) {
	log.Fatal(args...)
}

func (mt *mainTest) Fatalf(format string, args ...interface{}) {
	log.Fatalf(format, args...)
}

func (mt *mainTest) Log(args ...interface{}) {
	log.Print(args...)
}

func (mt *mainTest) Logf(format string, args ...interface{}) {
	log.Printf(format, args...)
}
