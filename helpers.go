package tests

import (
	"encoding/binary"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/common"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	defaultClusterCert = `-----BEGIN CERTIFICATE-----
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
	defaultClientKeyPair = `-----BEGIN CERTIFICATE-----
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
	defaultClusterHosts = "localhost"
	defaultRootName     = "test"
)

type TestInterface interface {
	Fatal(...interface{})
	Fatalf(string, ...interface{})
	Log(...interface{})
	Logf(string, ...interface{})
}

type TestHelper struct {
	TestInterface
	connections   []*Connection
	ClusterHosts  []string
	ClusterCert   []byte
	ClientKeyPair []byte
	RootName      string
}

type TestHelperTxnResult uint8

const Abort TestHelperTxnResult = iota

func (self TestHelperTxnResult) Error() string {
	return "Abort"
}

func NewTestHelper(t TestInterface) *TestHelper {
	if t == nil {
		t = defaultTestInterface
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	clusterHostsStr := os.Getenv("GOSHAWKDB_CLUSTER_HOSTS")
	if len(clusterHostsStr) == 0 {
		clusterHostsStr = defaultClusterHosts
	}
	clusterHosts := strings.Split(clusterHostsStr, ",")
	clusterCertPath := os.Getenv("GOSHAWKDB_CLUSTER_CERT")
	var clusterCert []byte
	if len(clusterCertPath) == 0 {
		clusterCert = []byte(defaultClusterCert)
	} else {
		if contents, err := ioutil.ReadFile(clusterCertPath); err != nil {
			t.Fatal(fmt.Sprintf("Error when loading the cluster cert from env var ('%s'): %v", clusterCertPath, err))
		} else {
			clusterCert = contents
		}
	}
	var clientKeyPair []byte
	clientKeyPairPath := os.Getenv("GOSHAWKDB_CLIENT_KEYPAIR")
	if len(clientKeyPairPath) == 0 {
		clientKeyPair = []byte(defaultClientKeyPair)
	} else {
		if contents, err := ioutil.ReadFile(clientKeyPairPath); err != nil {
			t.Fatal(fmt.Sprintf("Error when loading the client key pair from env var ('%s'): %v", clientKeyPairPath, err))
		} else {
			clientKeyPair = contents
		}
	}
	rootName := os.Getenv("GOSHAWKDB_ROOT_NAME")
	if rootName == "" {
		rootName = defaultRootName
	}
	return &TestHelper{
		TestInterface: t,
		ClusterHosts:  clusterHosts,
		ClusterCert:   clusterCert,
		ClientKeyPair: clientKeyPair,
		RootName:      rootName,
	}
}

func (th *TestHelper) MaybeFatal(err error) error {
	if err != nil {
		th.Fatal(err)
	}
	return err
}

func (th *TestHelper) CreateConnections(num int) []*Connection {
	results := make([]*Connection, num)
	for i := 0; i < num; i++ {
		host := th.ClusterHosts[i%len(th.ClusterHosts)]
		conn, err := client.NewConnection(host, th.ClientKeyPair, th.ClusterCert)
		if err != nil {
			th.Fatal(err)
		}
		results[i] = &Connection{
			TestHelper:              th,
			Connection:              conn,
			connIdx:                 i,
			submissionCount:         0,
			totalSubmissionDuration: time.Duration(0),
		}
	}
	if len(th.connections) == 0 {
		th.connections = results
	} else {
		th.connections = append(th.connections, results...)
	}
	return results
}

func (th *TestHelper) Shutdown() {
	for _, c := range th.connections {
		th.Logf("Average submission time: %v (%v submissions)", c.AvgSubmissionTime(), c.submissionCount)
		c.Connection.Shutdown()
	}
}

func (th *TestHelper) InParallel(n int, fun func(int, *Connection) error) (*sync.WaitGroup, chan error) {
	conns := th.CreateConnections(n)
	endBarrier := new(sync.WaitGroup)
	endBarrier.Add(n)
	errCh := make(chan error, n)
	for idx, conn := range conns {
		idxCopy, connCopy := idx, conn
		go func() {
			defer endBarrier.Done()
			if err := fun(idxCopy, connCopy); err != nil {
				errCh <- err
			}
		}()
	}
	return endBarrier, errCh
}

func (th *TestHelper) GetRootObject(txn *client.Txn) (client.ObjectRef, error) {
	rootObjs, err := txn.GetRootObjects()
	if err != nil {
		return client.ObjectRef{}, err
	}
	obj, found := rootObjs[th.RootName]
	if !found {
		return client.ObjectRef{}, fmt.Errorf("No root object named '%s' found", th.RootName)
	}
	return obj, nil
}

type Connection struct {
	*TestHelper
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

func (conn *Connection) SetRootToZeroUInt64() (*common.TxnId, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, 0)
	txnId, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		rootObjs, err := txn.GetRootObjects()
		if err != nil {
			return nil, err
		}
		rootObj, found := rootObjs[conn.RootName]
		if !found {
			return nil, fmt.Errorf("No root object named '%s' found", conn.RootName)
		}
		rootObj.Set(buf)
		return rootObj.Version()
	})
	return txnId.(*common.TxnId), conn.MaybeFatal(err)
}

func (conn *Connection) SetRootToNZeroObjs(n int) (*common.TxnId, error) {
	zeroBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(zeroBuf, 0)
	txnId, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		rootObjs, err := txn.GetRootObjects()
		if err != nil {
			return nil, err
		}
		objs := make([]client.ObjectRef, n)
		for idx := range objs {
			obj, err := txn.CreateObject(zeroBuf)
			if err != nil {
				return nil, err
			}
			objs[idx] = obj
		}
		rootObj, found := rootObjs[conn.RootName]
		if !found {
			return nil, fmt.Errorf("No root object named '%s' found", conn.RootName)
		}
		if err := rootObj.Set([]byte{}, objs...); err != nil {
			return nil, err
		}
		return rootObj.Version()
	})
	return txnId.(*common.TxnId), conn.MaybeFatal(err)
}

func (conn *Connection) AwaitRootVersionChange(vsn *common.TxnId) error {
	_, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		rootObjs, err := txn.GetRootObjects()
		if err != nil {
			return nil, err
		}
		rootObj, found := rootObjs[conn.RootName]
		if !found {
			return nil, fmt.Errorf("No root object named '%s' found", conn.RootName)
		}
		if rootVsn, err := rootObj.Version(); err != nil {
			return nil, err
		} else if vsn.Compare(rootVsn) == common.EQ {
			return client.Retry, nil
		}
		return nil, nil
	})
	return conn.MaybeFatal(err)
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
