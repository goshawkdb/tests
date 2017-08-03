package harness

import (
	"encoding/binary"
	"fmt"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/client"
	"goshawkdb.io/common"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
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
	log.Logger
	Fatal(keyvals ...interface{})
}

type TestHelper struct {
	TestInterface
	connections   []*Connection
	ClusterHosts  []string
	ClusterCert   []byte
	ClientKeyPair []byte
	RootName      string
}

type TestTAdaptor struct {
	log.Logger
	*testing.T
}

func NewMainHelper() *TestHelper {
	return NewHelper(newDefaultTestInterface())
}

func NewTestHelper(t *testing.T) *TestHelper {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	return NewHelper(&TestTAdaptor{
		Logger: logger,
		T:      t,
	})
}

func NewHelper(t TestInterface) *TestHelper {
	runtime.GOMAXPROCS(runtime.NumCPU())

	env := GetTestEnv()
	clusterHosts := strings.Split(env.EnsureEnv(ClusterHosts, defaultClusterHosts), ",")

	var clusterCert []byte
	clusterCertPath := env[ClusterCert]
	if len(clusterCertPath) == 0 {
		clusterCert = []byte(defaultClusterCert)
	} else {
		if contents, err := ioutil.ReadFile(clusterCertPath); err == nil {
			clusterCert = contents
		} else {
			t.Fatal("msg", "Error when loading the cluster cert from env var.", "envVar", clusterCertPath, "error", err)
		}
	}

	var clientKeyPair []byte
	clientKeyPairPath := env[ClientKeyPair]
	if len(clientKeyPairPath) == 0 {
		clientKeyPair = []byte(defaultClientKeyPair)
	} else {
		if contents, err := ioutil.ReadFile(clientKeyPairPath); err == nil {
			clientKeyPair = contents
		} else {
			t.Fatal("msg", "Error when loading the client key pair from env var.", "envVar", clientKeyPairPath, "error", err)
		}
	}
	rootName := env.EnsureEnv(RootName, defaultRootName)

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
		th.Fatal("error", err)
	}
	return err
}

func (th *TestHelper) CreateConnections(num int) []*Connection {
	offset := len(th.connections)
	results := make([]*Connection, num)
	for i := 0; i < num; i++ {
		host := th.ClusterHosts[i%len(th.ClusterHosts)]
		logger := log.With(th.TestInterface, "conn", offset+i)
		conn, err := client.NewConnection(host, th.ClientKeyPair, th.ClusterCert, logger)
		th.MaybeFatal(err)
		results[i] = &Connection{
			TestHelper:              th,
			Logger:                  logger,
			Connection:              conn,
			connIdx:                 offset + i,
			submissionCount:         0,
			totalSubmissionDuration: time.Duration(0),
		}
	}
	th.connections = append(th.connections, results...)
	return results
}

func (th *TestHelper) Shutdown() {
	for _, c := range th.connections {
		c.Log("AverageSubmissionTime", c.AvgSubmissionTime(), "SubmissionsCount", c.submissionCount)
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
	log.Logger
	*client.Connection
	connIdx                 int
	submissionCount         int
	totalSubmissionDuration time.Duration
}

func (conn *Connection) RunTransaction(fun func(*client.Txn) (interface{}, error)) (interface{}, *common.TxnId, error) {
	result, stats, err := conn.Connection.RunTransaction(fun)
	conn.submissionCount += len(stats.Submissions)
	if conn.submissionCount > 0 && conn.submissionCount%64 == 0 {
		conn.Log("submissionCount", conn.submissionCount)
	}
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

func newDefaultTestInterface() *mainTest {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	return &mainTest{
		Logger: logger,
	}
}

type mainTest struct {
	log.Logger
}

func (mt *mainTest) Fatal(keyvals ...interface{}) {
	if len(keyvals) != 0 {
		mt.Log(keyvals...)
	}
	os.Exit(1)
}
