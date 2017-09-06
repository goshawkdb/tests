package harness

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/client"
	"math/rand"
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
	Rng           *rand.Rand
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

	clusterCertFound, clusterCertBites, err := ClusterCert.LoadFromEnv(env)
	if !clusterCertFound {
		clusterCertBites = []byte(defaultClusterCert)
	} else if err != nil {
		t.Fatal("msg", "Error when loading the cluster cert from env var.", "envVar", env[ClusterCert], "error", err)
	}

	clientKeyPairFound, clientKeyPairBites, err := ClientKeyPair.LoadFromEnv(env)
	if !clientKeyPairFound {
		clientKeyPairBites = []byte(defaultClientKeyPair)
	} else if err != nil {
		t.Fatal("msg", "Error when loading the client key pair from env var.", "envVar", env[ClientKeyPair], "error", err)
	}
	rootName := env.EnsureEnv(RootName, defaultRootName)

	return &TestHelper{
		TestInterface: t,
		ClusterHosts:  clusterHosts,
		ClusterCert:   clusterCertBites,
		ClientKeyPair: clientKeyPairBites,
		RootName:      rootName,
		Rng:           rand.New(rand.NewSource(time.Now().UnixNano())),
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
			TestHelper: th,
			Logger:     logger,
			Connection: conn,
			connIdx:    offset + i,
		}
	}
	th.connections = append(th.connections, results...)
	return results
}

func (th *TestHelper) Shutdown() {
	for _, c := range th.connections {
		c.Connection.ShutdownSync()
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

type Connection struct {
	*TestHelper
	log.Logger
	*client.Connection
	connIdx int
}

func (conn *Connection) SetRootToNZeroObjs(n int) ([]byte, error) {
	guidBuf := make([]byte, 8)
	conn.Rng.Read(guidBuf)
	zeroBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(zeroBuf, 0)
	_, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
		rootPtr := txn.Root(conn.RootName)
		if rootPtr == nil {
			return nil, fmt.Errorf("No root object named '%s' found", conn.RootName)
		} else {
			ptrs := make([]client.RefCap, n)
			for idx := range ptrs {
				objPtr, err := txn.Create(zeroBuf)
				if err != nil {
					return nil, err
				}
				ptrs[idx] = objPtr
			}
			return nil, txn.Write(*rootPtr, guidBuf, ptrs...)
		}
	})
	return guidBuf, conn.MaybeFatal(err)
}

func (conn *Connection) AwaitRootVersionChange(guidBuf []byte, expectedPtrs int) ([]client.RefCap, error) {
	var refCaps []client.RefCap
	_, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
		rootPtr := txn.Root(conn.RootName)
		if rootPtr == nil {
			return nil, fmt.Errorf("No root object named '%s' found", conn.RootName)
		} else if val, refs, err := txn.Read(*rootPtr); err != nil || txn.RestartNeeded() {
			return nil, err
		} else if !bytes.Equal(val, guidBuf) {
			return nil, txn.Retry()
		} else if len(refs) != expectedPtrs {
			return nil, txn.Retry()
		} else {
			refCaps = refs
			return nil, nil
		}
	})
	return refCaps, conn.MaybeFatal(err)
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
