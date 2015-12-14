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
	testUsername = "test"
	testPassword = "test"
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

func (th *TestHelper) CreateConnections(num int) []*Connection {
	th.Connections = th.createConnections(num, th.hosts)
	return th.Connections
}

func (th *TestHelper) createConnections(num int, hosts []string) []*Connection {
	results := make([]*Connection, num)
	for i := 0; i < num; i++ {
		host := hosts[i%len(hosts)]
		conn, err := client.NewConnection(host, testUsername, []byte(testPassword))
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

func (th *TestHelper) SetRootToZeroUInt64() *common.TxnId {
	buf := make([]byte, 8)
	txnId, _ := th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
		rootObj, err := txn.GetRootObject()
		if err != nil {
			return nil, err
		}
		binary.BigEndian.PutUint64(buf, 0)
		rootObj.Set(buf)
		return rootObj.Version()
	})
	return txnId.(*common.TxnId)
}

func (th *TestHelper) SetRootToNZeroObjs(n int) *common.TxnId {
	txnId, _ := th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
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
	return txnId.(*common.TxnId)
}

func (th *TestHelper) AwaitRootVersionChange(connNum int, vsn *common.TxnId) {
	th.RunTransaction(connNum, func(txn *client.Txn) (interface{}, error) {
		rootObj, err := txn.GetRootObject()
		if err != nil {
			return nil, err
		}
		if rootVsn, err := rootObj.Version(); err != nil {
			return nil, err
		} else if vsn.Equal(rootVsn) {
			return client.Retry, nil
		}
		return nil, nil
	})
}

func (th *TestHelper) RunTransaction(connNum int, fun func(*client.Txn) (interface{}, error)) (interface{}, *common.TxnId) {
	conn := th.Connections[connNum]
	result, txnId, err := conn.RunTransaction(fun)
	if err == Abort || err == client.Restart {
		return nil, nil
	} else if err != nil {
		th.Fatal(err)
		return nil, txnId
	}
	return result, txnId
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
