package strongserializable

import (
	"encoding/binary"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/common"
	"goshawkdb.io/tests"
	"sync"
	"time"
)

// Careful, this one is quite timing sensitive - you want the number
// of proposers/acceptors to stay very close to 0 (<10).
func StrongSerializable(th *tests.TestHelper) {
	par := 3
	iterations := 1000

	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	vsn, _ := conn.SetRootToNZeroObjs(par + par)
	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(par)
	endBarrier, errCh := th.InParallel(par, func(connIdx int, conn *tests.Connection) error {
		return runTest(connIdx, conn, vsn, iterations, startBarrier)
	})
	go func() {
		endBarrier.Wait()
		close(errCh)
	}()
	th.MaybeFatal(<-errCh)
}

func runTest(connNum int, conn *tests.Connection, vsn *common.TxnId, iterations int, startBarrier *sync.WaitGroup) error {
	err := conn.AwaitRootVersionChange(vsn)
	startBarrier.Done()
	if err != nil {
		return err
	}
	startBarrier.Wait()
	buf := make([]byte, 8)
	res, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		rootObj, err := conn.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		objRefs, err := rootObj.References()
		if err != nil {
			return nil, err
		}
		return []*common.VarUUId{objRefs[connNum+connNum].Id, objRefs[connNum+connNum+1].Id}, nil
	})
	if err != nil {
		return err
	}
	objIds, ok := res.([]*common.VarUUId)
	if !ok {
		return fmt.Errorf("Returned result is not a [] var uuid!")
	}
	for ; iterations > 0; iterations-- {
		time.Sleep(11 * time.Millisecond)
		n := uint64(iterations)
		binary.BigEndian.PutUint64(buf, n)
		_, _, err = conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			objA, err := txn.GetObject(objIds[0])
			if err != nil {
				return nil, err
			}
			objB, err := txn.GetObject(objIds[1])
			if err != nil {
				return nil, err
			}
			if err = objA.Set(buf); err != nil {
				return nil, err
			}
			if err = objB.Set(buf); err != nil {
				return nil, err
			}
			return nil, nil
		})
		if err != nil {
			return err
		}
		time.Sleep(7 * time.Millisecond)
		n++
		binary.BigEndian.PutUint64(buf, n)
		_, _, err = conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			objA, err := txn.GetObject(objIds[0])
			if err != nil {
				return nil, err
			}
			return nil, objA.Set(buf)
		})
		if err != nil {
			return err
		}
		n++
		binary.BigEndian.PutUint64(buf, n)
		_, _, err = conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			objA, err := txn.GetObject(objIds[0])
			if err != nil {
				return nil, err
			}
			return nil, objA.Set(buf)
		})
		if err != nil {
			return err
		}
		res, _, err = conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			objA, err := txn.GetObject(objIds[0])
			if err != nil {
				return nil, err
			}
			val, err := objA.Value()
			if err != nil {
				return nil, err
			}
			return binary.BigEndian.Uint64(val), nil
		})
		if err != nil {
			return err
		}
		if m, ok := res.(uint64); !ok || m != n {
			return fmt.Errorf("Expected %v got %v (%v)", n, m, ok)
		}
	}
	return nil
}
