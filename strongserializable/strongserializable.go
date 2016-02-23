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

	th.CreateConnections(par)
	defer th.Shutdown()

	vsn, err := th.SetRootToNZeroObjs(par + par)
	th.MaybeFatal(err)
	startBarrier, endBarrier := new(sync.WaitGroup), new(sync.WaitGroup)
	startBarrier.Add(par)
	endBarrier.Add(par)
	errCh := make(chan error, par)
	for idx := 0; idx < par; idx++ {
		connNum := idx
		go runTest(connNum, th, vsn, iterations, startBarrier, endBarrier, errCh)
	}
	go func() {
		endBarrier.Wait()
		close(errCh)
	}()
	th.MaybeFatal(<-errCh)
}

func runTest(connNum int, th *tests.TestHelper, vsn *common.TxnId, iterations int, startBarrier, endBarrier *sync.WaitGroup, errCh chan error) {
	defer endBarrier.Done()
	err := th.AwaitRootVersionChange(connNum, vsn)
	startBarrier.Done()
	if err != nil {
		errCh <- err
		return
	}
	startBarrier.Wait()
	buf := make([]byte, 8)
	res, _, err := th.RunTransaction(connNum, func(txn *client.Txn) (interface{}, error) {
		rootObj, err := txn.GetRootObject()
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
		errCh <- err
		return
	}
	objIds, ok := res.([]*common.VarUUId)
	if !ok {
		errCh <- fmt.Errorf("Returned result is not a [] var uuid!")
		return
	}
	for ; iterations > 0; iterations-- {
		time.Sleep(11 * time.Millisecond)
		n := uint64(iterations)
		binary.BigEndian.PutUint64(buf, n)
		_, _, err = th.RunTransaction(connNum, func(txn *client.Txn) (interface{}, error) {
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
			errCh <- err
			return
		}
		time.Sleep(7 * time.Millisecond)
		n++
		binary.BigEndian.PutUint64(buf, n)
		_, _, err = th.RunTransaction(connNum, func(txn *client.Txn) (interface{}, error) {
			objA, err := txn.GetObject(objIds[0])
			if err != nil {
				return nil, err
			}
			return nil, objA.Set(buf)
		})
		if err != nil {
			errCh <- err
			return
		}
		n++
		binary.BigEndian.PutUint64(buf, n)
		_, _, err = th.RunTransaction(connNum, func(txn *client.Txn) (interface{}, error) {
			objA, err := txn.GetObject(objIds[0])
			if err != nil {
				return nil, err
			}
			return nil, objA.Set(buf)
		})
		if err != nil {
			errCh <- err
			return
		}
		res, _, err = th.RunTransaction(connNum, func(txn *client.Txn) (interface{}, error) {
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
			errCh <- err
			return
		}
		if m, ok := res.(uint64); !ok || m != n {
			errCh <- fmt.Errorf("Expected %v got %v (%v)", n, m, ok)
			return
		}
	}
}
