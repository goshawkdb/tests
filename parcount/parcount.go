package parcount

import (
	"encoding/binary"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/common"
	"goshawkdb.io/tests"
	"sync"
)

func ParCount(th *tests.TestHelper) {
	parCount := 16
	limit := uint64(1000)
	conn := th.CreateConnections(1)[0]

	defer th.Shutdown()
	vsn, _ := conn.SetRootToNZeroObjs(parCount)
	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(parCount)
	endBarrier, errCh := th.InParallel(parCount, func(idx int, conn *tests.Connection) error {
		return runCount(idx, conn, vsn, limit, startBarrier)
	})
	go func() {
		endBarrier.Wait()
		close(errCh)
	}()
	th.MaybeFatal(<-errCh)
}

func runCount(connIdx int, conn *tests.Connection, rootVsn *common.TxnId, limit uint64, startBarrier *sync.WaitGroup) error {
	err := conn.AwaitRootVersionChange(rootVsn)
	startBarrier.Done()
	if err != nil {
		return err
	}
	startBarrier.Wait()
	var myObjRef client.ObjectRef
	_, _, err = conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		rootObj, err := conn.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		refs, err := rootObj.References()
		if err != nil {
			return nil, err
		}
		myObjRef = refs[connIdx]
		return nil, nil
	})
	if err != nil {
		return err
	}
	encountered := make(map[uint64]bool)
	expected := uint64(0)
	for {
		res, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			obj, err := txn.GetObject(myObjRef)
			if err != nil {
				return nil, err
			}
			val, err := obj.Value()
			if err != nil {
				return nil, err
			}
			cur := binary.BigEndian.Uint64(val)
			encountered[cur] = true
			if cur != expected {
				return nil, fmt.Errorf("%v, Expected to find %v but found %v", connIdx, expected, cur)
			}
			cur++
			binary.BigEndian.PutUint64(val, cur)
			if err := obj.Set(val); err != nil {
				return nil, err
			}
			return cur, nil
		})
		if err != nil {
			return err
		}
		expected++
		if res.(uint64) == limit {
			break
		}
	}
	for n := uint64(0); n < limit; n++ {
		if !encountered[n] {
			return fmt.Errorf("%v: Failed to encounter %v", connIdx, n)
		}
	}
	return nil
}
