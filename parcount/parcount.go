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
	th.CreateConnections(parCount)

	defer th.Shutdown()
	vsn, err := th.SetRootToNZeroObjs(parCount)
	th.MaybeFatal(err)
	startBarrier, endBarrier := new(sync.WaitGroup), new(sync.WaitGroup)
	startBarrier.Add(parCount)
	endBarrier.Add(parCount)
	errCh := make(chan error, parCount)
	for idx := 0; idx < parCount; idx++ {
		idxCopy := idx
		go runCount(idxCopy, th, vsn, limit, startBarrier, endBarrier, errCh)
	}
	go func() {
		endBarrier.Wait()
		close(errCh)
	}()
	th.MaybeFatal(<-errCh)
}

func runCount(connIdx int, th *tests.TestHelper, rootVsn *common.TxnId, limit uint64, startBarrier, endBarrier *sync.WaitGroup, errCh chan error) {
	defer endBarrier.Done()
	err := th.AwaitRootVersionChange(connIdx, rootVsn)
	startBarrier.Done()
	if err != nil {
		errCh <- err
		return
	}
	startBarrier.Wait()
	var myObjVarUUId *common.VarUUId
	_, _, err = th.RunTransaction(connIdx, func(txn *client.Txn) (interface{}, error) {
		rootObj, err := txn.GetRootObject()
		if err != nil {
			return nil, err
		}
		refs, err := rootObj.References()
		if err != nil {
			return nil, err
		}
		myObj := refs[connIdx]
		myObjVarUUId = myObj.Id
		return nil, nil
	})
	if err != nil {
		errCh <- err
		return
	}
	encountered := make(map[uint64]bool)
	expected := uint64(0)
	buf := make([]byte, 8)
	for {
		res, _, _ := th.RunTransaction(connIdx, func(txn *client.Txn) (interface{}, error) {
			obj, err := txn.GetObject(myObjVarUUId)
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
			binary.BigEndian.PutUint64(buf, cur)
			if err := obj.Set(buf); err != nil {
				return nil, err
			}
			return cur, nil
		})
		if err != nil {
			errCh <- err
			return
		}
		expected++
		if res.(uint64) == limit {
			break
		}
	}
	for n := uint64(0); n < limit; n++ {
		if !encountered[n] {
			errCh <- fmt.Errorf("%v: Failed to encounter %v", connIdx, n)
			return
		}
	}
}
