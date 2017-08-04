package pqueue

import (
	"encoding/binary"
	"goshawkdb.io/client"
	"goshawkdb.io/common"
	"goshawkdb.io/tests/harness"
	"sync"
)

func PQueue(th *harness.TestHelper) {
	parCount := 16
	limit := uint64(1000)
	conn := th.CreateConnections(1)[0]

	defer th.Shutdown()
	vsn, _ := conn.SetRootToNZeroObjs(parCount)
	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(parCount)
	endBarrier, errCh := th.InParallel(parCount, func(idx int, conn *harness.Connection) error {
		return runEnqueue(idx, conn, vsn, limit, startBarrier)
	})
	go func() {
		endBarrier.Wait()
		close(errCh)
	}()
	th.MaybeFatal(<-errCh)
}

func runEnqueue(connIdx int, conn *harness.Connection, rootVsn *common.TxnId, limit uint64, startBarrier *sync.WaitGroup) error {
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

	val := make([]byte, 8)
	for ; limit > 1; limit-- {
		binary.BigEndian.PutUint64(val, limit)
		_, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			root, err := txn.GetObject(myObjRef)
			if err != nil {
				return nil, err
			}
			rootVal, rootRefs, err := root.ValueReferences()
			if err != nil {
				return nil, err
			}
			if len(rootRefs) == 0 {
				newHead, err := txn.CreateObject(val)
				if err != nil {
					return nil, err
				}
				return nil, root.Set(rootVal, newHead)
			} else {
				oldHead := rootRefs[0]
				newHead, err := txn.CreateObject(val, oldHead)
				if err != nil {
					return nil, err
				}
				return nil, root.Set(rootVal, newHead)
			}
		})
		if err != nil {
			return err
		}
	}

	return nil
}
