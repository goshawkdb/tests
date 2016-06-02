package simpleconflict

import (
	"encoding/binary"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/common"
	"goshawkdb.io/tests"
	"sync"
)

func SimpleConflict(th *tests.TestHelper) {
	parCount := 5
	objCount := 3
	limit := uint64(1000)
	conn := th.CreateConnections(1)[0]

	defer th.Shutdown()
	vsn, _ := conn.SetRootToNZeroObjs(objCount)
	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(parCount)
	endBarrier, errCh := th.InParallel(parCount, func(idx int, conn *tests.Connection) error {
		return runConflictCount(idx, conn, vsn, limit, startBarrier)
	})
	go func() {
		endBarrier.Wait()
		close(errCh)
	}()
	th.MaybeFatal(<-errCh)
}

func runConflictCount(connIdx int, conn *tests.Connection, rootVsn *common.TxnId, limit uint64, startBarrier *sync.WaitGroup) error {
	err := conn.AwaitRootVersionChange(rootVsn)
	startBarrier.Done()
	if err != nil {
		return err
	}
	startBarrier.Wait()
	objsVarUUIds := []*common.VarUUId{}
	_, _, err = conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		objsVarUUIds = objsVarUUIds[:0] // must reset the slice whenever we restart this txn
		rootObj, err := txn.GetRootObject()
		if err != nil {
			return nil, err
		}
		refs, err := rootObj.References()
		if err != nil {
			return nil, err
		}
		for _, obj := range refs {
			objsVarUUIds = append(objsVarUUIds, obj.Id)
		}
		return nil, nil
	})
	if err != nil {
		return err
	}
	buf := make([]byte, 8)
	for {
		res, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			obj, err := txn.GetObject(objsVarUUIds[0])
			if err != nil {
				return nil, err
			}
			val, err := obj.Value()
			if err != nil {
				return nil, err
			}
			cur := binary.BigEndian.Uint64(val)
			limitReached := cur == limit
			if !limitReached {
				binary.BigEndian.PutUint64(buf, cur+1)
				err := obj.Set(buf)
				if err != nil {
					return nil, err
				}
			}
			for _, vUUId := range objsVarUUIds[1:] {
				obj, err = txn.GetObject(vUUId)
				if err != nil {
					return nil, err
				}
				val, err = obj.Value()
				if err != nil {
					return nil, err
				}
				if num := binary.BigEndian.Uint64(val); cur != num {
					return nil, fmt.Errorf("%v, Expected to find %v but found %v", connIdx, cur, num)
				}
				if !limitReached {
					obj.Set(buf)
				}
			}
			return cur, nil
		})
		if err != nil {
			return err
		}
		if res.(uint64) == limit {
			break
		}
	}
	return nil
}
