package simpleconflict

import (
	"encoding/binary"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/common"
	"goshawkdb.io/tests/harness"
	"sync"
)

func SimpleConflict(th *harness.TestHelper) {
	parCount := 5
	objCount := 3
	limit := uint64(1000)
	conn := th.CreateConnections(1)[0]

	defer th.Shutdown()
	vsn, _ := conn.SetRootToNZeroObjs(objCount)
	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(parCount)
	endBarrier, errCh := th.InParallel(parCount, func(idx int, conn *harness.Connection) error {
		return runConflictCount(idx, conn, vsn, limit, startBarrier)
	})
	go func() {
		endBarrier.Wait()
		close(errCh)
	}()
	th.MaybeFatal(<-errCh)
}

func runConflictCount(connIdx int, conn *harness.Connection, rootVsn *common.TxnId, limit uint64, startBarrier *sync.WaitGroup) error {
	err := conn.AwaitRootVersionChange(rootVsn)
	startBarrier.Done()
	if err != nil {
		return err
	}
	startBarrier.Wait()
	for {
		res, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			rootObj, err := conn.GetRootObject(txn)
			if err != nil {
				return nil, err
			}
			refs, err := rootObj.References()
			if err != nil {
				return nil, err
			}
			obj := refs[0]
			val0, err := obj.Value()
			if err != nil {
				return nil, err
			}
			cur := binary.BigEndian.Uint64(val0)
			limitReached := cur == limit
			if !limitReached {
				binary.BigEndian.PutUint64(val0, cur+1)
				err := obj.Set(val0)
				if err != nil {
					return nil, err
				}
			}
			for _, obj := range refs[1:] {
				val, err := obj.Value()
				if err != nil {
					return nil, err
				}
				if num := binary.BigEndian.Uint64(val); cur != num {
					return nil, fmt.Errorf("%v, Expected to find %v but found %v", connIdx, cur, num)
				}
				if !limitReached {
					obj.Set(val0)
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
