package simpleconflict

import (
	"encoding/binary"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/tests/harness"
	"sync"
)

func SimpleConflict(th *harness.TestHelper) {
	parCount := 5
	objCount := 3
	limit := uint64(10000)
	conn := th.CreateConnections(1)[0]

	defer th.Shutdown()
	guidBuf, err := conn.SetRootToNZeroObjs(objCount)
	th.MaybeFatal(err)
	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(parCount)
	endBarrier, errCh := th.InParallel(parCount, func(idx int, conn *harness.Connection) error {
		return runConflictCount(idx, objCount, conn, guidBuf, limit, startBarrier)
	})
	go func() {
		endBarrier.Wait()
		close(errCh)
	}()
	th.MaybeFatal(<-errCh)
}

func runConflictCount(connIdx, objCount int, conn *harness.Connection, guidBuf []byte, limit uint64, startBarrier *sync.WaitGroup) error {
	rootRefs, err := conn.AwaitRootVersionChange(guidBuf, objCount)
	startBarrier.Done()
	if err != nil {
		return err
	}
	startBarrier.Wait()
	objPtr := rootRefs[0]
	rootRefsRem := rootRefs[1:]
	for {
		res, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
			if val0, _, err := txn.Read(objPtr); err != nil || txn.RestartNeeded() {
				return nil, err
			} else {
				cur := binary.BigEndian.Uint64(val0)
				limitReached := cur == limit
				if !limitReached {
					binary.BigEndian.PutUint64(val0, cur+1)
					if err = txn.Write(objPtr, val0); err != nil || txn.RestartNeeded() {
						return nil, err
					}
				}
				for _, ptr := range rootRefsRem {
					if val, _, err := txn.Read(ptr); err != nil || txn.RestartNeeded() {
						return nil, err
					} else {
						if num := binary.BigEndian.Uint64(val); cur != num {
							return nil, fmt.Errorf("%v, Expected to find %v but found %v", connIdx, cur, num)
						}
						if !limitReached {
							if err := txn.Write(ptr, val0); err != nil || txn.RestartNeeded() {
								return nil, err
							}
						}
					}
				}
				return cur, nil
			}
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
