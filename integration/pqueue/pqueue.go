package pqueue

import (
	"encoding/binary"
	"goshawkdb.io/client"
	"goshawkdb.io/tests/harness"
	"sync"
)

func PQueue(th *harness.TestHelper) {
	parCount := 12
	limit := uint64(1000)
	conn := th.CreateConnections(1)[0]

	defer th.Shutdown()
	guidBuf, err := conn.SetRootToNZeroObjs(parCount)
	th.MaybeFatal(err)
	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(parCount)
	endBarrier, errCh := th.InParallel(parCount, func(idx int, conn *harness.Connection) error {
		return runEnqueue(idx, parCount, conn, guidBuf, limit, startBarrier)
	})
	go func() {
		endBarrier.Wait()
		close(errCh)
	}()
	th.MaybeFatal(<-errCh)
}

func runEnqueue(connIdx, parCount int, conn *harness.Connection, guidBuf []byte, limit uint64, startBarrier *sync.WaitGroup) error {
	rootRefs, err := conn.AwaitRootVersionChange(guidBuf, parCount)
	startBarrier.Done()
	if err != nil {
		return err
	}
	startBarrier.Wait()
	myObjRef := rootRefs[connIdx]

	val := make([]byte, 8)
	for ; limit > 1; limit-- {
		binary.BigEndian.PutUint64(val, limit)
		_, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
			if rootVal, rootRefs, err := txn.Read(myObjRef); err != nil || txn.RestartNeeded() {
				return nil, err
			} else if len(rootRefs) == 0 {
				if newHead, err := txn.Create(val); err != nil || txn.RestartNeeded() {
					return nil, err
				} else {
					return nil, txn.Write(myObjRef, rootVal, newHead)
				}
			} else {
				oldHead := rootRefs[0]
				if newHead, err := txn.Create(val, oldHead); err != nil || txn.RestartNeeded() {
					return nil, err
				} else {
					return nil, txn.Write(myObjRef, rootVal, newHead)
				}
			}
		})
		if err != nil {
			return err
		}
	}

	return nil
}
