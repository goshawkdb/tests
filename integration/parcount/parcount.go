package parcount

import (
	"encoding/binary"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/tests/harness"
	"sync"
)

func ParCount(th *harness.TestHelper) {
	parCount := 16
	limit := uint64(1000)
	conn := th.CreateConnections(1)[0]

	defer th.Shutdown()
	guidBuf, _ := conn.SetRootToNZeroObjs(parCount)
	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(parCount)
	endBarrier, errCh := th.InParallel(parCount, func(idx int, conn *harness.Connection) error {
		return runCount(idx, parCount, conn, guidBuf, limit, startBarrier)
	})
	go func() {
		endBarrier.Wait()
		close(errCh)
	}()
	th.MaybeFatal(<-errCh)
}

func runCount(connIdx, parCount int, conn *harness.Connection, guidBuf []byte, limit uint64, startBarrier *sync.WaitGroup) error {
	rootRefs, err := conn.AwaitRootVersionChange(guidBuf, parCount)
	startBarrier.Done()
	if err != nil {
		return err
	}
	startBarrier.Wait()
	objPtr := rootRefs[connIdx]
	encountered := make(map[uint64]bool)
	expected := uint64(0)
	for {
		res, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
			if val, _, err := txn.Read(objPtr); err != nil || txn.RestartNeeded() {
				return nil, err
			} else {
				cur := binary.BigEndian.Uint64(val)
				encountered[cur] = true
				if cur != expected {
					return nil, fmt.Errorf("%v, Expected to find %v but found %v", connIdx, expected, cur)
				}
				cur++
				binary.BigEndian.PutUint64(val, cur)
				if err := txn.Write(objPtr, val); err != nil || txn.RestartNeeded() {
					return nil, err
				} else {
					return cur, nil
				}
			}
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
