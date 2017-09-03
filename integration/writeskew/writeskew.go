package writeskew

import (
	"encoding/binary"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/tests/harness"
	"sync"
)

// This tests for the A5B write skew anomaly.
func WriteSkew(th *harness.TestHelper) {
	parIncrs := 8
	parReset := 4
	iterations := 1000

	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	guidBuf, err := conn.SetRootToNZeroObjs(2)
	th.MaybeFatal(err)

	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(parIncrs + parReset)
	endBarrierIncrs, errChIncrs := th.InParallel(parIncrs, func(connIdx int, conn *harness.Connection) error {
		return incr(connIdx, 2, conn, guidBuf, iterations, startBarrier)
	})

	go func() {
		endBarrierIncrs.Wait()
		close(errChIncrs)
	}()

	endBarrierReset, errChReset := th.InParallel(parReset, func(connIdx int, conn *harness.Connection) error {
		return reset(conn, 2, guidBuf, startBarrier, errChIncrs)
	})

	go func() {
		endBarrierReset.Wait()
		close(errChReset)
	}()
	th.MaybeFatal(<-errChIncrs)
	th.MaybeFatal(<-errChReset)
}

func incr(connNum, objCount int, conn *harness.Connection, guidBuf []byte, itrs int, startBarrier *sync.WaitGroup) error {
	rootRefs, err := conn.AwaitRootVersionChange(guidBuf, objCount)
	startBarrier.Done()
	if err != nil {
		return err
	}
	startBarrier.Wait()
	incrIdx := connNum % 2
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, 1)
	for ; itrs > 0; itrs-- {
		res, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
			if xVal, _, err := txn.Read(rootRefs[0]); err != nil || txn.RestartNeeded() {
				return nil, err
			} else if yVal, _, err := txn.Read(rootRefs[1]); err != nil || txn.RestartNeeded() {
				return nil, err
			} else {
				x := binary.BigEndian.Uint64(xVal)
				y := binary.BigEndian.Uint64(yVal)
				switch {
				case x == 0 && y == 0:
					z := rootRefs[incrIdx]
					return false, txn.Write(z, buf)
				case x == 1 && y == 1:
					return true, nil
				default:
					return nil, txn.Retry()
				}
			}
		})
		if err != nil {
			return err
		}
		if res.(bool) {
			return fmt.Errorf("Discovered both x and y are 1!")
		}
	}
	return nil
}

func reset(conn *harness.Connection, objCount int, guidBuf []byte, startBarrier *sync.WaitGroup, terminate chan error) error {
	rootRefs, err := conn.AwaitRootVersionChange(guidBuf, objCount)
	startBarrier.Done()
	if err != nil {
		return err
	}
	startBarrier.Wait()
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, 0)
	finished := false
	for !finished {
		res, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
			if xVal, _, err := txn.Read(rootRefs[0]); err != nil || txn.RestartNeeded() {
				return nil, err
			} else if yVal, _, err := txn.Read(rootRefs[1]); err != nil || txn.RestartNeeded() {
				return nil, err
			} else {
				x := binary.BigEndian.Uint64(xVal)
				y := binary.BigEndian.Uint64(yVal)
				switch {
				case x == 1 && y == 1:
					return true, nil
				case x == 1:
					return false, txn.Write(rootRefs[0], buf)
				case y == 1:
					return false, txn.Write(rootRefs[1], buf)
				default:
					select {
					case <-terminate: // don't retry if the incrs have all finished
						finished = true
						return false, nil
					default:
						return nil, txn.Retry()
					}
				}
			}
		})
		if err != nil {
			return err
		}
		if res.(bool) {
			return fmt.Errorf("Discovered both x and y are 1!")
		}
	}
	return nil
}
