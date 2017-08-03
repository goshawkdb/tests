package writeskew

import (
	"encoding/binary"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/common"
	"goshawkdb.io/tests/harness"
	"sync"
)

// This tests for the A5B write skew anomaly.
func WriteSkew(th *harness.TestHelper) {
	parIncrs := 8
	parReset := 4
	iterations := 500

	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	rootVsn, _ := conn.SetRootToNZeroObjs(2)

	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(parIncrs + parReset)
	endBarrierIncrs, errChIncrs := th.InParallel(parIncrs, func(connIdx int, conn *harness.Connection) error {
		return incr(connIdx, conn, rootVsn, iterations, startBarrier)
	})

	go func() {
		endBarrierIncrs.Wait()
		close(errChIncrs)
	}()

	endBarrierReset, errChReset := th.InParallel(parReset, func(connIdx int, conn *harness.Connection) error {
		return reset(conn, rootVsn, startBarrier, errChIncrs)
	})

	go func() {
		endBarrierReset.Wait()
		close(errChReset)
	}()
	th.MaybeFatal(<-errChIncrs)
	th.MaybeFatal(<-errChReset)
}

func incr(connNum int, conn *harness.Connection, rootVsn *common.TxnId, itrs int, startBarrier *sync.WaitGroup) error {
	err := conn.AwaitRootVersionChange(rootVsn)
	startBarrier.Done()
	if err != nil {
		return err
	}
	startBarrier.Wait()
	incrIdx := connNum % 2
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, 1)
	for ; itrs > 0; itrs-- {
		res, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			rootObj, err := conn.GetRootObject(txn)
			if err != nil {
				return nil, err
			}
			objs, err := rootObj.References()
			if err != nil {
				return nil, err
			}
			xVal, err := objs[0].Value()
			if err != nil {
				return nil, err
			}
			yVal, err := objs[1].Value()
			if err != nil {
				return nil, err
			}
			x := binary.BigEndian.Uint64(xVal)
			y := binary.BigEndian.Uint64(yVal)
			switch {
			case x == 0 && y == 0:
				z := objs[incrIdx]
				return false, z.Set(buf)
			case x == 1 && y == 1:
				return true, nil
			default:
				return client.Retry, nil
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

func reset(conn *harness.Connection, rootVsn *common.TxnId, startBarrier *sync.WaitGroup, terminate chan error) error {
	err := conn.AwaitRootVersionChange(rootVsn)
	startBarrier.Done()
	if err != nil {
		return err
	}
	startBarrier.Wait()
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, 0)
	for {
		select {
		case <-terminate:
			return nil
		default:
		}
		res, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			rootObj, err := conn.GetRootObject(txn)
			if err != nil {
				return nil, err
			}
			objs, err := rootObj.References()
			if err != nil {
				return nil, err
			}
			xVal, err := objs[0].Value()
			if err != nil {
				return nil, err
			}
			yVal, err := objs[1].Value()
			if err != nil {
				return nil, err
			}
			x := binary.BigEndian.Uint64(xVal)
			y := binary.BigEndian.Uint64(yVal)
			switch {
			case x == 1 && y == 1:
				return true, nil
			case x == 1:
				return false, objs[0].Set(buf)
			case y == 1:
				return false, objs[1].Set(buf)
			default:
				select {
				case <-terminate: // don't retry if the incrs have all finished
					return false, nil
				default:
					return client.Retry, nil
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
}
