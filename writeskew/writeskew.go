package writeskew

import (
	"encoding/binary"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/common"
	"goshawkdb.io/tests"
	"sync"
)

// This tests for the A5B write skew anomaly.
func WriteSkew(th *tests.TestHelper) {
	parIncrs := 8
	parReset := 4
	iterations := 500

	th.CreateConnections(parIncrs + parReset + 1)
	defer th.Shutdown()

	rootVsn, err := th.SetRootToNZeroObjs(2)
	th.MaybeFatal(err)

	startBarrier, endBarrierIncrs, endBarrierReset := new(sync.WaitGroup), new(sync.WaitGroup), new(sync.WaitGroup)
	startBarrier.Add(parIncrs + parReset)
	endBarrierIncrs.Add(parIncrs)
	endBarrierReset.Add(parReset)
	errCh := make(chan error, parIncrs+parReset)
	for idx := 0; idx < parIncrs; idx++ {
		connNum := idx + 1
		go incr(connNum, th, rootVsn, iterations, startBarrier, endBarrierIncrs, errCh)
	}

	c := make(chan struct{})
	go func() {
		endBarrierIncrs.Wait()
		close(c)
	}()

	for idx := 0; idx < parReset; idx++ {
		connNum := parIncrs + idx + 1
		go reset(connNum, th, rootVsn, startBarrier, endBarrierReset, c, errCh)
	}

	go func() {
		endBarrierReset.Wait()
		close(errCh)
	}()
	th.MaybeFatal(<-errCh)
}

func incr(connNum int, th *tests.TestHelper, rootVsn *common.TxnId, itrs int, startBarrier, endBarrier *sync.WaitGroup, errCh chan error) {
	defer endBarrier.Done()
	err := th.AwaitRootVersionChange(connNum, rootVsn)
	startBarrier.Done()
	if err != nil {
		errCh <- err
		return
	}
	startBarrier.Wait()
	incrIdx := connNum % 2
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, 1)
	for ; itrs > 0; itrs-- {
		res, _, err := th.RunTransaction(connNum, func(txn *client.Txn) (interface{}, error) {
			rootObj, err := txn.GetRootObject()
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
			errCh <- err
			return
		}
		if res.(bool) {
			errCh <- fmt.Errorf("Discovered both x and y are 1!")
			return
		}
	}
}

func reset(connNum int, th *tests.TestHelper, rootVsn *common.TxnId, startBarrier, endBarrier *sync.WaitGroup, terminate chan struct{}, errCh chan error) {
	defer endBarrier.Done()
	err := th.AwaitRootVersionChange(connNum, rootVsn)
	startBarrier.Done()
	if err != nil {
		errCh <- err
		return
	}
	startBarrier.Wait()
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, 0)
	for {
		res, _, err := th.RunTransaction(connNum, func(txn *client.Txn) (interface{}, error) {
			rootObj, err := txn.GetRootObject()
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
				// erm, well this is curious!
				select {
				case <-terminate:
					return false, nil
				default:
					return client.Retry, nil
				}
			}
		})
		if err != nil {
			errCh <- err
			return
		}
		if res.(bool) {
			errCh <- fmt.Errorf("Discovered both x and y are 1!")
			return
		}
		select {
		case <-terminate:
			return
		default:
		}
	}
}
