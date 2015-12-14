package writeskew

import (
	"encoding/binary"
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

	rootVsn := th.SetRootToNZeroObjs(2)

	startBarrier, endBarrierIncrs, endBarrierReset := new(sync.WaitGroup), new(sync.WaitGroup), new(sync.WaitGroup)
	startBarrier.Add(parIncrs + parReset)
	endBarrierIncrs.Add(parIncrs)
	endBarrierReset.Add(parReset)
	for idx := 0; idx < parIncrs; idx++ {
		connNum := idx + 1
		go incr(connNum, th, rootVsn, iterations, startBarrier, endBarrierIncrs)
	}

	c := make(chan struct{})
	go func() {
		endBarrierIncrs.Wait()
		close(c)
	}()

	for idx := 0; idx < parReset; idx++ {
		connNum := parIncrs + idx + 1
		go reset(connNum, th, rootVsn, startBarrier, endBarrierReset, c)
	}

	endBarrierReset.Wait()
}

func incr(connNum int, th *tests.TestHelper, rootVsn *common.TxnId, itrs int, startBarrier, endBarrier *sync.WaitGroup) {
	defer endBarrier.Done()
	th.AwaitRootVersionChange(connNum, rootVsn)
	startBarrier.Done()
	startBarrier.Wait()
	incrIdx := connNum % 2
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, 1)
	for ; itrs > 0; itrs-- {
		res, _ := th.RunTransaction(connNum, func(txn *client.Txn) (interface{}, error) {
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
		if res.(bool) {
			th.Fatal("Discovered both x and y are 1!")
		}
	}
}

func reset(connNum int, th *tests.TestHelper, rootVsn *common.TxnId, startBarrier, endBarrier *sync.WaitGroup, terminate chan struct{}) {
	defer endBarrier.Done()
	th.AwaitRootVersionChange(connNum, rootVsn)
	startBarrier.Done()
	startBarrier.Wait()
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, 0)
	for {
		res, _ := th.RunTransaction(connNum, func(txn *client.Txn) (interface{}, error) {
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
		if res.(bool) {
			th.Fatal("Discovered both x and y are 1!")
		}
		select {
		case <-terminate:
			return
		default:
		}
	}
}
