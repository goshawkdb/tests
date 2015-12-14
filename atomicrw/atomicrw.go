package atomicrw

import (
	"encoding/binary"
	"goshawkdb.io/client"
	"goshawkdb.io/common"
	"goshawkdb.io/tests"
	"sync"
	"time"
)

// This is a variant of the write skew test, but this version doesn't
// rely on retry. Basically, the two txns in use are:
// t1: if x%2 == 0 then {x = x+1; y = x} else {x = x+1}
// t2: if x%2 == 0 then {y = x+2} else {x = x+1}
// Thus the only way that x goes odd is the first branch of t1. So if
// we observe an odd x, then we must have x == y
func AtomicRW(th *tests.TestHelper) {
	attempts := 10000

	conns := 4

	th.CreateConnections(1 + conns)
	defer th.Shutdown()

	vsn := th.SetRootToNZeroObjs(2)

	startBarrier, endBarrier := new(sync.WaitGroup), new(sync.WaitGroup)
	startBarrier.Add(conns)
	endBarrier.Add(conns)

	for idx := 0; idx < conns; idx++ {
		connNum := idx + 1
		go runTxn(th, vsn, connNum, attempts, startBarrier, endBarrier)
	}

	c := make(chan struct{})
	go func() {
		endBarrier.Wait()
		close(c)
	}()

	startBarrier.Wait()
	runObserver(th, c)
	runObserver(th, c)
}

func runTxn(th *tests.TestHelper, rootVsn *common.TxnId, connNum int, attempts int, startBarrier, endBarrier *sync.WaitGroup) {
	defer endBarrier.Done()
	th.AwaitRootVersionChange(connNum, rootVsn)
	startBarrier.Done()
	startBarrier.Wait()
	buf := make([]byte, 8)
	for ; attempts > 0; attempts-- {
		time.Sleep(10 * time.Millisecond)
		if attempts%2 == 0 {
			th.RunTransaction(connNum, func(txn *client.Txn) (interface{}, error) {
				rootObj, err := txn.GetRootObject()
				if err != nil {
					return nil, err
				}
				objs, err := rootObj.References()
				if err != nil {
					return nil, err
				}
				xObj := objs[0]
				yObj := objs[1]
				xVal, err := xObj.Value()
				if err != nil {
					return nil, err
				}
				x := binary.BigEndian.Uint64(xVal)
				if x%2 == 0 {
					binary.BigEndian.PutUint64(buf, x+1)
					if err = xObj.Set(buf); err != nil {
						return nil, err
					}
					if err = yObj.Set(buf); err != nil {
						return nil, err
					}
				} else {
					binary.BigEndian.PutUint64(buf, x+1)
					if err = xObj.Set(buf); err != nil {
						return nil, err
					}
				}
				return nil, nil
			})
		} else {
			th.RunTransaction(connNum, func(txn *client.Txn) (interface{}, error) {
				rootObj, err := txn.GetRootObject()
				if err != nil {
					return nil, err
				}
				objs, err := rootObj.References()
				if err != nil {
					return nil, err
				}
				xObj := objs[0]
				yObj := objs[1]
				xVal, err := xObj.Value()
				if err != nil {
					return nil, err
				}
				x := binary.BigEndian.Uint64(xVal)
				if x%2 == 0 {
					binary.BigEndian.PutUint64(buf, x+2)
					return nil, yObj.Set(buf)
				} else {
					binary.BigEndian.PutUint64(buf, x+1)
					return nil, xObj.Set(buf)
				}
			})
		}
	}
}

func runObserver(th *tests.TestHelper, terminate chan struct{}) {
	var x, y uint64
	for {
		res, _ := th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
			rootObj, err := txn.GetRootObject()
			if err != nil {
				return nil, err
			}
			objs, err := rootObj.References()
			if err != nil {
				return nil, err
			}
			xObj := objs[0]
			yObj := objs[1]
			xVal, err := xObj.Value()
			if err != nil {
				return nil, err
			}
			x := binary.BigEndian.Uint64(xVal)
			yVal, err := yObj.Value()
			if err != nil {
				return nil, err
			}
			y := binary.BigEndian.Uint64(yVal)
			if x%2 == 0 {
				return nil, nil
			} else {
				// x is odd, so x should == y
				return x == y, nil
			}
		})
		if resBool, ok := res.(bool); ok && !resBool {
			th.Fatal("Observed x ==", x, "y ==", y)
		}
		select {
		case <-terminate:
			return
		default:
		}
	}
}
