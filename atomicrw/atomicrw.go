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

	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	vsn, _ := conn.SetRootToNZeroObjs(2)

	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(conns)

	endBarrier, errCh := th.InParallel(conns, func(connIdx int, conn *tests.Connection) error {
		return runTxn(conn, vsn, attempts, startBarrier)
	})

	c := make(chan struct{})
	go func() {
		endBarrier.Wait()
		close(c)
		close(errCh)
	}()

	startBarrier.Wait()
	runObserver(conn, c)
	runObserver(conn, c)
	th.MaybeFatal(<-errCh)
}

func runTxn(conn *tests.Connection, rootVsn *common.TxnId, attempts int, startBarrier *sync.WaitGroup) error {
	err := conn.AwaitRootVersionChange(rootVsn)
	startBarrier.Done()
	if err != nil {
		return err
	}
	startBarrier.Wait()
	for ; attempts > 0; attempts-- {
		time.Sleep(10 * time.Millisecond)
		if attempts%2 == 0 {
			_, _, err = conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
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
				binary.BigEndian.PutUint64(xVal, x+1)
				if err = xObj.Set(xVal); err != nil {
					return nil, err
				}
				if x%2 == 0 {
					if err = yObj.Set(xVal); err != nil {
						return nil, err
					}
				}
				return nil, nil
			})
			if err != nil {
				return err
			}
		} else {
			_, _, err = conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
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
					binary.BigEndian.PutUint64(xVal, x+2)
					return nil, yObj.Set(xVal)
				} else {
					binary.BigEndian.PutUint64(xVal, x+1)
					return nil, xObj.Set(xVal)
				}
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func runObserver(conn *tests.Connection, terminate chan struct{}) {
	var x, y uint64
	for {
		select {
		case <-terminate:
			return
		default:
		}
		time.Sleep(10 * time.Millisecond)
		res, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
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
			x = binary.BigEndian.Uint64(xVal)
			yVal, err := yObj.Value()
			if err != nil {
				return nil, err
			}
			y = binary.BigEndian.Uint64(yVal)
			if x%2 == 0 {
				return nil, nil
			} else {
				// x is odd, so x should == y
				return x == y, nil
			}
		})
		conn.MaybeFatal(err)
		if resBool, ok := res.(bool); ok && !resBool {
			conn.Fatal("Observed x ==", x, "y ==", y)
		}
	}
}
