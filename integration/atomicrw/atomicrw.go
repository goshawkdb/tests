package atomicrw

import (
	"encoding/binary"
	"goshawkdb.io/client"
	"goshawkdb.io/tests/harness"
	"sync"
	"time"
)

// This is a variant of the write skew test, but this version doesn't
// rely on retry. Basically, the two txns in use are:
// t1: if x%2 == 0 then {x = x+1; y = x} else {x = x+1}
// t2: if x%2 == 0 then {y = x+2} else {x = x+1}
// Thus the only way that x goes odd is the first branch of t1. So if
// we observe an odd x, then we must have x == y
func AtomicRW(th *harness.TestHelper) {
	attempts := 10000

	conns := 4

	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	guidBuf, err := conn.SetRootToNZeroObjs(2)
	th.MaybeFatal(err)

	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(conns)

	endBarrier, errCh := th.InParallel(conns, func(connIdx int, conn *harness.Connection) error {
		return runTxn(conn, 2, guidBuf, attempts, startBarrier)
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

func runTxn(conn *harness.Connection, objCount int, guidBuf []byte, attempts int, startBarrier *sync.WaitGroup) error {
	rootRefs, err := conn.AwaitRootVersionChange(guidBuf, objCount)
	startBarrier.Done()
	if err != nil {
		return err
	}
	startBarrier.Wait()
	for ; attempts > 0; attempts-- {
		if attempts%500 == 0 {
			conn.Log("attempts", attempts)
		}
		time.Sleep(2 * time.Millisecond)
		if attempts%2 == 0 {
			_, err = conn.Transact(func(txn *client.Transaction) (interface{}, error) {
				xObj := rootRefs[0]
				yObj := rootRefs[1]
				if xVal, _, err := txn.Read(xObj); err != nil || txn.RestartNeeded() {
					return nil, err
				} else {
					x := binary.BigEndian.Uint64(xVal)
					binary.BigEndian.PutUint64(xVal, x+1)
					if err = txn.Write(xObj, xVal); err != nil || txn.RestartNeeded() {
						return nil, err
					}
					if x%2 == 0 {
						return nil, txn.Write(yObj, xVal)
					} else {
						return nil, nil
					}
				}
			})
			if err != nil {
				return err
			}
		} else {
			_, err = conn.Transact(func(txn *client.Transaction) (interface{}, error) {
				xObj := rootRefs[0]
				yObj := rootRefs[1]
				if xVal, _, err := txn.Read(xObj); err != nil || txn.RestartNeeded() {
					return nil, err
				} else {
					x := binary.BigEndian.Uint64(xVal)
					if x%2 == 0 {
						binary.BigEndian.PutUint64(xVal, x+2)
						return nil, txn.Write(yObj, xVal)
					} else {
						binary.BigEndian.PutUint64(xVal, x+1)
						return nil, txn.Write(xObj, xVal)
					}
				}
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func runObserver(conn *harness.Connection, terminate chan struct{}) {
	var x, y uint64
	for {
		select {
		case <-terminate:
			return
		default:
			time.Sleep(2 * time.Millisecond)
			res, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
				rootPtr, _ := txn.Root(conn.RootName)
				if _, rootRefs, err := txn.Read(rootPtr); err != nil || txn.RestartNeeded() {
					return nil, err
				} else {
					xObj := rootRefs[0]
					yObj := rootRefs[1]
					if xVal, _, err := txn.Read(xObj); err != nil || txn.RestartNeeded() {
						return nil, err
					} else {
						x = binary.BigEndian.Uint64(xVal)
						if yVal, _, err := txn.Read(yObj); err != nil || txn.RestartNeeded() {
							return nil, err
						} else {
							y = binary.BigEndian.Uint64(yVal)
							if x%2 == 0 {
								return nil, nil
							} else {
								// x is odd, so x should == y
								return x == y, nil
							}
						}
					}
				}
			})
			conn.MaybeFatal(err)
			if resBool, ok := res.(bool); ok && !resBool {
				conn.Fatal("Observed x ==", x, "y ==", y)
			}
		}
	}
}
