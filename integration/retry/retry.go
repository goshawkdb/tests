package retry

import (
	"encoding/binary"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/tests/harness"
	"sync"
	"time"
)

// Test that one write wakes up many retriers
func SimpleRetry(th *harness.TestHelper) {
	retriers := 9
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	guidBuf, err := conn.SetRootToNZeroObjs(1)
	th.MaybeFatal(err)
	magicNumber := uint64(42)

	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(retriers)
	endBarrier, errCh := th.InParallel(retriers, func(connIdx int, conn *harness.Connection) error {
		if rootRefs, err := conn.AwaitRootVersionChange(guidBuf, 1); err != nil {
			return err
		} else {
			triggered := false
			objPtr := rootRefs[0]
			res, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
				if val, _, err := txn.Read(objPtr); err != nil || txn.RestartNeeded() {
					return nil, err
				} else if num := binary.BigEndian.Uint64(val); num == 0 {
					if !triggered {
						triggered = true
						startBarrier.Done() // trigger the change in the other txn
					}
					return nil, txn.Retry()
				} else {
					return num, nil
				}
			})
			if err != nil {
				if !triggered {
					startBarrier.Done()
				}
				return err
			}
			num := res.(uint64)
			if num != magicNumber {
				return fmt.Errorf("%v Expected %v, got %v", connIdx, magicNumber, num)
			}
			return nil
		}
	})

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, magicNumber)
	startBarrier.Wait()
	time.Sleep(250 * time.Millisecond)
	if rootRefs, err := conn.AwaitRootVersionChange(guidBuf, 1); err != nil {
		th.MaybeFatal(err)
	} else {
		objPtr := rootRefs[0]
		_, err = conn.Transact(func(txn *client.Transaction) (interface{}, error) {
			return nil, txn.Write(objPtr, buf)
		})
		th.MaybeFatal(err)

		go func() {
			endBarrier.Wait()
			close(errCh)
		}()
		th.MaybeFatal(<-errCh)
	}
}

// Test that a retry on several objs gets restarted by one write.
func DisjointRetry(th *harness.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	changes := []bool{false, false, true}
	objCount := len(changes)
	guidBuf, err := conn.SetRootToNZeroObjs(objCount)
	th.MaybeFatal(err)
	magicNumber := uint64(43)

	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(1)

	endBarrier, errCh := th.InParallel(1, func(connIdx int, conn *harness.Connection) error {
		if rootRefs, err := conn.AwaitRootVersionChange(guidBuf, objCount); err != nil {
			return err
		} else {
			triggered := false
			changed := []bool{}
			_, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
				changed = changed[:0]
				anyChange := false
				for _, objPtr := range rootRefs {
					if val, _, err := txn.Read(objPtr); err != nil || txn.RestartNeeded() {
						return nil, err
					} else {
						if binary.BigEndian.Uint64(val) == magicNumber {
							anyChange = true
							changed = append(changed, true)
						} else {
							changed = append(changed, false)
						}
					}
				}
				if anyChange {
					return nil, nil
				}
				if !triggered {
					triggered = true
					startBarrier.Done()
				}
				return nil, txn.Retry()
			})
			if err != nil {
				if !triggered {
					startBarrier.Done()
				}
				return err
			}
			for idx, c := range changed {
				if c != changes[idx] {
					return fmt.Errorf("Expected to find object %v had changed, but instead %v changed", changes, changed)
				}
			}
			if !triggered {
				startBarrier.Done()
				return fmt.Errorf("Found magic number in the right place without triggering the writer!")
			}
			return nil
		}
	})

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, magicNumber)
	startBarrier.Wait()
	time.Sleep(250 * time.Millisecond)
	if rootRefs, err := conn.AwaitRootVersionChange(guidBuf, objCount); err != nil {
		th.MaybeFatal(err)
	} else {
		_, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
			for idx, change := range changes {
				if change {
					if err := txn.Write(rootRefs[idx], buf); err != nil || txn.RestartNeeded() {
						return nil, err
					}
				}
			}
			return nil, nil
		})
		th.MaybeFatal(err)
		go func() {
			endBarrier.Wait()
			close(errCh)
		}()
		th.MaybeFatal(<-errCh)
	}
}
