package retry

import (
	"encoding/binary"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/tests"
	"sync"
	"time"
)

// Test that one write wakes up many retriers
func SimpleRetry(th *tests.TestHelper) {
	retriers := 9
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	rootVsn, _ := conn.SetRootToZeroUInt64()
	magicNumber := uint64(42)

	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(retriers)
	endBarrier, errCh := th.InParallel(retriers, func(connIdx int, conn *tests.Connection) error {
		if err := conn.AwaitRootVersionChange(rootVsn); err != nil {
			return err
		}
		triggered := false
		res, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			rootObj, err := txn.GetRootObject()
			if err != nil {
				return nil, err
			}
			val, err := rootObj.Value()
			if err != nil {
				return nil, err
			}
			num := binary.BigEndian.Uint64(val)
			if num == 0 {
				if !triggered {
					triggered = true
					startBarrier.Done() // trigger the change in the other txn
				}
				return client.Retry, nil
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
	})

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, magicNumber)
	startBarrier.Wait()
	time.Sleep(250 * time.Millisecond)
	_, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		rootObj, err := txn.GetRootObject()
		if err != nil {
			return nil, err
		}
		return nil, rootObj.Set(buf)
	})
	th.MaybeFatal(err)

	go func() {
		endBarrier.Wait()
		close(errCh)
	}()
	th.MaybeFatal(<-errCh)
}

// Test that a retry on several objs gets restarted by one write.
func DisjointRetry(th *tests.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	rootVsn, _ := conn.SetRootToNZeroObjs(3)
	magicNumber := uint64(43)

	startBarrier := new(sync.WaitGroup)
	startBarrier.Add(1)

	changes := []bool{true, false, true}

	endBarrier, errCh := th.InParallel(1, func(connIdx int, conn *tests.Connection) error {
		if err := conn.AwaitRootVersionChange(rootVsn); err != nil {
			return err
		}
		triggered := false
		changed := []bool{}
		_, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			changed = changed[:0]
			rootObj, err := txn.GetRootObject()
			if err != nil {
				return nil, err
			}
			objs, err := rootObj.References()
			if err != nil {
				return nil, err
			}
			anyChange := false
			for _, obj := range objs {
				val, err := obj.Value()
				if err != nil {
					return nil, err
				}
				if binary.BigEndian.Uint64(val) == magicNumber {
					anyChange = true
					changed = append(changed, true)
				} else {
					changed = append(changed, false)
				}
			}
			if anyChange {
				return nil, nil
			}
			if !triggered {
				triggered = true
				startBarrier.Done()
			}
			return client.Retry, nil
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
	})

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, magicNumber)
	startBarrier.Wait()
	time.Sleep(250 * time.Millisecond)
	_, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		rootObj, err := txn.GetRootObject()
		if err != nil {
			return nil, err
		}
		refs, err := rootObj.References()
		if err != nil {
			return nil, err
		}
		for idx, change := range changes {
			if change {
				err := refs[idx].Set(buf)
				if err != nil {
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
