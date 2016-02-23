package retry

import (
	"encoding/binary"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/common"
	"goshawkdb.io/tests"
	"sync"
)

// Test that one write wakes up many retriers
func SimpleRetry(th *tests.TestHelper) {
	retriers := 9
	th.CreateConnections(retriers + 1)
	defer th.Shutdown()

	rootVsn, err := th.SetRootToZeroUInt64()
	th.MaybeFatal(err)
	magicNumber := uint64(42)

	startBarrier, endBarrier := new(sync.WaitGroup), new(sync.WaitGroup)
	startBarrier.Add(retriers)
	endBarrier.Add(retriers)
	errCh := make(chan error, retriers)

	for i := 0; i < retriers; i++ {
		connIdx := i + 1
		go func() {
			defer endBarrier.Done()
			triggered := false
			res, _, err := th.RunTransaction(connIdx, func(txn *client.Txn) (interface{}, error) {
				rootObj, err := txn.GetRootObject()
				if err != nil {
					return nil, err
				}
				vsn, err := rootObj.Version()
				if err != nil {
					return nil, err
				}
				if !triggered && rootVsn.Compare(vsn) == common.EQ {
					return client.Retry, nil
				}
				val, err := rootObj.Value()
				if err != nil {
					return nil, err
				}
				num := binary.BigEndian.Uint64(val)
				if num == 0 {
					triggered = true
					startBarrier.Done() // trigger the change in the other txn
					return client.Retry, nil
				} else {
					return num, nil
				}
			})
			if err != nil {
				errCh <- err
				if !triggered {
					startBarrier.Done()
				}
				return
			}
			num := res.(uint64)
			if num != magicNumber {
				th.Fatalf("%v Expected %v, got %v", connIdx, magicNumber, num)
			}
		}()
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, magicNumber)
	startBarrier.Wait()

	_, _, err = th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
		rootObj, err := txn.GetRootObject()
		if err != nil {
			return nil, err
		}
		err = rootObj.Set(buf)
		if err != nil {
			return nil, err
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

// Test that a retry on several objs gets restarted by one write.
func DisjointRetry(th *tests.TestHelper) {
	th.CreateConnections(2)
	defer th.Shutdown()

	rootVsn, err := th.SetRootToNZeroObjs(3)
	th.MaybeFatal(err)
	magicNumber := uint64(43)

	startBarrier, endBarrier := new(sync.WaitGroup), new(sync.WaitGroup)
	startBarrier.Add(1)
	endBarrier.Add(1)
	errCh := make(chan error, 2)

	changes := []bool{true, false, true}

	go func() {
		defer endBarrier.Done()
		triggered := false
		changed := []bool{}
		_, _, err := th.RunTransaction(1, func(txn *client.Txn) (interface{}, error) {
			changed = changed[:0]
			rootObj, err := txn.GetRootObject()
			if err != nil {
				return nil, err
			}
			vsn, err := rootObj.Version()
			if err != nil {
				return nil, err
			}
			if !triggered && rootVsn.Compare(vsn) == common.EQ {
				return client.Retry, nil
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
			errCh <- err
			if !triggered {
				startBarrier.Done()
			}
			return
		}
		for idx, c := range changed {
			if c != changes[idx] {
				errCh <- fmt.Errorf("Expected to find object %v had changed, but instead %v changed", changes, changed)
				return
			}
		}
		if !triggered {
			errCh <- fmt.Errorf("Found magic number in the right place without triggering the writer!")
			return
		}
	}()

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, magicNumber)
	startBarrier.Wait()
	_, _, err = th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
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
