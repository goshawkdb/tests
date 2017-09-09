package nested

import (
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/tests/harness"
	"sync"
	"time"
)

func NestedRead(th *harness.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	// Just read the root var from several nested txns
	result, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
		rootPtr0, _ := txn.Root(conn.RootName)
		result, err := txn.Transact(func(txn *client.Transaction) (interface{}, error) {
			rootPtr1, _ := txn.Root(conn.RootName)
			if !rootPtr0.SameReferent(rootPtr1) {
				return nil, fmt.Errorf("Should have pointers to the same object in nested txns")
			}
			result, err := txn.Transact(func(txn *client.Transaction) (interface{}, error) {
				rootPtr2, _ := txn.Root(conn.RootName)
				if !rootPtr0.SameReferent(rootPtr2) {
					return nil, fmt.Errorf("Should have pointers to the same object in nested txns")
				}
				return 42, nil
			})
			if err != nil {
				return nil, err
			}
			if result.(int) != 42 {
				return nil, fmt.Errorf("Expecting to get 42 back from nested txn but got %d", result)
			}
			return 43, nil
		})
		if err != nil {
			return nil, err
		}
		if result.(int) != 43 {
			return nil, fmt.Errorf("Expecting to get 43 back from nested txn but got %d", result)
		}
		return 44, nil
	})
	th.MaybeFatal(err)
	if result.(int) != 44 {
		th.Fatal("Expecting to get 44 back from outer txn but got", result)
	}
}

func NestedWrite(th *harness.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	// A write made in a parent should be visible in the child
	_, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
		rootPtr0, _ := txn.Root(conn.RootName)
		if err := txn.Write(rootPtr0, []byte("from outer")); err != nil || txn.RestartNeeded() {
			return nil, err
		}
		_, err := txn.Transact(func(txn *client.Transaction) (interface{}, error) {
			rootPtr1, _ := txn.Root(conn.RootName)
			if val, _, err := txn.Read(rootPtr1); err != nil || txn.RestartNeeded() {
				return nil, err
			} else if str := string(val); str != "from outer" {
				return nil, fmt.Errorf("Expected value to be 'from outer', but it was '%s'", str)
			} else if err = txn.Write(rootPtr1, []byte("from mid")); err != nil || txn.RestartNeeded() {
				return nil, err
			}
			_, err := txn.Transact(func(txn *client.Transaction) (interface{}, error) {
				rootPtr2, _ := txn.Root(conn.RootName)
				if val, _, err := txn.Read(rootPtr2); err != nil || txn.RestartNeeded() {
					return nil, err
				} else if str := string(val); str != "from mid" {
					return nil, fmt.Errorf("Expected value to be 'from mid', but it was '%s'", str)
				} else {
					return nil, txn.Write(rootPtr2, []byte("from inner"))
				}
			})
			if err != nil {
				return nil, err
			} else if val, _, err := txn.Read(rootPtr1); err != nil || txn.RestartNeeded() {
				return nil, err
			} else if str := string(val); str != "from inner" {
				return nil, fmt.Errorf("On return, expected value to be 'from inner', but it was '%s'", str)
			} else {
				return nil, nil
			}
		})
		if err != nil {
			return nil, err
		} else if val, _, err := txn.Read(rootPtr0); err != nil {
			return nil, err
		} else if str := string(val); str != "from inner" {
			return nil, fmt.Errorf("On return, expected value to be 'from inner', but it was '%s'", str)
		} else {
			return nil, nil
		}
	})
	th.MaybeFatal(err)
}

func NestedInnerAbort(th *harness.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	// A write made in a child which is aborted should not be seen in
	// the parent
	_, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
		rootPtr0, _ := txn.Root(conn.RootName)
		if err := txn.Write(rootPtr0, []byte("from outer")); err != nil {
			return nil, err
		}
		_, err := txn.Transact(func(txn *client.Transaction) (interface{}, error) {
			rootPtr1, _ := txn.Root(conn.RootName)
			if val, _, err := txn.Read(rootPtr1); err != nil || txn.RestartNeeded() {
				return nil, err
			} else if str := string(val); str != "from outer" {
				return nil, fmt.Errorf("Expected value to be 'from outer', but it was '%s'", str)
			} else if err = txn.Write(rootPtr1, []byte("from mid")); err != nil || txn.RestartNeeded() {
				return nil, err
			}
			_, err := txn.Transact(func(txn *client.Transaction) (interface{}, error) {
				rootPtr2, _ := txn.Root(conn.RootName)
				if val, _, err := txn.Read(rootPtr2); err != nil || txn.RestartNeeded() {
					return nil, err
				} else if str := string(val); str != "from mid" {
					return nil, fmt.Errorf("Expected value to be 'from mid', but it was '%s'", str)
				} else if err = txn.Write(rootPtr2, []byte("from inner")); err != nil || txn.RestartNeeded() {
					return nil, err
				} else {
					return nil, txn.Abort()
				}
			})
			if err != nil {
				return nil, err
			}
			if val, _, err := txn.Read(rootPtr1); err != nil || txn.RestartNeeded() {
				return nil, err
			} else if str := string(val); str != "from mid" {
				return nil, fmt.Errorf("On return, expected value to be 'from mid', but it was '%s'", str)
			} else {
				return nil, nil
			}
		})
		if err != nil {
			return nil, err
		}
		if val, _, err := txn.Read(rootPtr0); err != nil || txn.RestartNeeded() {
			return nil, err
		} else if str := string(val); str != "from mid" {
			return nil, fmt.Errorf("On return, expected value to be 'from mid', but it was '%s'", str)
		} else {
			return nil, nil
		}
	})
	th.MaybeFatal(err)
}

func NestedInnerRetry(th *harness.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	guidBuf, err := conn.SetRootToNZeroObjs(1)
	th.MaybeFatal(err)

	var o1 sync.Once
	var o2 sync.Once
	b1 := new(sync.WaitGroup) // for delaying start of 2nd txn
	b2 := new(sync.WaitGroup) // for signalling back to the first txn to make the change
	b1.Add(1)
	b2.Add(1)

	endBarrier, errCh := th.InParallel(1, func(connIdx int, conn *harness.Connection) error {
		if rootRefs, err := conn.AwaitRootVersionChange(guidBuf, 1); err != nil {
			return err
		} else {
			o1.Do(b1.Done)
			b2.Wait()
			time.Sleep(250 * time.Millisecond)
			_, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
				objPtr := rootRefs[0]
				return nil, txn.Write(objPtr, []byte("Magic"))
			})
			return err
		}
	})

	b1.Wait()

	// If a child txn issues a retry, the parent may restart if the parent
	rootRefs, err := conn.AwaitRootVersionChange(guidBuf, 1)
	th.MaybeFatal(err)
	_, err = conn.Transact(func(txn *client.Transaction) (interface{}, error) {
		objPtr := rootRefs[0]
		if val, _, err := txn.Read(objPtr); err != nil || txn.RestartNeeded() {
			return nil, err
		} else if str := string(val); str == "Magic" {
			return nil, nil
		} else {
			return txn.Transact(func(txn *client.Transaction) (interface{}, error) {
				// Even though we've not read root in this inner txn,
				// retry should still work!
				o2.Do(b2.Done)
				return nil, txn.Retry()
			})
		}
	})
	th.MaybeFatal(err)
	go func() {
		endBarrier.Wait()
		close(errCh)
	}()
	th.MaybeFatal(<-errCh)
}

func NestedInnerCreate(th *harness.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	// A create made in a child, returned to the parent should both be
	// directly usable and writable.
	_, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
		rootPtr, _ := txn.Root(conn.RootName)
		var ptr client.RefCap
		_, err := txn.Transact(func(txn *client.Transaction) (interface{}, error) {
			_, err := txn.Transact(func(txn *client.Transaction) (interface{}, error) {
				var err error
				if ptr, err = txn.Create([]byte("Hello")); err != nil || txn.RestartNeeded() {
					return nil, err
				} else {
					return nil, txn.Write(rootPtr, nil, ptr)
				}
			})
			if err != nil {
				return nil, err
			}
			if _, rootRefs, err := txn.Read(rootPtr); err != nil || txn.RestartNeeded() {
				return nil, err
			} else if !rootRefs[0].SameReferent(ptr) {
				return nil, fmt.Errorf("On return, expected to find obj in references of root")
			} else if val, _, err := txn.Read(ptr); err != nil || txn.RestartNeeded() {
				return nil, err
			} else if str := string(val); str != "Hello" {
				return nil, fmt.Errorf("On return, expected to find value 'Hello', but found '%s'", str)
			} else {
				return nil, txn.Write(ptr, []byte("Goodbye"))
			}
		})
		return nil, err
	})
	th.MaybeFatal(err)

	result, err := conn.Transact(func(txn *client.Transaction) (interface{}, error) {
		rootPtr, _ := txn.Root(conn.RootName)
		if _, rootRefs, err := txn.Read(rootPtr); err != nil || txn.RestartNeeded() {
			return nil, err
		} else if val, _, err := txn.Read(rootRefs[0]); err != nil || txn.RestartNeeded() {
			return nil, err
		} else {
			return string(val), nil
		}
	})
	th.MaybeFatal(err)
	if str := result.(string); str != "Goodbye" {
		th.Fatal("Expected to find obj hads value 'Goodbye', but actually has", str)
	}
}
