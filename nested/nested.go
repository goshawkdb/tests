package nested

import (
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/tests"
	"time"
)

func NestedRead(th *tests.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	// Just read the root var from several nested txns
	result, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		rootObj0, err := conn.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		result, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			rootObj1, err := conn.GetRootObject(txn)
			if err != nil {
				return nil, err
			}
			if rootObj0 != rootObj1 {
				return nil, fmt.Errorf("Should have pointer equality between the same object in nested txns")
			}
			result, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
				rootObj2, err := conn.GetRootObject(txn)
				if err != nil {
					return nil, err
				}
				if rootObj0 != rootObj2 {
					return nil, fmt.Errorf("Should have pointer equality between the same object in nested txns")
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

func NestedWrite(th *tests.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	// A write made in a parent should be visible in the child
	_, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		rootObj0, err := conn.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		err = rootObj0.Set([]byte("outer"))
		if err != nil {
			return nil, err
		}
		_, _, err = conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			rootObj1, err := conn.GetRootObject(txn)
			if err != nil {
				return nil, err
			}
			val, err := rootObj1.Value()
			if err != nil {
				return nil, err
			}
			if str := string(val); str != "outer" {
				return nil, fmt.Errorf("Expected value to be 'outer', but it was '%s'", str)
			}
			err = rootObj1.Set([]byte("mid"))
			if err != nil {
				return nil, err
			}
			_, _, err = conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
				rootObj2, err := conn.GetRootObject(txn)
				if err != nil {
					return nil, err
				}
				val, err := rootObj2.Value()
				if err != nil {
					return nil, err
				}
				if str := string(val); str != "mid" {
					return nil, fmt.Errorf("Expected value to be 'mid', but it was '%s'", str)
				}
				err = rootObj2.Set([]byte("inner"))
				if err != nil {
					return nil, err
				}
				return nil, nil
			})
			if err != nil {
				return nil, err
			}
			val, err = rootObj1.Value()
			if err != nil {
				return nil, err
			}
			if str := string(val); str != "inner" {
				return nil, fmt.Errorf("On return, expected value to be 'inner', but it was '%s'", str)
			}
			return nil, nil
		})
		if err != nil {
			return nil, err
		}
		val, err := rootObj0.Value()
		if err != nil {
			return nil, err
		}
		if str := string(val); str != "inner" {
			return nil, fmt.Errorf("On return, expected value to be 'inner', but it was '%s'", str)
		}
		return nil, nil
	})
	th.MaybeFatal(err)
}

func NestedInnerAbort(th *tests.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	// A write made in a child which is aborted should not be seen in
	// the parent
	_, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		rootObj0, err := conn.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		err = rootObj0.Set([]byte("outer"))
		if err != nil {
			return nil, err
		}
		_, _, err = conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			rootObj1, err := conn.GetRootObject(txn)
			if err != nil {
				return nil, err
			}
			val, err := rootObj1.Value()
			if err != nil {
				return nil, err
			}
			if str := string(val); str != "outer" {
				return nil, fmt.Errorf("Expected value to be 'outer', but it was '%s'", str)
			}
			if err = rootObj1.Set([]byte("mid")); err != nil {
				return nil, err
			}
			_, _, err = conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
				rootObj2, err := conn.GetRootObject(txn)
				if err != nil {
					return nil, err
				}
				val, err := rootObj2.Value()
				if err != nil {
					return nil, err
				}
				if str := string(val); str != "mid" {
					return nil, fmt.Errorf("Expected value to be 'mid', but it was '%s'", str)
				}
				if err = rootObj2.Set([]byte("inner")); err != nil {
					return nil, err
				}
				return nil, tests.Abort
			})
			if err != tests.Abort {
				return nil, fmt.Errorf("Expected to get tests.Abort returned, but actually got %#v", err)
			}
			val, err = rootObj1.Value()
			if err != nil {
				return nil, err
			}
			if str := string(val); str != "mid" {
				return nil, fmt.Errorf("On return, expected value to be 'mid', but it was '%s'", str)
			}
			return nil, nil
		})
		if err != nil {
			return nil, err
		}
		val, err := rootObj0.Value()
		if err != nil {
			return nil, err
		}
		if str := string(val); str != "mid" {
			return nil, fmt.Errorf("On return, expected value to be 'mid', but it was '%s'", str)
		}
		return nil, nil
	})
	th.MaybeFatal(err)
}

func NestedInnerRetry(th *tests.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	rootVsn, _ := conn.SetRootToZeroUInt64()
	signal := make(chan struct{}, 1)

	endBarrier, errCh := th.InParallel(1, func(connIdx int, conn *tests.Connection) error {
		if err := conn.AwaitRootVersionChange(rootVsn); err != nil {
			return err
		}
		<-signal
		time.Sleep(250 * time.Millisecond)
		_, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			rootObj, err := conn.GetRootObject(txn)
			if err != nil {
				return nil, err
			}
			return nil, rootObj.Set([]byte("Magic"))
		})
		return err
	})

	// If a child txn issues a retry, the parent must restart.
	conn.AwaitRootVersionChange(rootVsn)
	_, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		rootObj0, err := conn.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		val, err := rootObj0.Value()
		if err != nil {
			return nil, err
		}
		if str := string(val); str == "Magic" {
			return nil, nil
		} else {
			_, _, err = conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
				// Even though we've not read root in this inner txn,
				// retry should still work!
				close(signal)
				return client.Retry, nil
			})
			return nil, err
		}
	})
	th.MaybeFatal(err)
	go func() {
		endBarrier.Wait()
		close(errCh)
	}()
	th.MaybeFatal(<-errCh)
}

func NestedInnerCreate(th *tests.TestHelper) {
	conn := th.CreateConnections(1)[0]
	defer th.Shutdown()

	// A create made in a child, returned to the parent should both be
	// directly usable and writable.
	_, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		rootObj, err := conn.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		var obj *client.Object
		_, _, err = conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			_, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
				obj, err = txn.CreateObject([]byte("Hello"))
				if err != nil {
					return nil, err
				}
				return nil, rootObj.Set(nil, obj)
			})
			if err != nil {
				return nil, err
			}
			refs, err := rootObj.References()
			if err != nil {
				return nil, err
			}
			if refs[0] != obj {
				return nil, fmt.Errorf("On return, expected to find obj in references of root")
			}
			val, err := obj.Value()
			if err != nil {
				return nil, err
			}
			if str := string(val); str != "Hello" {
				return nil, fmt.Errorf("On return, expected to find obj has value 'Hello', but actually has '%s'", str)
			}
			return nil, obj.Set([]byte("Goodbye"))
		})
		return nil, err
	})
	th.MaybeFatal(err)

	result, _, err := conn.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		rootObj, err := conn.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		refs, err := rootObj.References()
		if err != nil {
			return nil, err
		}
		val, err := refs[0].Value()
		if err != nil {
			return nil, err
		}
		return string(val), nil
	})
	th.MaybeFatal(err)
	if str := result.(string); str != "Goodbye" {
		th.Fatal("Expected to find obj hads value 'Goodbye', but actually has", str)
	}
}
