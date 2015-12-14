package nested

import (
	"goshawkdb.io/client"
	"goshawkdb.io/tests"
)

func NestedRead(th *tests.TestHelper) {
	th.CreateConnections(1)
	defer th.Shutdown()

	// Just read the root var from several nested txns
	result, _ := th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
		rootObj0, err := txn.GetRootObject()
		if err != nil {
			return nil, err
		}
		result, _ := th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
			rootObj1, err := txn.GetRootObject()
			if err != nil {
				return nil, err
			}
			if rootObj0 != rootObj1 {
				th.Fatal("Should have pointer equality between the same object in nested txns")
			}
			result, _ := th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
				rootObj2, err := txn.GetRootObject()
				if err != nil {
					return nil, err
				}
				if rootObj0 != rootObj2 {
					th.Fatal("Should have pointer equality between the same object in nested txns")
				}
				return 42, nil
			})
			if result.(int) != 42 {
				th.Fatal("Expecting to get 42 back from nested txn but got", result)
			}
			return 43, nil
		})
		if result.(int) != 43 {
			th.Fatal("Expecting to get 43 back from nested txn but got", result)
		}
		return 44, nil
	})
	if result.(int) != 44 {
		th.Fatal("Expecting to get 44 back from outer txn but got", result)
	}
}

func NestedWrite(th *tests.TestHelper) {
	th.CreateConnections(1)
	defer th.Shutdown()

	// A write made in a parent should be visible in the child
	th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
		rootObj0, err := txn.GetRootObject()
		if err != nil {
			return nil, err
		}
		err = rootObj0.Set([]byte("outer"))
		if err != nil {
			return nil, err
		}
		th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
			rootObj1, err := txn.GetRootObject()
			if err != nil {
				return nil, err
			}
			val, err := rootObj1.Value()
			if err != nil {
				return nil, err
			}
			if str := string(val); str != "outer" {
				th.Fatal("Expected value to be 'outer', but it was", str)
			}
			err = rootObj1.Set([]byte("mid"))
			if err != nil {
				return nil, err
			}
			th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
				rootObj2, err := txn.GetRootObject()
				if err != nil {
					return nil, err
				}
				val, err := rootObj2.Value()
				if err != nil {
					return nil, err
				}
				if str := string(val); str != "mid" {
					th.Fatal("Expected value to be 'mid', but it was", str)
				}
				err = rootObj2.Set([]byte("inner"))
				if err != nil {
					return nil, err
				}
				return nil, nil
			})
			val, err = rootObj1.Value()
			if err != nil {
				return nil, err
			}
			if str := string(val); str != "inner" {
				th.Fatal("On return, expected value to be 'inner', but it was", str)
			}
			return nil, nil
		})
		val, err := rootObj0.Value()
		if err != nil {
			return nil, err
		}
		if str := string(val); str != "inner" {
			th.Fatal("On return, expected value to be 'inner', but it was", str)
		}
		return nil, nil
	})
}

func NestedInnerAbort(th *tests.TestHelper) {
	th.CreateConnections(1)
	defer th.Shutdown()

	// A write made in a child which is aborted should not be seen in
	// the parent
	th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
		rootObj0, err := txn.GetRootObject()
		if err != nil {
			return nil, err
		}
		err = rootObj0.Set([]byte("outer"))
		if err != nil {
			return nil, err
		}
		th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
			rootObj1, err := txn.GetRootObject()
			if err != nil {
				return nil, err
			}
			val, err := rootObj1.Value()
			if err != nil {
				return nil, err
			}
			if str := string(val); str != "outer" {
				th.Fatal("Expected value to be 'outer', but it was", str)
			}
			err = rootObj1.Set([]byte("mid"))
			if err != nil {
				return nil, err
			}
			th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
				rootObj2, err := txn.GetRootObject()
				if err != nil {
					return nil, err
				}
				val, err := rootObj2.Value()
				if err != nil {
					return nil, err
				}
				if str := string(val); str != "mid" {
					th.Fatal("Expected value to be 'mid', but it was", str)
				}
				err = rootObj2.Set([]byte("inner"))
				if err != nil {
					return nil, err
				}
				return nil, tests.Abort
			})
			val, err = rootObj1.Value()
			if err != nil {
				return nil, err
			}
			if str := string(val); str != "mid" {
				th.Fatal("On return, expected value to be 'mid', but it was", str)
			}
			return nil, nil
		})
		val, err := rootObj0.Value()
		if err != nil {
			return nil, err
		}
		if str := string(val); str != "mid" {
			th.Fatal("On return, expected value to be 'mid', but it was", str)
		}
		return nil, nil
	})
}

func NestedInnerRetry(th *tests.TestHelper) {
	th.CreateConnections(2)
	defer th.Shutdown()

	rootVsn := th.SetRootToZeroUInt64()
	signal := make(chan struct{})

	go func() {
		th.AwaitRootVersionChange(1, rootVsn)
		<-signal
		th.RunTransaction(1, func(txn *client.Txn) (interface{}, error) {
			rootObj, err := txn.GetRootObject()
			if err != nil {
				return nil, err
			}
			return nil, rootObj.Set([]byte("Magic"))
		})
	}()

	// If a child txn issues a retry, the parent must restart.
	th.AwaitRootVersionChange(0, rootVsn)
	th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
		rootObj0, err := txn.GetRootObject()
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
			th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
				// Even though we've not read root in this inner txn,
				// retry should still work!
				close(signal)
				return client.Retry, nil
			})
			return nil, nil
		}
	})
}

func NestedInnerCreate(th *tests.TestHelper) {
	th.CreateConnections(1)
	defer th.Shutdown()

	// A create made in a child, returned to the parent should both be
	// directly usable and writable.
	th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
		var obj *client.Object
		rootObj, err := txn.GetRootObject()
		if err != nil {
			return nil, err
		}
		th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
			th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
				obj, err = txn.CreateObject([]byte("Hello"))
				if err != nil {
					return nil, err
				}
				return nil, rootObj.Set([]byte{}, obj)
			})
			refs, err := rootObj.References()
			if err != nil {
				return nil, err
			}
			if refs[0] != obj {
				th.Fatal("On return, expected to find obj in references of root")
			}
			val, err := obj.Value()
			if err != nil {
				return nil, err
			}
			if str := string(val); str != "Hello" {
				th.Fatal("On return, expected to find obj has value 'Hello', but actually has", str)
			}
			return nil, obj.Set([]byte("Goodbye"))
		})
		return nil, nil
	})

	result, _ := th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
		rootObj, err := txn.GetRootObject()
		if err != nil {
			return nil, err
		}
		refs, err := rootObj.References()
		if err != nil {
			return nil, err
		}
		obj := refs[0]
		val, err := obj.Value()
		if err != nil {
			return nil, err
		}
		return string(val), nil
	})
	if str := result.(string); str != "Goodbye" {
		th.Fatal("Expected to find obj hads value 'Goodbye', but actually has", str)
	}
}
