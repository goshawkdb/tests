package caps

import (
	"bytes"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/common"
	"goshawkdb.io/tests/harness"
)

func createObjOffRoot(c *harness.Connection, cap *common.Capability, value []byte) {
	_, err := c.Transact(func(txn *client.Transaction) (interface{}, error) {
		rootPtr := txn.Root(c.RootName)
		if rootPtr == nil {
			return nil, fmt.Errorf("No root object named '%s' found", c.RootName)
		} else {
			objPtr, err := txn.Create(value)
			if err != nil {
				return nil, err
			}
			return nil, txn.Write(*rootPtr, []byte{}, objPtr.GrantCapability(cap))
		}
	})
	if err != nil {
		c.Fatal(err)
	}
}

func attemptRead(c *harness.Connection, refsLen, refsIdx int, refCap, objCap *common.Capability, value []byte) {
	result, err := c.Transact(func(txn *client.Transaction) (interface{}, error) {
		rootPtr := txn.Root(c.RootName)
		if rootPtr == nil {
			return nil, fmt.Errorf("No root object named '%s' found", c.RootName)
		} else if _, refs, err := txn.Read(*rootPtr); err != nil || txn.RestartNeeded() {
			return nil, err
		} else if len(refs) != refsLen {
			return nil, fmt.Errorf("Expected root to have %d reference; got %d", refsLen, len(refs))
		} else {
			objPtr := refs[refsIdx]
			if cap := objPtr.RefCapability(); cap != refCap {
				return nil, fmt.Errorf("Expected %v reference capability; got %v", refCap, cap)
			}
			if cap, err := txn.ObjectCapability(objPtr); err != nil || txn.RestartNeeded() {
				return nil, err
			} else if cap != objCap {
				return nil, fmt.Errorf("Expected %v object capability; got %v", objCap, cap)
			}
			val, _, err := txn.Read(objPtr)
			if txn.RestartNeeded() {
				return nil, nil
			}
			if objCap.CanRead() {
				if err != nil {
					return nil, err
				} else {
					return val, nil
				}
			} else {
				if err == nil {
					return nil, fmt.Errorf("Expected to error on attempted read; got value %v", val)
				} else {
					return nil, nil
				}
			}
		}
	})
	if err != nil {
		c.Fatal(err)
	} else if objCap.CanRead() {
		if bites, ok := result.([]byte); !ok || !bytes.Equal(bites, value) {
			c.Fatal("error", "Unexpected read.", "read", bites, "expected", value)
		}
	}
}

func attemptWrite(c *harness.Connection, refsLen, refsIdx int, refCap, objCap *common.Capability, value []byte) {
	_, err := c.Transact(func(txn *client.Transaction) (interface{}, error) {
		rootPtr := txn.Root(c.RootName)
		if rootPtr == nil {
			return nil, fmt.Errorf("No root object named '%s' found", c.RootName)
		} else if _, refs, err := txn.Read(*rootPtr); err != nil || txn.RestartNeeded() {
			return nil, err
		} else if len(refs) != refsLen {
			return nil, fmt.Errorf("Expected root to have %d reference; got %d", refsLen, len(refs))
		} else {
			objPtr := refs[refsIdx]
			if cap := objPtr.RefCapability(); cap != refCap {
				return nil, fmt.Errorf("Expected %v reference capability; got %v", refCap, cap)
			}
			if cap, err := txn.ObjectCapability(objPtr); err != nil || txn.RestartNeeded() {
				return nil, err
			} else if cap != objCap {
				return nil, fmt.Errorf("Expected %v object capability; got %v", objCap, cap)
			}

			err := txn.Write(objPtr, value)
			if txn.RestartNeeded() {
				return nil, nil
			}
			if objCap.CanWrite() {
				return nil, err
			} else {
				if err == nil {
					return nil, fmt.Errorf("Expected to error on attempted write")
				} else {
					return nil, nil
				}
			}
		}
	})
	if err != nil {
		c.Fatal(err)
	}
}

func none(th *harness.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with none caps
	createObjOffRoot(c1, common.NoneCapability, []byte("Hello World"))
	// c2 shouldn't be able to read it
	attemptRead(c2, 1, 0, common.NoneCapability, common.NoneCapability, nil)
	// and c2 shouldn't be able to write it
	attemptWrite(c2, 1, 0, common.NoneCapability, common.NoneCapability, []byte("illegal"))
}

func readOnly(th *harness.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with read only caps
	createObjOffRoot(c1, common.ReadOnlyCapability, []byte("Hello World"))
	// c2 should be able to read it
	attemptRead(c2, 1, 0, common.ReadOnlyCapability, common.ReadOnlyCapability, []byte("Hello World"))
	// but c2 shouldn't be able to write it
	attemptWrite(c2, 1, 0, common.ReadOnlyCapability, common.ReadOnlyCapability, []byte("illegal"))
}

func writeOnly(th *harness.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with write only caps
	createObjOffRoot(c1, common.WriteOnlyCapability, []byte("Hello World"))
	// c2 shouldn't be able to read it
	attemptRead(c2, 1, 0, common.WriteOnlyCapability, common.WriteOnlyCapability, nil)
	// but c2 should be able to write it
	attemptWrite(c2, 1, 0, common.WriteOnlyCapability, common.WriteOnlyCapability, []byte("Goodbye World"))
	// and c1 should be able to read it, as it created it, even though
	// it'll only find a Write capability on the ref.
	attemptRead(c1, 1, 0, common.WriteOnlyCapability, common.ReadWriteCapability, []byte("Goodbye World"))
}

func readWrite(th *harness.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with read-write caps
	createObjOffRoot(c1, common.ReadWriteCapability, []byte("Hello World"))
	// c2 should be able to read it
	attemptRead(c2, 1, 0, common.ReadWriteCapability, common.ReadWriteCapability, []byte("Hello World"))
	// and c2 should be able to write it
	attemptWrite(c2, 1, 0, common.ReadWriteCapability, common.ReadWriteCapability, []byte("Goodbye World"))
	// and c1 should be able to read it.
	attemptRead(c1, 1, 0, common.ReadWriteCapability, common.ReadWriteCapability, []byte("Goodbye World"))
}

func fakeRead(th *harness.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with write-only caps
	createObjOffRoot(c1, common.WriteOnlyCapability, []byte("Hello World"))
	// c2 shouldn't be able to read it
	attemptRead(c2, 1, 0, common.WriteOnlyCapability, common.WriteOnlyCapability, nil)
	// and even if we're bad and fake the capability, we shouldn't be
	// able to read it. There is no point faking it locally only as the
	// server hasn't sent c2 the value. So the only hope is to fake it
	// locally and write it back into the root. Of course, the server
	// should reject the txn:
	_, err := c2.Transact(func(txn *client.Transaction) (interface{}, error) {
		rootPtr := txn.Root(c2.RootName)
		if rootPtr == nil {
			return nil, fmt.Errorf("No root object named '%s' found", c2.RootName)
		} else if _, refs, err := txn.Read(*rootPtr); err != nil || txn.RestartNeeded() {
			return nil, err
		} else if len(refs) != 1 {
			return nil, fmt.Errorf("Expected root to have 1 reference; got %v", len(refs))
		} else {
			objPtr := refs[0]
			return nil, txn.Write(*rootPtr, nil, objPtr.GrantCapability(common.ReadOnlyCapability))
		}
	})
	if err == nil {
		th.Fatal("Should have got an error when attempting to escalate capability")
	} else {
		th.Log("msg", "Correctly got error.", "error", err)
	}
}

func fakeWrite(th *harness.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with read-only caps
	createObjOffRoot(c1, common.ReadOnlyCapability, []byte("Hello World"))
	// c2 shouldn't be able to write it
	attemptWrite(c2, 1, 0, common.ReadOnlyCapability, common.ReadOnlyCapability, []byte("illegal"))
	// and even if we're bad and fake the capability, we shouldn't be
	// able to read it. There is no point faking it locally only as the
	// server hasn't sent c2 the value. So the only hope is to fake it
	// locally and write it back into the root. Of course, the server
	// should reject the txn:
	_, err := c2.Transact(func(txn *client.Transaction) (interface{}, error) {
		rootPtr := txn.Root(c2.RootName)
		if rootPtr == nil {
			return nil, fmt.Errorf("No root object named '%s' found", c2.RootName)
		} else if _, refs, err := txn.Read(*rootPtr); err != nil || txn.RestartNeeded() {
			return nil, err
		} else if len(refs) != 1 {
			return nil, fmt.Errorf("Expected root to have 1 reference; got %v", len(refs))
		} else {
			objPtr := refs[0]
			return nil, txn.Write(*rootPtr, nil, objPtr.GrantCapability(common.WriteOnlyCapability))
		}
	})
	if err == nil {
		th.Fatal("Should have got an error when attempting to escalate capability")
	} else {
		th.Log("msg", "Correctly got error.", "error", err)
	}
}

func capabilitiesCanGrowSingleTxn(th *harness.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// we want to construct the following graph
	// root --rw--> obj3 --rw--> obj2
	//     1\   0   1|r      0     0|
	//       \       v              /
	//        \-n-> obj1 <--w------/
	//
	// However, because we're creating this whole structure in a single
	// txn, c2 will get told about the whole txn in one go, and so
	// should immediately learn that obj1 is read-write.

	_, err := c1.Transact(func(txn *client.Transaction) (interface{}, error) {
		rootPtr := txn.Root(c1.RootName)
		if rootPtr == nil {
			return nil, fmt.Errorf("No root object named '%s' found", c1.RootName)
		} else {
			obj1Ptr, err := txn.Create([]byte("Hello World"))
			if err != nil || txn.RestartNeeded() {
				return nil, err
			}
			obj2Ptr, err := txn.Create(nil, obj1Ptr.GrantCapability(common.WriteOnlyCapability))
			if err != nil || txn.RestartNeeded() {
				return nil, err
			}
			obj3Ptr, err := txn.Create(nil, obj2Ptr, obj1Ptr.GrantCapability(common.ReadOnlyCapability))
			if err != nil || txn.RestartNeeded() {
				return nil, err
			}
			return nil, txn.Write(*rootPtr, nil, obj3Ptr, obj1Ptr.GrantCapability(common.NoneCapability))
		}
	})
	if err != nil {
		th.Fatal(err)
	}
	attemptRead(c2, 2, 1, common.NoneCapability, common.ReadWriteCapability, []byte("Hello World"))
	attemptWrite(c2, 2, 1, common.NoneCapability, common.ReadWriteCapability, []byte("Goodbye World"))
	attemptRead(c1, 2, 1, common.NoneCapability, common.ReadWriteCapability, []byte("Goodbye World"))
}

func capabilitiesCanGrowMultiTxn(th *harness.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// we want to construct the same graph as last time:
	// root --rw--> obj3 --rw--> obj2
	//     1\   0   1|r      0     0|
	//       \       v              /
	//        \-n-> obj1 <--w------/
	//
	// This time though we do it in multiple txns which means c2 will
	// actually have to read bits to finally discover its full
	// capabilities on obj1: The point is that when c2 only reaches
	// root, it should have no access to obj1.  After it's reached obj3
	// it should be able to read only obj1. After it's reached obj2, it
	// should have read-write access to obj1.

	// txn1: create all the objs, but only have root point to obj1.
	_, err := c1.Transact(func(txn *client.Transaction) (interface{}, error) {
		rootPtr := txn.Root(c1.RootName)
		if rootPtr == nil {
			return nil, fmt.Errorf("No root object named '%s' found", c1.RootName)
		} else {
			obj1Ptr, err := txn.Create([]byte("Hello World"))
			if err != nil || txn.RestartNeeded() {
				return nil, err
			}
			obj2Ptr, err := txn.Create(nil)
			if err != nil || txn.RestartNeeded() {
				return nil, err
			}
			obj3Ptr, err := txn.Create(nil, obj2Ptr)
			if err != nil || txn.RestartNeeded() {
				return nil, err
			}
			return nil, txn.Write(*rootPtr, nil, obj3Ptr, obj1Ptr.GrantCapability(common.NoneCapability))
		}
	})
	if err != nil {
		th.Fatal(err)
	}
	// txn2: add the read pointer from obj3 to obj1
	_, err = c1.Transact(func(txn *client.Transaction) (interface{}, error) {
		rootPtr := txn.Root(c1.RootName)
		if rootPtr == nil {
			return nil, fmt.Errorf("No root object named '%s' found", c1.RootName)
		} else if _, rootRefs, err := txn.Read(*rootPtr); err != nil || txn.RestartNeeded() {
			return nil, err
		} else {
			obj3Ptr := rootRefs[0]
			obj1Ptr := rootRefs[1]
			if _, obj3Refs, err := txn.Read(obj3Ptr); err != nil || txn.RestartNeeded() {
				return nil, err
			} else {
				return nil, txn.Write(obj3Ptr, nil, obj3Refs[0], obj1Ptr.GrantCapability(common.ReadOnlyCapability))
			}
		}
	})
	if err != nil {
		th.Fatal(err)
	}
	// txn3: add the write pointer from obj2 to obj1
	_, err = c1.Transact(func(txn *client.Transaction) (interface{}, error) {
		rootPtr := txn.Root(c1.RootName)
		if rootPtr == nil {
			return nil, fmt.Errorf("No root object named '%s' found", c1.RootName)
		} else if _, rootRefs, err := txn.Read(*rootPtr); err != nil || txn.RestartNeeded() {
			return nil, err
		} else {
			obj3Ptr := rootRefs[0]
			obj1Ptr := rootRefs[1]
			if _, obj3Refs, err := txn.Read(obj3Ptr); err != nil || txn.RestartNeeded() {
				return nil, err
			} else {
				obj2Ptr := obj3Refs[0]
				return nil, txn.Write(obj2Ptr, nil, obj1Ptr.GrantCapability(common.WriteOnlyCapability))
			}
		}
	})
	if err != nil {
		th.Fatal(err)
	}
	// initially, c2 should not be able to read obj1
	attemptRead(c2, 2, 1, common.NoneCapability, common.NoneCapability, nil)
	// but, if c2 first reads obj3, it should find it can read obj1
	attemptRead(c2, 2, 0, common.ReadWriteCapability, common.ReadWriteCapability, []byte{})
	attemptRead(c2, 2, 1, common.NoneCapability, common.ReadOnlyCapability, []byte("Hello World"))
	// finally, if c2 reads to obj2 then we should discover we can actually write obj1
	_, err = c2.Transact(func(txn *client.Transaction) (interface{}, error) {
		rootPtr := txn.Root(c2.RootName)
		if rootPtr == nil {
			return nil, fmt.Errorf("No root object named '%s' found", c2.RootName)
		} else if _, rootRefs, err := txn.Read(*rootPtr); err != nil || txn.RestartNeeded() {
			return nil, err
		} else {
			obj3Ptr := rootRefs[0]
			if _, obj3Refs, err := txn.Read(obj3Ptr); err != nil || txn.RestartNeeded() {
				return nil, err
			} else {
				obj2Ptr := obj3Refs[0]
				_, _, err := txn.Read(obj2Ptr)
				return nil, err
			}
		}
	})
	if err != nil {
		th.Fatal(err)
	}
	attemptWrite(c2, 2, 1, common.NoneCapability, common.ReadWriteCapability, []byte("Goodbye World"))
	attemptRead(c1, 2, 1, common.NoneCapability, common.ReadWriteCapability, []byte("Goodbye World"))
}
