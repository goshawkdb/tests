package caps

import (
	"bytes"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/tests"
)

func createObjOffRoot(c *tests.Connection, cap client.Capability, value []byte) {
	_, _, err := c.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		root, err := c.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		objRef, err := txn.CreateObject(value)
		if err != nil {
			return nil, err
		}
		return nil, root.Set([]byte{}, objRef.GrantCapability(cap))
	})
	if err != nil {
		c.Fatal(err)
	}
}

func attemptRead(c *tests.Connection, refsLen, refsIdx int, refCap, objCap client.Capability, value []byte) {
	canRead := objCap == client.Read || objCap == client.ReadWrite
	result, _, err := c.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		root, err := c.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		refs, err := root.References()
		if err != nil {
			return nil, err
		}
		if len(refs) != refsLen {
			return nil, fmt.Errorf("Expected root to have 1 reference; got %v", len(refs))
		}
		obj := refs[refsIdx]
		if cap := obj.RefCapability(); cap != refCap {
			return nil, fmt.Errorf("Expected %v reference capability; got %v", refCap, cap)
		}
		if cap := obj.ObjectCapability(); cap != objCap {
			return nil, fmt.Errorf("Expected %v object capability; got %v", objCap, cap)
		}
		if canRead {
			return obj.Value()
		} else {
			objValue, err := obj.Value()
			if err == nil {
				return nil, fmt.Errorf("Expected to error on attempted read; got value %v", objValue)
			} else {
				return nil, nil
			}
		}
	})
	if err != nil {
		c.Fatal(err)
	} else if canRead {
		if bites, ok := result.([]byte); !ok || !bytes.Equal(bites, value) {
			c.Fatalf("Expected to read value %v but read %v", value, bites)
		}
	}
}

func attemptWrite(c *tests.Connection, refsLen, refsIdx int, refCap, objCap client.Capability, value []byte) {
	canWrite := objCap == client.Write || objCap == client.ReadWrite
	_, _, err := c.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		root, err := c.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		refs, err := root.References()
		if err != nil {
			return nil, err
		}
		if len(refs) != refsLen {
			return nil, fmt.Errorf("Expected root to have 1 reference; got %v", len(refs))
		}
		obj := refs[refsIdx]
		if cap := obj.RefCapability(); cap != refCap {
			return nil, fmt.Errorf("Expected %v reference capability; got %v", refCap, cap)
		}
		if cap := obj.ObjectCapability(); cap != objCap {
			return nil, fmt.Errorf("Expected %v object capability; got %v", objCap, cap)
		}
		if canWrite {
			return nil, obj.Set(value)
		} else {
			err := obj.Set(value)
			if err == nil {
				return nil, fmt.Errorf("Expected to error on attempted write")
			} else {
				return nil, nil
			}
		}
	})
	if err != nil {
		c.Fatal(err)
	}
}

func none(th *tests.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with none caps
	createObjOffRoot(c1, client.None, []byte("Hello World"))
	// c2 shouldn't be able to read it
	attemptRead(c2, 1, 0, client.None, client.None, nil)
	// and c2 shouldn't be able to write it
	attemptWrite(c2, 1, 0, client.None, client.None, []byte("illegal"))
}

func readOnly(th *tests.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with read only caps
	createObjOffRoot(c1, client.Read, []byte("Hello World"))
	// c2 should be able to read it
	attemptRead(c2, 1, 0, client.Read, client.Read, []byte("Hello World"))
	// but c2 shouldn't be able to write it
	attemptWrite(c2, 1, 0, client.Read, client.Read, []byte("illegal"))
}

func writeOnly(th *tests.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with write only caps
	createObjOffRoot(c1, client.Write, []byte("Hello World"))
	// c2 shouldn't be able to read it
	attemptRead(c2, 1, 0, client.Write, client.Write, nil)
	// but c2 should be able to write it
	attemptWrite(c2, 1, 0, client.Write, client.Write, []byte("Goodbye World"))
	// and c1 should be able to read it, as it created it, even though
	// it'll only find a Write capability on the ref.
	attemptRead(c1, 1, 0, client.Write, client.ReadWrite, []byte("Goodbye World"))
}

func readWrite(th *tests.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with read-write caps
	createObjOffRoot(c1, client.ReadWrite, []byte("Hello World"))
	// c2 should be able to read it
	attemptRead(c2, 1, 0, client.ReadWrite, client.ReadWrite, []byte("Hello World"))
	// and c2 should be able to write it
	attemptWrite(c2, 1, 0, client.ReadWrite, client.ReadWrite, []byte("Goodbye World"))
	// and c1 should be able to read it.
	attemptRead(c1, 1, 0, client.ReadWrite, client.ReadWrite, []byte("Goodbye World"))
}

func fakeRead(th *tests.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with write-only caps
	createObjOffRoot(c1, client.Write, []byte("Hello World"))
	// c2 shouldn't be able to read it
	attemptRead(c2, 1, 0, client.Write, client.Write, nil)
	// and even if we're bad and fake the capability, we shouldn't be
	// able to read it. There is no point faking it locally only as the
	// server hasn't sent c2 the value. So the only hope is to fake it
	// locally and write it back into the root. Of course, the server
	// should reject the txn:
	_, _, err := c2.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		root, err := c2.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		refs, err := root.References()
		if err != nil {
			return nil, err
		}
		if len(refs) != 1 {
			return nil, fmt.Errorf("Expected root to have 1 reference; got %v", len(refs))
		}
		obj := refs[0]
		return nil, root.Set([]byte{}, obj.GrantCapability(client.Read))
	})
	if err == nil {
		th.Fatal("Should have got an error when attempting to escalate capability")
	} else {
		th.Log("Correctly got error:", err)
	}
}

func fakeWrite(th *tests.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with read-only caps
	createObjOffRoot(c1, client.Read, []byte("Hello World"))
	// c2 shouldn't be able to write it
	attemptWrite(c2, 1, 0, client.Read, client.Read, []byte("illegal"))
	// and even if we're bad and fake the capability, we shouldn't be
	// able to read it. There is no point faking it locally only as the
	// server hasn't sent c2 the value. So the only hope is to fake it
	// locally and write it back into the root. Of course, the server
	// should reject the txn:
	_, _, err := c2.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		root, err := c2.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		refs, err := root.References()
		if err != nil {
			return nil, err
		}
		if len(refs) != 1 {
			return nil, fmt.Errorf("Expected root to have 1 reference; got %v", len(refs))
		}
		obj := refs[0]
		return nil, root.Set([]byte{}, obj.GrantCapability(client.Write))
	})
	if err == nil {
		th.Fatal("Should have got an error when attempting to escalate capability")
	} else {
		th.Log("Correctly got error:", err)
	}
}

func capabilitiesCanGrowSingleTxn(th *tests.TestHelper) {
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

	_, _, err := c1.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		root, err := c1.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		obj1, err := txn.CreateObject([]byte("Hello World"))
		if err != nil {
			return nil, err
		}
		obj2, err := txn.CreateObject([]byte{}, obj1.GrantCapability(client.Write))
		if err != nil {
			return nil, err
		}
		obj3, err := txn.CreateObject([]byte{}, obj2, obj1.GrantCapability(client.Read))
		if err != nil {
			return nil, err
		}
		return nil, root.Set([]byte{}, obj3, obj1.GrantCapability(client.None))
	})
	if err != nil {
		th.Fatal(err)
	}
	attemptRead(c2, 2, 1, client.None, client.ReadWrite, []byte("Hello World"))
	attemptWrite(c2, 2, 1, client.None, client.ReadWrite, []byte("Goodbye World"))
	attemptRead(c1, 2, 1, client.None, client.ReadWrite, []byte("Goodbye World"))
}

func capabilitiesCanGrowMultiTxn(th *tests.TestHelper) {
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
	_, _, err := c1.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		root, err := c1.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		obj1, err := txn.CreateObject([]byte("Hello World"))
		if err != nil {
			return nil, err
		}
		obj2, err := txn.CreateObject([]byte{})
		if err != nil {
			return nil, err
		}
		obj3, err := txn.CreateObject([]byte{}, obj2)
		if err != nil {
			return nil, err
		}
		return nil, root.Set([]byte{}, obj3, obj1.GrantCapability(client.None))
	})
	if err != nil {
		th.Fatal(err)
	}
	// txn2: add the read pointer from obj3 to obj1
	_, _, err = c1.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		root, err := c1.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		rootRefs, err := root.References()
		if err != nil {
			return nil, err
		}
		obj3 := rootRefs[0]
		obj1 := rootRefs[1]
		obj3Refs, err := obj3.References()
		if err != nil {
			return nil, err
		}
		return nil, obj3.Set([]byte{}, obj3Refs[0], obj1.GrantCapability(client.Read))
	})
	if err != nil {
		th.Fatal(err)
	}
	// txn3: add the write pointer from obj2 to obj1
	_, _, err = c1.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		root, err := c1.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		rootRefs, err := root.References()
		if err != nil {
			return nil, err
		}
		obj3 := rootRefs[0]
		obj1 := rootRefs[1]
		obj3Refs, err := obj3.References()
		if err != nil {
			return nil, err
		}
		obj2 := obj3Refs[0]
		return nil, obj2.Set([]byte{}, obj1.GrantCapability(client.Write))
	})
	if err != nil {
		th.Fatal(err)
	}
	// initially, c2 should not be able to read obj1
	attemptRead(c2, 2, 1, client.None, client.None, nil)
	// but, if c2 first reads obj3, it should find it can read obj1
	attemptRead(c2, 2, 0, client.ReadWrite, client.ReadWrite, []byte{})
	attemptRead(c2, 2, 1, client.None, client.Read, []byte("Hello World"))
	// finally, if c2 reads to obj2 then we should discover we can actually write obj1
	_, _, err = c2.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		root, err := c2.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		rootRefs, err := root.References()
		if err != nil {
			return nil, err
		}
		obj3 := rootRefs[0]
		obj3Refs, err := obj3.References()
		if err != nil {
			return nil, err
		}
		obj2 := obj3Refs[0]
		return obj2.Value()
	})
	if err != nil {
		th.Fatal(err)
	}
	attemptWrite(c2, 2, 1, client.None, client.ReadWrite, []byte("Goodbye World"))
	attemptRead(c1, 2, 1, client.None, client.ReadWrite, []byte("Goodbye World"))
}
