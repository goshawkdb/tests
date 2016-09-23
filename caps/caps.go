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

func attemptRead(c *tests.Connection, cap client.Capability, value []byte) {
	canRead := cap == client.Read || cap == client.ReadWrite
	result, _, err := c.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		root, err := c.GetRootObject(txn)
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
		if objCap := obj.Capability(); objCap != cap {
			return nil, fmt.Errorf("Expected %v capability; got %v", cap, objCap)
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

func attemptWrite(c *tests.Connection, cap client.Capability, value []byte) {
	canWrite := cap == client.Write || cap == client.ReadWrite
	_, _, err := c.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		root, err := c.GetRootObject(txn)
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
		if objCap := obj.Capability(); objCap != cap {
			return nil, fmt.Errorf("Expected %v capability; got %v", cap, objCap)
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
	attemptRead(c2, client.None, nil)
	// and c2 shouldn't be able to write it
	attemptWrite(c2, client.None, []byte("illegal"))
}

func readOnly(th *tests.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with read only caps
	createObjOffRoot(c1, client.Read, []byte("Hello World"))
	// c2 should be able to read it
	attemptRead(c2, client.Read, []byte("Hello World"))
	// but c2 shouldn't be able to write it
	attemptWrite(c2, client.Read, []byte("illegal"))
}

func writeOnly(th *tests.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with write only caps
	createObjOffRoot(c1, client.Write, []byte("Hello World"))
	// c2 shouldn't be able to read it
	attemptRead(c2, client.Write, nil)
	// but c2 should be able to write it
	attemptWrite(c2, client.Write, []byte("Goodbye World"))
	// and c1 should be able to read it, as it created it, even though
	// it'll only find a Write capability on the ref.
	attemptRead(c1, client.Write, []byte("Goodbye World"))
}

func readWrite(th *tests.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with read-write caps
	createObjOffRoot(c1, client.ReadWrite, []byte("Hello World"))
	// c2 should be able to read it
	attemptRead(c2, client.ReadWrite, []byte("Hello World"))
	// and c2 should be able to write it
	attemptWrite(c2, client.ReadWrite, []byte("Goodbye World"))
	// and c1 should be able to read it.
	attemptRead(c1, client.ReadWrite, []byte("Goodbye World"))
}

func fakeRead(th *tests.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with write-only caps
	createObjOffRoot(c1, client.Write, []byte("Hello World"))
	// c2 shouldn't be able to read it
	attemptRead(c2, client.Write, nil)
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
	attemptWrite(c2, client.Read, []byte("illegal"))
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

func capabilitiesCanGrow(th *tests.TestHelper) {
	defer th.Shutdown()
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// we want to construct the following graph
	// root --rw--> obj3 --rw--> obj2
	//     1\   0   1|r      0     0|
	//       \       v              /
	//        \-n-> obj1 <--w------/
	//
	// The point is that when c2 only reaches root, it should have no
	// access to obj1.  After it's reached obj3 it should be able to
	// read only obj1. After it's reached obj2, it should have
	// read-write access to obj1.

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
		obj3, err := txn.CreateObject([]byte{})
		if err != nil {
			return nil, err
		}
		if err = obj2.Set([]byte{}, obj1.GrantCapability(client.Write)); err != nil {
			return nil, err
		}
		if err = obj3.Set([]byte{}, obj2, obj1.GrantCapability(client.Read)); err != nil {
			return nil, err
		}
		return nil, root.Set([]byte{}, obj3, obj1.GrantCapability(client.None))
	})
	if err != nil {
		th.Fatal(err)
	}

	// just read root first
	_, _, err = c2.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		root, err := c2.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		refs, err := root.References()
		if err != nil {
			return nil, err
		}
		if len(refs) != 2 {
			return nil, fmt.Errorf("Expected root to have 2 reference; got %v", len(refs))
		}
		obj1 := refs[1]
		if objCap := obj1.Capability(); objCap != client.None {
			return nil, fmt.Errorf("Expected None capability; got %v", objCap)
		}
		objValue, err := obj1.Value()
		if err == nil {
			return nil, fmt.Errorf("Expected to error on attempted read; got value %v", objValue)
		} else {
			return nil, nil
		}
	})
	if err != nil {
		th.Fatal(err)
	}

	// now read root and obj3
	result, _, err := c2.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		root, err := c2.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		refs, err := root.References()
		if err != nil {
			return nil, err
		}
		if len(refs) != 2 {
			return nil, fmt.Errorf("Expected root to have 2 reference; got %v", len(refs))
		}
		obj3 := refs[0]
		_, err = obj3.References()
		if err != nil {
			return nil, err
		}
		obj1 := refs[1]
		if objCap := obj1.Capability(); objCap != client.None {
			return nil, fmt.Errorf("Expected None capability; got %v", objCap)
		}
		return obj1.Value()
	})
	// despite the None, we should have been able to read it because of
	// the existance of the reference from obj3 to obj1, even though we
	// went via the reference from root.
	if bites, ok := result.([]byte); !ok || !bytes.Equal(bites, []byte("Hello World")) {
		th.Fatalf("Expected to read value Hello World but read %v", bites)
	}
	// ...but we still shouldn't be able to write it
	_, _, err = c2.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		root, err := c2.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		refs, err := root.References()
		if err != nil {
			return nil, err
		}
		if len(refs) != 2 {
			return nil, fmt.Errorf("Expected root to have 2 reference; got %v", len(refs))
		}
		obj1 := refs[1]
		if objCap := obj1.Capability(); objCap != client.None {
			return nil, fmt.Errorf("Expected None capability; got %v", objCap)
		}
		err = obj1.Set([]byte("illegal"))
		if err == nil {
			return nil, fmt.Errorf("Expected to error on attempted write")
		} else {
			return nil, nil
		}
	})
	if err != nil {
		th.Fatal(err)
	}

	// finally, if we read root and obj3 and obj2 then we should be able to read and write obj1
	result, _, err = c2.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		root, err := c2.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		refs, err := root.References()
		if err != nil {
			return nil, err
		}
		if len(refs) != 2 {
			return nil, fmt.Errorf("Expected root to have 2 reference; got %v", len(refs))
		}
		obj3 := refs[0]
		obj3Refs, err := obj3.References()
		if err != nil {
			return nil, err
		}
		obj2 := obj3Refs[0]
		obj2Refs, err := obj2.References()
		if err != nil {
			return nil, err
		}
		th.Log(obj2Refs)
		obj1 := refs[1]
		if objCap := obj1.Capability(); objCap != client.None {
			return nil, fmt.Errorf("Expected None capability; got %v", objCap)
		}
		return obj1.Value()
	})
	if bites, ok := result.([]byte); !ok || !bytes.Equal(bites, []byte("Hello World")) {
		th.Fatalf("Expected to read value Hello World but read %v", bites)
	}
	// ...and now, we should be able to write it
	_, _, err = c2.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		root, err := c2.GetRootObject(txn)
		if err != nil {
			return nil, err
		}
		refs, err := root.References()
		if err != nil {
			return nil, err
		}
		if len(refs) != 2 {
			return nil, fmt.Errorf("Expected root to have 2 reference; got %v", len(refs))
		}
		obj1 := refs[1]
		if objCap := obj1.Capability(); objCap != client.None {
			return nil, fmt.Errorf("Expected None capability; got %v", objCap)
		}
		return nil, obj1.Set([]byte("Goodbye World"))
	})
	if err != nil {
		th.Fatal(err)
	}
}
