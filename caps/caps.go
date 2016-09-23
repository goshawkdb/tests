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
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with none caps
	createObjOffRoot(c1, client.Read, []byte("Hello World"))
	// c2 should be able to read it
	attemptRead(c2, client.Read, []byte("Hello World"))
	// but c2 shouldn't be able to write it
	attemptWrite(c2, client.Read, []byte("illegal"))
}

func writeOnly(th *tests.TestHelper) {
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with none caps
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
	conns := th.CreateConnections(2)
	c1, c2 := conns[0], conns[1]

	// c1 writes a ref to root with none caps
	createObjOffRoot(c1, client.ReadWrite, []byte("Hello World"))
	// c2 should be able to read it
	attemptRead(c2, client.ReadWrite, []byte("Hello World"))
	// and c2 should be able to write it
	attemptWrite(c2, client.ReadWrite, []byte("Goodbye World"))
	// and c1 should be able to read it.
	attemptRead(c1, client.ReadWrite, []byte("Goodbye World"))
}
