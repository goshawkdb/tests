package solocount

import (
	"encoding/binary"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/tests/harness"
)

// We have one client, and it counts from 0 to 1000
func SoloCount(th *harness.TestHelper) {
	c := th.CreateConnections(1)[0]
	limit := uint64(1000)

	defer th.Shutdown()
	c.SetRootToZeroUInt64()
	encountered := make(map[uint64]bool)
	expected := uint64(0)
	for {
		res, err := c.RunTransaction(func(txn *client.Transaction) (interface{}, error) {
			rootPtr := c.GetRootObject(txn)
			if rootPtr == nil {
				return nil, fmt.Errorf("No root object named '%s' found", c.RootName)
			} else {
				val, _, err := txn.Read(*rootPtr)
				if err != nil {
					return nil, err
				}
				cur := binary.BigEndian.Uint64(val)
				encountered[cur] = true
				if cur != expected {
					return nil, fmt.Errorf("Expected to find %v but found %v", expected, cur)
				}
				cur++
				binary.BigEndian.PutUint64(val, cur)
				if err := txn.Write(*rootPtr, val); err != nil {
					return nil, err
				}
				return cur, nil
			}
		})
		th.MaybeFatal(err)
		expected++
		if res.(uint64) == limit {
			break
		}
	}
	for n := uint64(0); n < limit; n++ {
		if !encountered[n] {
			th.Fatal("error", fmt.Errorf("Failed to encounter: %d", n))
		}
	}
}
