package solocount

import (
	"encoding/binary"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/tests"
)

// We have one client, and it counts from 0 to 1000
func SoloCount(th *tests.TestHelper) {
	c := th.CreateConnections(1)[0]
	limit := uint64(1000)

	defer th.Shutdown()
	c.SetRootToZeroUInt64()
	encountered := make(map[uint64]bool)
	expected := uint64(0)
	for {
		res, _, err := c.RunTransaction(func(txn *client.Txn) (interface{}, error) {
			rootObj, err := c.GetRootObject(txn)
			if err != nil {
				return nil, err
			}
			rootVal, err := rootObj.Value()
			if err != nil {
				return nil, err
			}
			cur := binary.BigEndian.Uint64(rootVal)
			encountered[cur] = true
			if cur != expected {
				return nil, fmt.Errorf("Expected to find %v but found %v", expected, cur)
			}
			cur++
			binary.BigEndian.PutUint64(rootVal, cur)
			if err := rootObj.Set(rootVal); err != nil {
				return nil, err
			}
			return cur, nil
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
