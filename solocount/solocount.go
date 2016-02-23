package solocount

import (
	"encoding/binary"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/tests"
)

// We have one client, and it counts from 0 to 1000
func SoloCount(th *tests.TestHelper) {
	th.CreateConnections(1)
	limit := uint64(1000)

	defer th.Shutdown()
	th.SetRootToZeroUInt64()
	encountered := make(map[uint64]bool)
	expected := uint64(0)
	buf := make([]byte, 8)
	for {
		res, _, err := th.RunTransaction(0, func(txn *client.Txn) (interface{}, error) {
			rootObj, err := txn.GetRootObject()
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
			binary.BigEndian.PutUint64(buf, cur)
			if err := rootObj.Set(buf); err != nil {
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
			th.Fatal("Failed to encounter", n)
		}
	}
}
