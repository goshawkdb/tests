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
	limit := uint64(10000)

	defer th.Shutdown()
	guidBuf, _ := c.SetRootToNZeroObjs(1)
	objPtrs, _ := c.AwaitRootVersionChange(guidBuf, 1)
	objPtr := objPtrs[0]

	encountered := make(map[uint64]bool)
	expected := uint64(0)
	for {
		res, err := c.Transact(func(txn *client.Transaction) (interface{}, error) {
			if val, _, err := txn.Read(objPtr); err != nil || txn.RestartNeeded() {
				return nil, err
			} else {
				cur := binary.BigEndian.Uint64(val)
				encountered[cur] = true
				if cur != expected {
					return nil, fmt.Errorf("Expected to find %v but found %v", expected, cur)
				}
				cur++
				binary.BigEndian.PutUint64(val, cur)
				if err := txn.Write(objPtr, val); err != nil {
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
