package main

import (
	"fmt"
	h "goshawkdb.io/tests/harness"
	ht "goshawkdb.io/tests/harness/topology"
	"log"
	"time"
)

type ClusterChange struct {
	Before *ht.PortsAndF
	After  *ht.PortsAndF
}

func (cc *ClusterChange) String() string {
	return fmt.Sprintf("ClusterChange from %v to %v", cc.Before, cc.After)
}

func AddToClusterScenarios() []*ClusterChange {
	return []*ClusterChange{
		// 1 to 2
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
		},

		// 1 to 3
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
		},

		// 1 to 4
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
		},

		// 1 to 5
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
		},

		// 2 to 3
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
		},

		// 2 to 4
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
		},

		// 2 to 5
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
		},

		// 3 to 3
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
		},

		// 3 to 4
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
		},

		// 3 to 5
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
		},

		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
		},

		// 4 to 5
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
		},

		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
		},

		// 5 to 5
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
		},
	}
}

func RemoveFromClusterScenarios() []*ClusterChange {
	return []*ClusterChange{
		// 2 to 1
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001}, F: 0},
		},

		// 3 to 1
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001}, F: 0},
		},
		// 3 to 2
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
		},

		// 4 to 1
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001}, F: 0},
		},
		// 4 to 2
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
		},
		// 4 to 3
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
		},

		// 5 to 1
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &ht.PortsAndF{Ports: []uint16{10001}, F: 0},
		},
		// 5 to 2
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
		},
		// 5 to 3
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
		},
	}
}

func ReplaceInClusterScenarios() []*ClusterChange {
	return []*ClusterChange{
		// 2 to 2
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10003}, F: 0},
		},

		// 3 to 3 (replace 1)
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10004}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10004}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10004}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10004}, F: 0},
		},

		// 3 to 3 (replace 2)
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10004, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10004, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10004, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10004, 10005}, F: 0},
		},

		// 4 to 4 (replace 1)
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10005}, F: 0},
		},

		// 4 to 4 (replace 2)
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10005, 10006}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10005, 10006}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10005, 10006}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10005, 10006}, F: 0},
		},

		// 4 to 4 (replace 3)
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10005, 10006, 10007}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10005, 10006, 10007}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10005, 10006, 10007}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10005, 10006, 10007}, F: 0},
		},

		// 5 to 5 (replace 1)
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10006}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10006}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10006}, F: 2},
		},

		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10006}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10006}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10006}, F: 2},
		},

		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10006}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10006}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10006}, F: 2},
		},

		// 5 to 5 (replace 2)
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10006, 10007}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10006, 10007}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10006, 10007}, F: 2},
		},

		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10006, 10007}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10006, 10007}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10006, 10007}, F: 2},
		},

		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10006, 10007}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10006, 10007}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10006, 10007}, F: 2},
		},

		// 5 to 5 (replace 3)
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10006, 10007, 10008}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10006, 10007, 10008}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10006, 10007, 10008}, F: 2},
		},

		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10006, 10007, 10008}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10006, 10007, 10008}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10006, 10007, 10008}, F: 2},
		},

		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10006, 10007, 10008}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10006, 10007, 10008}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10002, 10006, 10007, 10008}, F: 2},
		},

		// 5 to 5 (replace 4)
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10006, 10007, 10008, 10009}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10006, 10007, 10008, 10009}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10006, 10007, 10008, 10009}, F: 2},
		},

		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10006, 10007, 10008, 10009}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10006, 10007, 10008, 10009}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10006, 10007, 10008, 10009}, F: 2},
		},

		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10006, 10007, 10008, 10009}, F: 0},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10006, 10007, 10008, 10009}, F: 1},
		},
		&ClusterChange{
			Before: &ht.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &ht.PortsAndF{Ports: []uint16{10001, 10006, 10007, 10008, 10009}, F: 2},
		},
	}
}

func runScenarios(he *h.HarnessEnv, scenarios ...*ClusterChange) {
	start := time.Now()
	for idx, scenario := range scenarios {
		setup := h.NewSetup()
		prog := ht.TopologyChange(scenario.Before, scenario.After, setup)
		log.Printf("[%v/%v]: Starting scenario %v (program: %v).", idx+1, len(scenarios), scenario, prog)
		if err := he.Run(setup, h.Program(prog)); err == nil {
			log.Printf("Scenario %v completed successfully.", scenario)
		} else {
			log.Fatalf("Scenario %v errored: %v", scenario, err)
		}
	}
	log.Printf("All scenarios completed successfully in %v.", time.Now().Sub(start))
}

func main() {
	he := h.BuildHarnessEnv()
	runScenarios(he, AddToClusterScenarios()...)
	runScenarios(he, RemoveFromClusterScenarios()...)
	runScenarios(he, ReplaceInClusterScenarios()...)
}
