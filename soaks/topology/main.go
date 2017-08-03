package main

import (
	"fmt"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/tests/harness/interpreter"
	"goshawkdb.io/tests/harness/interpreter/topology"
	"time"
)

type ClusterChange struct {
	Before *topology.PortsAndF
	After  *topology.PortsAndF
}

func (cc *ClusterChange) String() string {
	return fmt.Sprintf("ClusterChange from %v to %v", cc.Before, cc.After)
}

func AddToClusterScenarios() []*ClusterChange {
	return []*ClusterChange{
		// 1 to 2
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
		},

		// 1 to 3
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
		},

		// 1 to 4
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
		},

		// 1 to 5
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
		},

		// 2 to 3
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
		},

		// 2 to 4
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
		},

		// 2 to 5
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
		},

		// 3 to 3
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
		},

		// 3 to 4
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
		},

		// 3 to 5
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
		},

		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
		},

		// 4 to 5
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
		},

		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
		},

		// 5 to 5
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
		},
	}
}

func RemoveFromClusterScenarios() []*ClusterChange {
	return []*ClusterChange{
		// 2 to 1
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001}, F: 0},
		},

		// 3 to 1
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001}, F: 0},
		},
		// 3 to 2
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
		},

		// 4 to 1
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001}, F: 0},
		},
		// 4 to 2
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
		},
		// 4 to 3
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
		},

		// 5 to 1
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &topology.PortsAndF{Ports: []uint16{10001}, F: 0},
		},
		// 5 to 2
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
		},
		// 5 to 3
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
		},
	}
}

func ReplaceInClusterScenarios() []*ClusterChange {
	return []*ClusterChange{
		// 2 to 2
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10003}, F: 0},
		},

		// 3 to 3 (replace 1)
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10004}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10004}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10004}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10004}, F: 0},
		},

		// 3 to 3 (replace 2)
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10004, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10004, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10004, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10004, 10005}, F: 0},
		},

		// 4 to 4 (replace 1)
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10005}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10005}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10005}, F: 0},
		},

		// 4 to 4 (replace 2)
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10005, 10006}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10005, 10006}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10005, 10006}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10005, 10006}, F: 0},
		},

		// 4 to 4 (replace 3)
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10005, 10006, 10007}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10005, 10006, 10007}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10005, 10006, 10007}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10005, 10006, 10007}, F: 0},
		},

		// 5 to 5 (replace 1)
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10006}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10006}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10006}, F: 2},
		},

		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10006}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10006}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10006}, F: 2},
		},

		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10006}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10006}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10006}, F: 2},
		},

		// 5 to 5 (replace 2)
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10006, 10007}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10006, 10007}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10006, 10007}, F: 2},
		},

		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10006, 10007}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10006, 10007}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10006, 10007}, F: 2},
		},

		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10006, 10007}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10006, 10007}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10006, 10007}, F: 2},
		},

		// 5 to 5 (replace 3)
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10006, 10007, 10008}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10006, 10007, 10008}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10006, 10007, 10008}, F: 2},
		},

		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10006, 10007, 10008}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10006, 10007, 10008}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10006, 10007, 10008}, F: 2},
		},

		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10006, 10007, 10008}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10006, 10007, 10008}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10002, 10006, 10007, 10008}, F: 2},
		},

		// 5 to 5 (replace 4)
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10006, 10007, 10008, 10009}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10006, 10007, 10008, 10009}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 0},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10006, 10007, 10008, 10009}, F: 2},
		},

		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10006, 10007, 10008, 10009}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10006, 10007, 10008, 10009}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 1},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10006, 10007, 10008, 10009}, F: 2},
		},

		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10006, 10007, 10008, 10009}, F: 0},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10006, 10007, 10008, 10009}, F: 1},
		},
		&ClusterChange{
			Before: &topology.PortsAndF{Ports: []uint16{10001, 10002, 10003, 10004, 10005}, F: 2},
			After:  &topology.PortsAndF{Ports: []uint16{10001, 10006, 10007, 10008, 10009}, F: 2},
		},
	}
}

func runScenarios(ie *interpreter.InterpreterEnv, scenarios ...*ClusterChange) {
	start := time.Now()
	for idx, scenario := range scenarios {
		setup := interpreter.NewSetup()
		prog := topology.TopologyChange(scenario.Before, scenario.After, setup)
		l := log.With(ie.Logger, "scenario", idx, "of", len(scenarios))
		l.Log("program", fmt.Sprint(prog))
		ie.MaybeExit(ie.Run(setup, interpreter.Program(prog)))
		l.Log("msg", "completed")
	}
	ie.Log("msg", "All scenarios completed successfully", "duration", time.Now().Sub(start))
}

func main() {
	ie := interpreter.NewInterpreterEnv()
	for {
		runScenarios(ie, AddToClusterScenarios()...)
		runScenarios(ie, RemoveFromClusterScenarios()...)
		runScenarios(ie, ReplaceInClusterScenarios()...)
	}
}
