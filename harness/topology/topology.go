package topology

import (
	"fmt"
	config "goshawkdb.io/server/configuration"
	h "goshawkdb.io/tests/harness"
	"syscall"
	"time"
)

type PortsAndF struct {
	Ports []uint16
	F     uint8
}

func TopologyChange(before, after *PortsAndF) []h.Instruction {
	setup := h.NewSetup()

	beforePorts := make(map[uint16]bool, len(before.Ports))
	afterPorts := make(map[uint16]bool, len(after.Ports))
	beforeHosts := make([]string, 0, len(before.Ports))

	rmsSurvived := make([]*h.RM, 0, len(before.Ports))
	rmsRemoved := make([]*h.RM, 0, len(before.Ports))
	rmsAdded := make([]*h.RM, 0, len(after.Ports))

	for _, port := range before.Ports {
		beforePorts[port] = true
		beforeHosts = append(beforeHosts, fmt.Sprintf("localhost:%d", port))
	}

	for _, port := range after.Ports {
		afterPorts[port] = true
	}

	baseConfig := &config.ConfigurationJSON{
		Hosts:      beforeHosts,
		F:          before.F,
		MaxRMCount: 5,
		NoSync:     true,
		ClientCertificateFingerprints: map[string]map[string]*config.CapabilityJSON{
			"6c5b2b2efc0ef77248af64cda16445fdfe936c9f5484711d77c9d67bba5dfe44": {
				"test": {
					Read:  true,
					Write: true,
				},
				"system:config": {
					Read: true,
				},
			},
		},
	}

	configPath := setup.Dir.Join("config.json")

	configProviderBefore := h.NewMutableConfigProvider(baseConfig)
	configProviderAfter := configProviderBefore.Clone()

	configProviderAfter.ChangeF(after.F)

	for idx, port := range before.Ports {
		if afterPorts[port] {
			rm := setup.NewRM(fmt.Sprintf("survived%d", idx), port, nil, configPath)
			rmsSurvived = append(rmsSurvived, rm)
		} else {
			rm := setup.NewRM(fmt.Sprintf("removed%d", idx), port, nil, configPath)
			rmsRemoved = append(rmsRemoved, rm)
			configProviderAfter.RemoveHost(fmt.Sprintf("localhost:%d", port))
		}
	}

	for idx, port := range after.Ports {
		if !beforePorts[port] {
			rm := setup.NewRM(fmt.Sprintf("added%d", idx), port, nil, configPath)
			rmsAdded = append(rmsAdded, rm)
			configProviderAfter.AddHost(fmt.Sprintf("localhost:%d", port))
		}
	}

	instrs := []h.Instruction{
		setup,
		configProviderBefore.Writer(configPath),
	}
	rmsInitial := append(rmsSurvived, rmsRemoved...)
	for _, rm := range rmsInitial {
		instrs = append(instrs, rm.Start())
	}
	instrs = append(instrs, []h.Instruction{
		setup.Sleep(time.Duration(5+len(rmsInitial)) * time.Second),
		configProviderBefore.NewConfigComparer(beforeHosts...),
		configProviderAfter.Writer(configPath),
	}...)

	if len(rmsAdded) == 0 {
		instrs = append(instrs, rmsInitial[0].Signal(syscall.SIGHUP))
	} else {
		for _, rm := range rmsAdded {
			instrs = append(instrs, rm.Start())
		}
	}

	instrs = append(instrs, setup.Sleep(time.Duration(15+len(rmsInitial))*time.Second))
	for _, rm := range rmsRemoved {
		instrs = append(instrs, rm.Wait())
	}

	rmsEventual := append(rmsSurvived, rmsAdded...)
	for _, rm := range rmsEventual {
		instrs = append(instrs, configProviderAfter.NewConfigComparer(fmt.Sprintf("localhost:%d", rm.Port)))
	}
	for _, rm := range rmsEventual {
		instrs = append(instrs, rm.Terminate())
	}
	for _, rm := range rmsEventual {
		instrs = append(instrs, rm.Wait())
	}

	return instrs
}
