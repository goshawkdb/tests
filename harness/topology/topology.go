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

func TopologyChange(before, after *PortsAndF, setup *h.Setup) []h.Instruction {
	beforePorts := make(map[uint16]bool, len(before.Ports))
	afterPorts := make(map[uint16]bool, len(after.Ports))

	beforeHosts := make([]string, 0, len(before.Ports))
	afterHosts := make([]string, 0, len(before.Ports))

	rmsSurvived := make([]*h.RM, 0, len(before.Ports)+len(after.Ports))
	rmsRemoved := make([]*h.RM, 0, len(before.Ports))
	rmsAdded := make([]*h.RM, 0, len(after.Ports))

	for _, port := range before.Ports {
		beforePorts[port] = true
		beforeHosts = append(beforeHosts, localhost(port))
	}

	for _, port := range after.Ports {
		afterPorts[port] = true
		afterHosts = append(afterHosts, localhost(port))
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

	for _, port := range before.Ports {
		if afterPorts[port] {
			rm := setup.NewRM(fmt.Sprintf("survived%d", len(rmsSurvived)), port, nil, configPath)
			rmsSurvived = append(rmsSurvived, rm)
		} else {
			rm := setup.NewRM(fmt.Sprintf("removed%d", len(rmsRemoved)), port, nil, configPath)
			rmsRemoved = append(rmsRemoved, rm)
			configProviderAfter.RemoveHost(localhost(port))
		}
	}

	for _, port := range after.Ports {
		if !beforePorts[port] {
			rm := setup.NewRM(fmt.Sprintf("added%d", len(rmsAdded)), port, nil, configPath)
			rmsAdded = append(rmsAdded, rm)
			configProviderAfter.AddHost(localhost(port))
		}
	}

	instrs := []h.Instruction{
		setup,
		configProviderBefore.Writer(configPath),
	}

	rmsBefore := append(rmsSurvived, rmsRemoved...)
	for _, rm := range rmsBefore {
		instrs = append(instrs, rm.Start())
	}

	instrs = append(instrs, []h.Instruction{
		setup.Sleep(time.Duration(5+len(rmsBefore)) * time.Second),
		configProviderBefore.NewConfigComparer(beforeHosts...),
		configProviderAfter.Writer(configPath),
	}...)

	if len(rmsAdded) == 0 {
		instrs = append(instrs, rmsBefore[0].Signal(syscall.SIGHUP))
	} else {
		for _, rm := range rmsAdded {
			instrs = append(instrs, rm.Start())
		}
	}

	rmsAfter := append(rmsSurvived, rmsAdded...)
	instrs = append(instrs, setup.Sleep(time.Duration(15+len(rmsBefore)+len(rmsAfter))*time.Second))
	for _, rm := range rmsRemoved {
		instrs = append(instrs, rm.Wait())
	}

	instrs = append(instrs, configProviderAfter.NewConfigComparer(afterHosts...))

	for _, rm := range rmsAfter {
		instrs = append(instrs, rm.Terminate())
	}
	for _, rm := range rmsAfter {
		instrs = append(instrs, rm.Wait())
	}

	return instrs
}

func localhost(port uint16) string {
	return fmt.Sprintf("localhost:%d", port)
}
