package main

import (
	config "goshawkdb.io/server/configuration"
	h "goshawkdb.io/tests/harness"
	"log"
	"time"
)

func main() {
	setup := h.NewSetup()

	config := &config.ConfigurationJSON{
		Hosts:      []string{"localhost:10001"},
		F:          0,
		MaxRMCount: 5,
		NoSync:     true,
		ClientCertificateFingerprints: map[string]map[string]*config.CapabilityJSON{
			"6c5b2b2efc0ef77248af64cda16445fdfe936c9f5484711d77c9d67bba5dfe44": {
				"test": {
					Read:  true,
					Write: true,
				},
			},
		},
	}
	configProvider := h.NewConfigProvider(config)
	configProvider2 := configProvider.Clone()
	configProvider2.AddHost("localhost:10002")
	configProvider2.AddHost("localhost:10003")

	configPath := setup.Dir.Join("config.json")

	rm1 := setup.NewRM("one", 10001, nil, configPath)
	rm2 := setup.NewRM("two", 10002, nil, configPath)
	rm3 := setup.NewRM("three", 10003, nil, configPath)

	prog := h.Program([]h.Instruction{
		setup,
		configProvider.Writer(configPath),
		rm1.Start(),
		setup.Sleep(5 * time.Second),
		configProvider2.Writer(configPath),
		rm2.Start(),
		rm3.Start(),
		setup.Sleep(15 * time.Second),
		rm1.Terminate(),
		rm2.Terminate(),
		rm3.Terminate(),
		rm1.Wait(),
		rm2.Wait(),
		rm3.Wait(),
	})
	log.Println(h.Run(setup, prog))

}
