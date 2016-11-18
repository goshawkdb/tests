package harness

import (
	"flag"
	"fmt"
	"os"
)

func Run(setup *Setup, prog Instruction) error {
	envMap := extractFromEnv(
		"GOSHAWKDB_BINARY",
		"GOSHAWKDB_CLUSTER_CONFIG",
		"GOSHAWKDB_CLUSTER_HOSTS",
		"GOSHAWKDB_CLUSTER_CERT",
		"GOSHAWKDB_CLUSTER_KEYPAIR",
		"GOSHAWKDB_ROOT_NAME",
		"GOPATH")

	var binaryPath, certPath, configPath string
	flag.StringVar(&binaryPath, "goshawkdb", "", "`Path` to GoshawkDB binary.")
	flag.StringVar(&certPath, "cert", "", "`Path` to cluster certificate and key file.")
	flag.StringVar(&configPath, "config", "", "`Path` to configuration file.")
	flag.Parse()

	if len(binaryPath) > 0 {
		if err := setup.GosBin.SetPath(binaryPath, true); err != nil {
			return err
		}
		fmt.Println(setup.GosBin.Path())
		delete(envMap, "GOSHAWKDB_BINARY")
	} else if path, found := envMap["GOSHAWKDB_BINARY"]; found {
		if err := setup.GosBin.SetPath(path, true); err != nil {
			return err
		}
	}

	if len(certPath) > 0 {
		if err := setup.GosCert.SetPath(certPath, false); err != nil {
			return err
		}
		delete(envMap, "GOSHAWKDB_CLUSTER_CERT")
	} else if path, found := envMap["GOSHAWKDB_CLUSTER_CERT"]; found {
		if err := setup.GosCert.SetPath(path, false); err != nil {
			return err
		}
	}

	if len(configPath) > 0 {
		if err := setup.GosConfig.SetPath(configPath, false); err != nil {
			return err
		}
		delete(envMap, "GOSHAWKDB_CLUSTER_CONFIG")
	} else if path, found := envMap["GOSHAWKDB_CLUSTER_CONFIG"]; found {
		if err := setup.GosConfig.SetPath(path, false); err != nil {
			return err
		}
	}

	setup.SetEnv(envMap)

	l := setup.NewLogger()
	return prog.Exec(l)
}

func extractFromEnv(keys ...string) map[string]string {
	resultMap := make(map[string]string, len(keys))
	for _, key := range keys {
		value := os.Getenv(key)
		if len(value) > 0 {
			resultMap[key] = value
		}
	}
	return resultMap
}
