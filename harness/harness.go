package harness

import (
	"flag"
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
		if err := setup.SetGoshawkDBBinary(binaryPath); err != nil {
			return err
		}
		delete(envMap, "GOSHAWKDB_BINARY")
	} else if path, found := envMap["GOSHAWKDB_BINARY"]; found {
		if err := setup.SetGoshawkDBBinary(path); err != nil {
			return err
		}
	}

	if len(certPath) > 0 {
		if err := setup.SetGoshawkDBCertFile(certPath); err != nil {
			return err
		}
		delete(envMap, "GOSHAWKDB_CLUSTER_CERT")
	} else if path, found := envMap["GOSHAWKDB_CLUSTER_CERT"]; found {
		if err := setup.SetGoshawkDBCertFile(path); err != nil {
			return err
		}
	}

	if len(configPath) > 0 {
		if err := setup.SetGoshawkDBConfigFile(configPath); err != nil {
			return err
		}
		delete(envMap, "GOSHAWKDB_CLUSTER_CONFIG")
	} else if path, found := envMap["GOSHAWKDB_CLUSTER_CONFIG"]; found {
		if err := setup.SetGoshawkDBConfigFile(path); err != nil {
			return err
		}
	}

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
