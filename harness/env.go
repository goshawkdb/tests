package harness

import (
	"flag"
	"os"
)

type TestHelperTxnResult uint8

const Abort TestHelperTxnResult = iota

func (self TestHelperTxnResult) Error() string {
	return "Abort"
}

type TestEnv map[ConfigKey]string

type ConfigKey string

const (
	GoshawkDB      ConfigKey = "GOSHAWKDB_BINARY"
	ClusterConfig            = "GOSHAWKDB_CLUSTER_CONFIG"
	ClusterHosts             = "GOSHAWKDB_CLUSTER_HOSTS"
	ClusterCert              = "GOSHAWKDB_CLUSTER_CERT"
	ClusterKeyPair           = "GOSHAWKDB_CLUSTER_KEYPAIR"
	ClientKeyPair            = "GOSHAWKDB_CLIENT_KEYPAIR"
	RootName                 = "GOSHAWKDB_ROOT_NAME"
	GoPath                   = "GOPATH"
)

func BuildTestEnv() TestEnv {
	envMap := extractFromEnv(
		GoshawkDB,
		ClusterConfig,
		ClusterHosts,
		ClusterCert,
		ClusterKeyPair,
		RootName,
		GoPath,
	)

	var binaryPath, certPath, configPath string
	flag.StringVar(&binaryPath, "goshawkdb", "", "`Path` to GoshawkDB binary.")
	flag.StringVar(&certPath, "cert", "", "`Path` to cluster certificate and key file.")
	flag.StringVar(&configPath, "config", "", "`Path` to configuration file.")
	flag.Parse()

	if len(binaryPath) != 0 {
		envMap[GoshawkDB] = binaryPath
	}
	if len(certPath) != 0 {
		envMap[ClusterCert] = certPath
	}
	if len(configPath) != 0 {
		envMap[ClusterConfig] = configPath
	}

	return envMap
}

func extractFromEnv(keys ...ConfigKey) map[ConfigKey]string {
	resultMap := make(map[ConfigKey]string, len(keys))
	for _, key := range keys {
		value := os.Getenv(string(key))
		if len(value) > 0 {
			resultMap[key] = value
		}
	}
	return resultMap
}

func (te TestEnv) EnsureEnv(key ConfigKey, value string) string {
	if v, found := te[key]; found {
		return v
	} else {
		te[key] = value
		return value
	}
}
