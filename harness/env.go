package harness

import (
	"flag"
	"io/ioutil"
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
	ClusterConfig  ConfigKey = "GOSHAWKDB_CLUSTER_CONFIG"
	ClusterHosts   ConfigKey = "GOSHAWKDB_CLUSTER_HOSTS"
	ClusterCert    ConfigKey = "GOSHAWKDB_CLUSTER_CERT"
	ClusterKeyPair ConfigKey = "GOSHAWKDB_CLUSTER_KEYPAIR"
	ClientKeyPair  ConfigKey = "GOSHAWKDB_CLIENT_KEYPAIR"
	RootName       ConfigKey = "GOSHAWKDB_ROOT_NAME"
	GoPath         ConfigKey = "GOPATH"
)

var environment = buildTestEnv()

func GetTestEnv() TestEnv {
	return environment.Clone()
}

func buildTestEnv() TestEnv {
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

func (te TestEnv) Clone() TestEnv {
	result := make(map[ConfigKey]string, len(te))
	for k, v := range te {
		result[k] = v
	}
	return result
}

func (ck ConfigKey) LoadFromEnv(env TestEnv) (bool, []byte, error) {
	if path, found := env[ck]; found {
		bites, err := ioutil.ReadFile(path)
		return true, bites, err
	} else {
		return false, nil, nil
	}
}
