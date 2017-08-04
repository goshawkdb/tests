package compare

import (
	"encoding/json"
	"fmt"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/client"
	"goshawkdb.io/server"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/tests/harness"
)

func CompareConfigs(host string, provided *configuration.ConfigurationJSON, logger log.Logger) (bool, error) {
	// we just use harness to grab certs from the env
	th := harness.NewMainHelper()

	c, err := client.NewConnection(host, th.ClientKeyPair, th.ClusterCert, logger)
	if err != nil {
		return false, fmt.Errorf("Error on connection: %v", err)
	}
	defer c.Shutdown()

	result, _, err := c.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		rootObjs, err := txn.GetRootObjects()
		if err != nil {
			return nil, err
		}
		obj, found := rootObjs[server.ConfigRootName]
		if !found {
			return nil, fmt.Errorf("No such root (%s) found for this account.", server.ConfigRootName)
		}
		return obj.Value()
	})
	if err != nil {
		return false, err
	}

	configFromGos := &configuration.ConfigurationJSON{}
	if err = json.Unmarshal(result.([]byte), configFromGos); err != nil {
		return false, err
	}

	if configFromGos.Equal(provided) {
		return true, nil
	} else {
		logger.Log("providedConfig", provided, "goshawkDBConfig", configFromGos)
		return false, nil
	}
}
