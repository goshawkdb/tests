package compare

import (
	"encoding/json"
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/server"
	"goshawkdb.io/server/configuration"
	"goshawkdb.io/tests"
)

func CompareConfigs(host string, provided *configuration.ConfigurationJSON) (bool, error) {
	// we just use TestHelper to grab certs from the env
	th := tests.NewTestHelper(nil)

	c, err := client.NewConnection(host, th.ClientKeyPair, th.ClusterCert)
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
		return false, nil
	}
}
