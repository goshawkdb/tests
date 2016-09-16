package main

import (
	"fmt"
	"goshawkdb.io/client"
	"log"
)

const (
	clusterCertPEM = `-----BEGIN CERTIFICATE-----
MIIBxzCCAW2gAwIBAgIIQqu37k6KPOIwCgYIKoZIzj0EAwIwOjESMBAGA1UEChMJ
R29zaGF3a0RCMSQwIgYDVQQDExtDbHVzdGVyIENBIFJvb3QgQ2VydGlmaWNhdGUw
IBcNMTYwMTAzMDkwODE2WhgPMjIxNjAxMDMwOTA4MTZaMDoxEjAQBgNVBAoTCUdv
c2hhd2tEQjEkMCIGA1UEAxMbQ2x1c3RlciBDQSBSb290IENlcnRpZmljYXRlMFkw
EwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEjHBXt+0n477zVZHTsGgu9rLYzNz/WMLm
l7/KC5v2nx+RC9yfkyfBKq8jJk3KYoB/YJ7s8BH0T456/+nRQIUo7qNbMFkwDgYD
VR0PAQH/BAQDAgIEMA8GA1UdEwEB/wQFMAMBAf8wGQYDVR0OBBIEEL9sxrcr6QTw
wk5csm2ZcfgwGwYDVR0jBBQwEoAQv2zGtyvpBPDCTlyybZlx+DAKBggqhkjOPQQD
AgNIADBFAiAy9NW3zE1ACYDWcp+qeTjQOfEtED3c/LKIXhrbzg2N/QIhANLb4crz
9ENxIifhZcJ/S2lqf49xZZS91dLF4x5ApKci
-----END CERTIFICATE-----`
	clientCertAndKeyPEM1 = `-----BEGIN CERTIFICATE-----
MIIBszCCAVmgAwIBAgIIfOmxD9dF8ZMwCgYIKoZIzj0EAwIwOjESMBAGA1UEChMJ
R29zaGF3a0RCMSQwIgYDVQQDExtDbHVzdGVyIENBIFJvb3QgQ2VydGlmaWNhdGUw
IBcNMTYwMTAzMDkwODUwWhgPMjIxNjAxMDMwOTA4NTBaMBQxEjAQBgNVBAoTCUdv
c2hhd2tEQjBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABFrAPcdlw5DWQmS9mCFX
FlD6R8ABaBf4LA821wVmPa9tiM6n8vRJvbmHuSjy8LwJJRRjo9GJq7KD6ZmsK9P9
sXijbTBrMA4GA1UdDwEB/wQEAwIHgDATBgNVHSUEDDAKBggrBgEFBQcDAjAMBgNV
HRMBAf8EAjAAMBkGA1UdDgQSBBBX9qcbG4ofUoUTHGwOgGvFMBsGA1UdIwQUMBKA
EL9sxrcr6QTwwk5csm2ZcfgwCgYIKoZIzj0EAwIDSAAwRQIgOK9PVJt7KdvDU/9v
z9gQI8JnVLZm+6gsh6ro9WnaZ8YCIQDXhjfQAWaUmJNTgKq3rLHiEbPS4Mxl7h7S
kbkX/2GIjg==
-----END CERTIFICATE-----
-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIN9Mf6CzDgCs1EbzJqDK3+12wcr7Ua3Huz6qNhyXCrS1oAoGCCqGSM49
AwEHoUQDQgAEWsA9x2XDkNZCZL2YIVcWUPpHwAFoF/gsDzbXBWY9r22Izqfy9Em9
uYe5KPLwvAklFGOj0YmrsoPpmawr0/2xeA==
-----END EC PRIVATE KEY-----`
	clientCertAndKeyPEM2 = `-----BEGIN CERTIFICATE-----
MIIBtTCCAVqgAwIBAgIJAM1u1BztQUtzMAoGCCqGSM49BAMCMDoxEjAQBgNVBAoT
CUdvc2hhd2tEQjEkMCIGA1UEAxMbQ2x1c3RlciBDQSBSb290IENlcnRpZmljYXRl
MCAXDTE2MDkxNjEwMTc0MVoYDzIyMTYwOTE2MTExNzQxWjAUMRIwEAYDVQQKEwlH
b3NoYXdrREIwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAT8e1VswMarHEht0aGE
iew/thoWuE+xmW3dlfHU0z2qWQ3uODt84xkbuBO9n/9wRRyfPSb5yUqCLfE+Zcr/
r2VFo20wazAOBgNVHQ8BAf8EBAMCB4AwEwYDVR0lBAwwCgYIKwYBBQUHAwIwDAYD
VR0TAQH/BAIwADAZBgNVHQ4EEgQQJtjcYy4cL2hQoXc5Vy4gpTAbBgNVHSMEFDAS
gBC/bMa3K+kE8MJOXLJtmXH4MAoGCCqGSM49BAMCA0kAMEYCIQDuvHeOsMknuAu9
H+9cFJR48S8F08SVzRJ7BQqwQD9i0AIhAN+8on1z/Br2HdGmERq3FNfq5TzaFAZP
zxgsTQ3LgaUu
-----END CERTIFICATE-----
-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIEPiYYcS9XfgSRBzYKMIzOvxMowIuztYpdalNpnCGGC8oAoGCCqGSM49
AwEHoUQDQgAE/HtVbMDGqxxIbdGhhInsP7YaFrhPsZlt3ZXx1NM9qlkN7jg7fOMZ
G7gTvZ//cEUcnz0m+clKgi3xPmXK/69lRQ==
-----END EC PRIVATE KEY-----`
)

func main() {
	port := 7894

	conn1, err := client.NewConnection(fmt.Sprintf("localhost:%d", port), []byte(clientCertAndKeyPEM1), []byte(clusterCertPEM))
	if err != nil {
		log.Fatalln(err)
	}
	defer conn1.Shutdown()
	conn2, err := client.NewConnection(fmt.Sprintf("localhost:%d", port), []byte(clientCertAndKeyPEM2), []byte(clusterCertPEM))
	if err != nil {
		log.Fatalln(err)
	}
	defer conn2.Shutdown()

	_, _, err = conn1.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		roots, err := txn.GetRootObjects()
		if err != nil {
			return nil, err
		}
		root := roots["test"]
		return nil, root.Set([]byte("Hello World"))
	})
	if err != nil {
		log.Fatalln(err)
	}

	value, _, err := conn2.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		roots, err := txn.GetRootObjects()
		if err != nil {
			return nil, err
		}
		root := roots["test"]
		return root.Value()
	})
	if err != nil {
		log.Fatalln(err)
	}
	if str := string(value.([]byte)); str != "Hello World" {
		log.Fatalln("conn2 got back", str)
	}

	_, _, err = conn2.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		roots, err := txn.GetRootObjects()
		if err != nil {
			return nil, err
		}
		root := roots["test"]
		return nil, root.Set([]byte("LOL!")) // should fail!
	})
	if err == nil {
		log.Fatalln("Expected an error!")
	}

	value, _, err = conn1.RunTransaction(func(txn *client.Txn) (interface{}, error) {
		roots, err := txn.GetRootObjects()
		if err != nil {
			return nil, err
		}
		root := roots["test"]
		return root.Value()
	})
	if err != nil {
		log.Fatalln(err)
	}
	if str := string(value.([]byte)); str != "Hello World" {
		log.Fatalln("conn1 got back", str)
	}
}
