# GoshawkDB Integration Tests

These tests are all written in Go using the
[go client](https://goshawkdb.io/documentation/client-go.html). Several
of these tests really take the form of soak tests: it's impossible to
_prove_ from a test that (for example) strong serializability can't be
violated, so the best you can do is try and write tests that would
fail if strong serializability is violated, and run them for a long
time. So budget a good amount of time if you want to run all of
these. Fast computers and disks will make a big difference.

The tests all need a working GoshawkDB server to run against. The
simplest way to get started is to
[install the server](https://goshawkdb.io/starting.html), then:


    $ goshawkdb -config testConfig.json -cert testCert.pem


This will start a 1-node GoshawkDB cluster running on `localhost` with
the default port, using known certificates and key pairs. Each test
has its own directory, and the `order.txt` file gives a suggested
order for running the tests in, starting from the simplest and
fastest. So, starting with `solocount`:

    $ cd solocount
    $ go test -v |& tee test.log

This uses certificates and client keypairs that are baked into the
test harness (see `helpers.go` and `testCert.pem`). Obviously, these
certificates and key pairs are public, so please do not deploy them in
production! These are also the same certificates and key pairs that
are checked into the
[Java client](https://goshawkdb.io/documentation/client-java.html)

To set the authentication or customise the servers to which the tests
connect, there are three environment variables to set (again, these
are exactly the same as the Java client and work in the same way):

* `GOSHAWKDB_CLUSTER_HOSTS` This is a comma-separated list of hosts to
  which the tests will try to connect. They must all be part of the
  same cluster. If, for example, you happen to be running a three-node
  cluster all on `localhost` ports 10001, 10002 and 10003, you may
  wish to set this to
  `"localhost:10001,localhost:10002,localhost:10003"`.
* `GOSHAWKDB_CLUSTER_CERT` This is a path to a file containing
  the cluster X.509 certificate which the client uses to authenticate
  the server. This file should contain the cluster certificate only,
  and *not* the cluster key pair.
* `GOSHAWKDB_CLIENT_KEYPAIR` This is a path to a file
  containing a client X.509 certificate and key pair which the client
  uses to authenticate to the server.
* `GOSHAWKDB_ROOT_NAME` This is the name of the root object to use.
  By default it is `test`. You must configure your GoshawkDB cluster to
  have such a root object and it must be fully writable by the test
  client account.

If you set these in the terminal before invoking `go test` then
they will be picked up.

## Topology Migration Tests

In the `topology` directory are many scenarios for testing topology
migrations. These are all manual tests currently: it is up to you to
figure out how to build the first topology and what needs to be done
to migrate to the latter.
