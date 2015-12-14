package banktransfer

import (
	"goshawkdb.io/tests"
	"testing"
)

var hosts = []string{"localhost:10001", "localhost:10002", "localhost:10003"}

// This is essentially testing for the A6 phantom anomaly.
func TestBankTransfer(t *testing.T) {
	BankTransfer(tests.NewTestHelper(t, hosts...))
}
