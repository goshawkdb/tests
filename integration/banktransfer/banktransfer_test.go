package banktransfer

import (
	"goshawkdb.io/tests/harness"
	"testing"
)

// This is essentially testing for the A6 phantom anomaly.
func TestBankTransfer(t *testing.T) {
	BankTransfer(harness.NewTestHelper(t))
}
