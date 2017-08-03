package interpreter

import (
	"github.com/go-kit/kit/log"
	"goshawkdb.io/tests/harness"
	"os"
)

type InterpreterEnv struct {
	env harness.TestEnv
	log.Logger
}

func NewInterpreterEnv() *InterpreterEnv {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	return &InterpreterEnv{
		env:    harness.GetTestEnv(),
		Logger: logger,
	}
}

func (ie InterpreterEnv) Run(setup *Setup, prog Instruction) error {
	binaryPath := ie.env[harness.GoshawkDB]
	certPath := ie.env[harness.ClusterCert]
	configPath := ie.env[harness.ClusterConfig]

	ie.Log("GoshawkDB", binaryPath, "ClusterCert", certPath, "ClusterConfig", configPath)

	if len(binaryPath) > 0 {
		if err := setup.GosBin.SetPath(binaryPath, true); err != nil {
			return err
		}
	}

	if len(certPath) > 0 {
		if err := setup.GosCert.SetPath(certPath, false); err != nil {
			return err
		}
	}

	if len(configPath) > 0 {
		if err := setup.GosConfig.SetPath(configPath, false); err != nil {
			return err
		}
	}

	setup.SetEnv(ie.env.Clone())

	return prog.Exec(ie.Logger)
}

func (ie InterpreterEnv) MaybeExit(err error) error {
	if err != nil {
		ie.Log("error", err)
		os.Exit(1)
	}
	return err
}
