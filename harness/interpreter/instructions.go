package interpreter

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-kit/kit/log"
	config "goshawkdb.io/server/configuration"
	"goshawkdb.io/tests/harness"
	iconfig "goshawkdb.io/tests/harness/interpreter/config"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Instruction interface {
	Exec(log.Logger) error
}

type Setup struct {
	rng       *rand.Rand
	GosBin    *PathProvider
	GosConfig *PathProvider
	GosCert   *PathProvider
	Dir       *PathProvider
	env       []string
}

func NewSetup() *Setup {
	return &Setup{
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
		GosBin:    &PathProvider{},
		GosConfig: &PathProvider{},
		GosCert:   &PathProvider{},
		Dir:       &PathProvider{},
	}
}

func (s *Setup) SetEnv(envMap harness.TestEnv) {
	env := make([]string, 0, len(envMap))
	for k, v := range envMap {
		env = append(env, fmt.Sprintf("%s=%s", string(k), v))
	}
	s.env = env
}

func (s *Setup) Exec(l log.Logger) error {
	l = log.With(l, "i", s.String())

	if len(s.Dir.Path()) == 0 {
		dir, err := ioutil.TempDir(os.TempDir(), "GoshawkDBHarness")
		if err != nil {
			return err
		}
		l.Log("msg", "Created directory.", "directory", dir)
		return s.Dir.SetPath(dir, false)
	} else {
		return s.Dir.EnsureDir()
	}
}

func (s *Setup) String() string {
	return "Setup"
}

// lazy path

type LazyPath interface {
	Path() string
}

// path provider

type PathProvider struct {
	p string
}

func NewPathProvider(p string, isCmd bool) (*PathProvider, error) {
	pp := &PathProvider{}
	if err := pp.SetPath(p, isCmd); err == nil {
		return pp, nil
	} else {
		return nil, err
	}
}

func (pp *PathProvider) SetPath(p string, isCmd bool) (err error) {
	if len(p) > 0 {
		if isCmd {
			p, err = exec.LookPath(p)
			if err != nil {
				return
			}
		}

		p, err = filepath.Abs(p)
		if err != nil {
			return
		}
	}
	pp.p = p
	return
}

func (pp *PathProvider) Path() string {
	return pp.p
}

func (pp *PathProvider) EnsureDir() error {
	if len(pp.p) == 0 {
		return errors.New("Cannot create dir of empty path")
	}
	return os.MkdirAll(pp.p, 0750)
}

func (pp *PathProvider) Join(str string) *PathJoin {
	return &PathJoin{
		pp:    pp,
		extra: str,
	}
}

// path join

type PathJoin struct {
	pp    *PathProvider
	extra string
}

func (pj *PathJoin) Path() string {
	return filepath.Join(pj.pp.Path(), pj.extra)
}

// path copier

type PathCopier struct {
	src      *PathProvider
	dest     *PathProvider
	receiver *PathProvider
}

func (pp *PathProvider) CopyTo(dir, receiver *PathProvider) Instruction {
	return &PathCopier{
		src:      pp,
		dest:     dir,
		receiver: receiver,
	}
}

func (pc *PathCopier) Exec(l log.Logger) error {
	l = log.With(l, "i", pc.String())

	src := pc.src.Path()
	dest := pc.dest.Path()
	l.Log("source", src, "destination", dest)
	data, err := ioutil.ReadFile(src)
	if err != nil {
		return err
	}
	destStat, err := os.Stat(dest)
	if err != nil {
		return err
	}
	if destStat.Mode().IsDir() {
		dest = filepath.Join(dest, filepath.Base(src))
	}
	err = ioutil.WriteFile(dest, data, 0644)
	if err != nil {
		return err
	}
	return pc.receiver.SetPath(dest, false)
}

func (pc *PathCopier) String() string {
	return "PathCopier"
}

// Config provider

type ConfigProvider interface {
	Eval() (*config.ConfigurationJSON, error)
}

type BaseConfigProvider config.ConfigurationJSON

func (bcp *BaseConfigProvider) Eval() (*config.ConfigurationJSON, error) {
	return (*config.ConfigurationJSON)(bcp), nil
}

type MutableConfigProvider struct {
	cp           ConfigProvider
	result       *config.ConfigurationJSON
	hostDeltas   map[string]bool
	fPrime       *uint8
	clientDeltas map[string]map[string]*config.CapabilityJSON
}

func NewMutableConfigProvider(c *config.ConfigurationJSON) *MutableConfigProvider {
	return &MutableConfigProvider{
		cp:           (*BaseConfigProvider)(c),
		hostDeltas:   make(map[string]bool),
		clientDeltas: make(map[string]map[string]*config.CapabilityJSON),
	}
}

func (mcp *MutableConfigProvider) Clone() *MutableConfigProvider {
	return &MutableConfigProvider{
		cp:           mcp,
		hostDeltas:   make(map[string]bool),
		clientDeltas: make(map[string]map[string]*config.CapabilityJSON),
	}
}

func (mcp *MutableConfigProvider) Ports() ([]uint16, error) {
	c, err := mcp.cp.Eval()
	if err != nil {
		return nil, err
	}
	ports := make([]uint16, len(c.Hosts))
	for idx, host := range c.Hosts {
		_, portStr, err := net.SplitHostPort(host)
		if err != nil {
			return nil, err
		}
		portInt64, err := strconv.ParseUint(portStr, 0, 16)
		if err != nil {
			return nil, err
		}
		ports[idx] = uint16(portInt64)
	}
	return ports, nil
}

func (mcp *MutableConfigProvider) AddHost(host string) *MutableConfigProvider {
	mcp.hostDeltas[host] = true
	return mcp
}

func (mcp *MutableConfigProvider) RemoveHost(host string) *MutableConfigProvider {
	mcp.hostDeltas[host] = false
	return mcp
}

func (mcp *MutableConfigProvider) ChangeF(f uint8) *MutableConfigProvider {
	mcp.fPrime = &f
	return mcp
}

func (mcp *MutableConfigProvider) Eval() (*config.ConfigurationJSON, error) {
	if mcp.result != nil {
		return mcp.result, nil
	}
	c, err := mcp.cp.Eval()
	if err != nil {
		return nil, err
	}

	c.Version += 1
	if len(c.ClusterId) == 0 {
		c.ClusterId = fmt.Sprintf("Test%d", time.Now().UnixNano())
	}

	for hostPrime, added := range mcp.hostDeltas {
		found := false
		for idx, host := range c.Hosts {
			if found = host == hostPrime; found {
				if !added {
					c.Hosts = append(c.Hosts[:idx], c.Hosts[idx+1:]...)
				}
				break
			}
		}
		if added && !found {
			c.Hosts = append(c.Hosts, hostPrime)
		}
	}
	if mcp.fPrime != nil {
		c.F = *mcp.fPrime
	}
	for fingerprint, roots := range mcp.clientDeltas {
		if roots == nil {
			delete(c.ClientCertificateFingerprints, fingerprint)
		} else {
			c.ClientCertificateFingerprints[fingerprint] = roots
		}
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	mcp.result = c
	return c, nil
}

func (mcp *MutableConfigProvider) Writer(lp LazyPath) *ConfigWriter {
	return &ConfigWriter{
		lp: lp,
		cp: mcp,
	}
}

func (mcp *MutableConfigProvider) NewConfigComparer(hosts ...string) *ConfigComparer {
	return &ConfigComparer{
		cp:    mcp,
		hosts: hosts,
	}
}

// config writer

type ConfigWriter struct {
	lp LazyPath
	cp ConfigProvider
}

func (cw *ConfigWriter) Exec(l log.Logger) error {
	l = log.With(l, "i", cw.String())
	if c, err := cw.cp.Eval(); err == nil {
		dest := cw.lp.Path()
		l.Log("destination", dest)
		data, err := json.MarshalIndent(c, "", "\t")
		if err != nil {
			return err
		}
		return ioutil.WriteFile(dest, data, 0644)
	} else {
		return err
	}
}

func (cw *ConfigWriter) String() string {
	return "ConfigWriter"
}

// Config Compare

type ConfigComparer struct {
	cp    ConfigProvider
	hosts []string
}

func (cc *ConfigComparer) Exec(l log.Logger) error {
	l = log.With(l, "i", cc.String())
	if c, err := cc.cp.Eval(); err == nil {
		for _, host := range cc.hosts {
			m := log.With(l, "host", host)
			if equal, err := iconfig.CompareConfigs(host, c, m); err != nil {
				m.Log("error", err)
				return err
			} else if !equal {
				err = errors.New("Configs do not match.")
				m.Log("error", err)
				return err
			} else {
				m.Log("msg", "Configs match.")
			}
		}
		return nil
	} else {
		return err
	}
}

func (cc *ConfigComparer) String() string {
	return "ConfigComparer"
}

// Command

type Command struct {
	setup     *Setup
	exePath   *PathProvider
	args      []string
	cwd       *PathProvider
	env       []string
	cmd       *exec.Cmd
	stdout    io.ReadCloser
	stderr    io.ReadCloser
	readersWG *sync.WaitGroup
}

func (s *Setup) NewCmd(exePath *PathProvider, args []string, cwd *PathProvider, env []string) *Command {
	return &Command{
		setup:   s,
		exePath: exePath,
		args:    args,
		cwd:     cwd,
		env:     env,
	}
}

// CommandStart. Does not block to wait for end of cmd

type CommandStart Command

func (cmd *Command) Start() *CommandStart {
	return (*CommandStart)(cmd)
}

func (cmd *CommandStart) Exec(l log.Logger) error {
	l = log.With(l, "i", cmd.String())
	return cmd.start(l)
}

func (cmd *CommandStart) start(l log.Logger) error {
	eCmd := exec.Command(cmd.exePath.Path(), cmd.args...)
	eCmd.Env = cmd.env
	if err := cmd.cwd.EnsureDir(); err != nil {
		return err
	}
	eCmd.Dir = cmd.cwd.Path()

	killFun := func(err error) error {
		if err == nil {
			return nil
		} else {
			l.Log("error", err)
			var errkill error
			if errkill = eCmd.Process.Kill(); errkill == nil {
				errkill = eCmd.Wait()
			}
			if errkill != nil {
				l.Log("msg", "Supplementary error encountered when killing.", "error", errkill)
			}
			return err
		}
	}

	stdout, err := eCmd.StdoutPipe()
	if err = killFun(err); err != nil {
		return err
	}
	stderr, err := eCmd.StderrPipe()
	if err = killFun(err); err != nil {
		return err
	}
	err = eCmd.Start()
	if err = killFun(err); err != nil {
		return err
	}

	cmd.cmd = eCmd
	cmd.stdout = stdout
	cmd.stderr = stderr
	cmd.readersWG = new(sync.WaitGroup)
	cmd.readersWG.Add(2)
	go cmd.reader(stdout, log.With(l, "fd", "stdout"))
	go cmd.reader(stderr, log.With(l, "fd", "stderr"))

	return nil
}

func (cmd *CommandStart) reader(reader io.ReadCloser, l log.Logger) {
	defer cmd.readersWG.Done()
	lineReader := bufio.NewReader(reader)
	var err error
	var line []byte
	for err == nil {
		line, err = lineReader.ReadBytes('\n')
		if len(line) > 0 {
			l.Log("line", string(line))
		}
	}
	if err != nil && err != io.EOF {
		l.Log("error", err)
	} else {
		l.Log("msg", "Reader finished.")
	}
}

func (cmd *CommandStart) String() string {
	return "CommandStart"
}

// CommandSignal

type CommandSignal struct {
	*Command
	sig os.Signal
}

func (cmd *Command) Signal(sig os.Signal) *CommandSignal {
	return &CommandSignal{
		Command: cmd,
		sig:     sig,
	}
}

func (cmds *CommandSignal) Exec(l log.Logger) error {
	l = log.With(l, "i", cmds.String())
	l.Log("signal", cmds.sig.String())
	if err := cmds.cmd.Process.Signal(cmds.sig); err != nil {
		l.Log("ignoredError", err)
	}
	return nil
}

func (cmds *CommandSignal) String() string {
	return "Signal"
}

func (cmd *Command) Terminate() *CommandSignal {
	return cmd.Signal(syscall.SIGTERM)
}

func (cmd *Command) Kill() *CommandSignal {
	return cmd.Signal(syscall.SIGKILL)
}

// CommandWait

type CommandWait Command

func (cmd *Command) Wait() *CommandWait {
	return (*CommandWait)(cmd)
}

func (cmdw *CommandWait) Exec(l log.Logger) error {
	l = log.With(l, "i", cmdw.String())
	l.Log("msg", "begin")
	cmdw.readersWG.Wait()
	if err := cmdw.cmd.Wait(); err != nil {
		l.Log("error", err)
		return err
	}
	cmdw.cmd = nil
	cmdw.stdout = nil
	cmdw.stderr = nil
	cmdw.readersWG = nil
	l.Log("msg", "end")
	return nil
}

func (cmdw *CommandWait) String() string {
	return "Wait"
}

// RM

type RM struct {
	setup *Setup
	*Command
	Name       string
	Port       uint16
	certPath   LazyPath
	configPath LazyPath
}

func (s *Setup) NewRM(name string, port uint16, certPath, configPath LazyPath) *RM {
	if certPath == nil {
		certPath = s.GosCert
	}
	if configPath == nil {
		configPath = s.GosConfig
	}
	return &RM{
		setup:      s,
		Command:    s.NewCmd(s.GosBin, nil, &PathProvider{}, nil),
		Name:       name,
		Port:       port,
		certPath:   certPath,
		configPath: configPath,
	}
}

func (rm *RM) Start() *RMStart {
	return (*RMStart)(rm)
}

// RMStart

type RMStart RM

func (rms *RMStart) Exec(l log.Logger) error {
	l = log.With(l, "i", rms.String())

	if rms.Command.args == nil {
		dirPP := rms.Command.cwd
		err := dirPP.SetPath(filepath.Join(rms.setup.Dir.Path(), rms.Name), false)
		if err != nil {
			l.Log("error", err)
			return err
		}
		if err = dirPP.EnsureDir(); err != nil {
			l.Log("error", err)
			return err
		}

		rms.Command.args = []string{
			"-dir", dirPP.Path(),
			"-port", fmt.Sprintf("%d", rms.Port),
			"-cert", rms.certPath.Path(),
			"-config", rms.configPath.Path(),
			"-prometheusPort", fmt.Sprintf("%d", 1000+rms.Port),
			"-wssPort", "0",
		}

		rms.Command.env = rms.setup.env
	}

	return rms.Command.Start().start(l)
}

func (rms *RMStart) String() string {
	return fmt.Sprintf("RMStart(%s)", rms.Name)
}

// sleepy

type Sleep struct {
	setup    *Setup
	min, max time.Duration
}

func (s *Setup) Sleep(d time.Duration) *Sleep {
	return &Sleep{
		setup: s,
		min:   d,
		max:   d,
	}
}

func (s *Setup) SleepRandom(min, max time.Duration) *Sleep {
	return &Sleep{
		setup: s,
		min:   min,
		max:   max,
	}
}

func (s *Sleep) Exec(l log.Logger) error {
	d := s.min
	if diff := s.max - s.min; diff > 0 {
		d = s.min + time.Duration(s.setup.rng.Int63n(int64(diff)))
	}
	l = log.With(l, "i", s.String())
	l.Log("begin", d)
	time.Sleep(d)
	l.Log("end", d)
	return nil
}

func (s Sleep) String() string {
	return "Sleep"
}

// absorbing errors

type AbsorbError struct {
	wrapped Instruction
}

func (s *Setup) AbsorbError(instr Instruction) *AbsorbError {
	return &AbsorbError{wrapped: instr}
}

func (ae AbsorbError) Exec(l log.Logger) error {
	l = log.With(l, "i", ae.String())
	if err := ae.wrapped.Exec(l); err != nil {
		l.Log("absorbed", err)
	}
	return nil
}

func (ae AbsorbError) String() string {
	return "AbsorbError"
}

// programs

type Program []Instruction

func (p Program) Exec(l log.Logger) error {
	l = log.With(l, "i", p.String())
	for idx, instr := range p {
		m := log.With(l, "c", idx)
		if err := instr.Exec(m); err != nil {
			m.Log("error", err)
			return err
		}
	}
	return nil
}

func (p Program) String() string {
	return fmt.Sprintf("Program(%v)", len(p))
}

// InParallel. This waits for the end of all of them

type InParallel struct {
	setup  *Setup
	instrs []Instruction
}

func (s *Setup) InParallel(instrs ...Instruction) *InParallel {
	return &InParallel{
		setup:  s,
		instrs: instrs,
	}
}

func (ip *InParallel) Exec(l log.Logger) error {
	l = log.With(l, "i", ip.String())
	wg := new(sync.WaitGroup)
	wg.Add(len(ip.instrs))
	errChan := make(chan error, len(ip.instrs))
	for idx, instr := range ip.instrs {
		instrCopy := instr
		m := log.With(l, "c", idx)
		go func() {
			defer wg.Done()
			if err := instrCopy.Exec(m); err != nil {
				m.Log("error", err)
				errChan <- err
			}
		}()
	}
	wg.Wait()
	errors := make([]error, 0, len(ip.instrs))
	close(errChan)
	for err := range errChan {
		errors = append(errors, err)
	}
	if len(errors) > 0 {
		return Errors(errors)
	} else {
		return nil
	}
}

func (ip *InParallel) String() string {
	return fmt.Sprintf("InParallel(%d)", len(ip.instrs))
}

// UntilError

type UntilError struct {
	wrapped Instruction
}

func (s *Setup) UntilError(instr Instruction) Instruction {
	return &UntilError{
		wrapped: instr,
	}
}

func (ue *UntilError) Exec(l log.Logger) error {
	l = log.With(l, "i", ue.String())
	for idx := 0; true; idx++ {
		m := log.With(l, "c", idx)
		if err := ue.wrapped.Exec(m); err != nil {
			m.Log("error", err)
			return err
		}
	}
	return nil // ha! no dead code elimination!
}

func (ue *UntilError) String() string {
	return "UntilError"
}

// PickOne

type PickOne struct {
	setup  *Setup
	instrs []Instruction
}

func (s *Setup) PickOne(instrs ...Instruction) *PickOne {
	return &PickOne{
		setup:  s,
		instrs: instrs,
	}
}

func (po *PickOne) Exec(l log.Logger) error {
	l = log.With(l, "i", po.String())
	picked := po.setup.rng.Intn(len(po.instrs))
	instr := po.instrs[picked]
	l.Log("picked", picked)
	if err := instr.Exec(l); err != nil {
		l.Log("error", err)
		return err
	}
	return nil
}

func (po *PickOne) String() string {
	return fmt.Sprintf("PickOne(%d)", len(po.instrs))
}

type LogMsg string

func (s *Setup) Log(msg string) LogMsg {
	return LogMsg(msg)
}

func (s LogMsg) Exec(l log.Logger) error {
	l.Log("i", s.String, "msg", string(s))
	return nil
}

func (s LogMsg) String() string {
	return "LogMsg"
}

// UntilStopped (also stops on error)

type UntilStopped struct {
	wrapped  Instruction
	stopped  uint32
	finished chan struct{}
}

func (s *Setup) UntilStopped(instr Instruction) *UntilStopped {
	us := &UntilStopped{
		wrapped:  instr,
		stopped:  0,
		finished: make(chan struct{}),
	}
	return us
}

func (us *UntilStopped) Exec(l log.Logger) error {
	l = log.With(l, "i", us.String())
	for idx := 0; 0 == atomic.LoadUint32(&us.stopped); idx++ {
		m := log.With(l, "c", idx)
		if err := us.wrapped.Exec(m); err != nil {
			m.Log("error", err)
			return err
		}
	}
	close(us.finished)
	return nil
}

func (us *UntilStopped) Stop() *UntilStoppedStop {
	return (*UntilStoppedStop)(us)
}

func (us *UntilStopped) String() string {
	return "UntilStopped"
}

type UntilStoppedStop UntilStopped

func (uss *UntilStoppedStop) Exec(l log.Logger) error {
	l.Log("i", uss.String())
	atomic.StoreUint32(&uss.stopped, 1)
	<-uss.finished
	return nil
}

func (uss *UntilStoppedStop) String() string {
	return "UntilStoppedStop"
}

// errors

type Errors []error

func (e Errors) Error() string {
	str := ""
	for _, err := range e {
		str = fmt.Sprintf("%s\n%v", str, err)
	}
	if len(str) == 0 {
		return str
	} else {
		return str[1:]
	}
}
