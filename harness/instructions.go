package harness

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type Instruction interface {
	Exec(*log.Logger) error
}

type Setup struct {
	rng       *rand.Rand
	logOutput io.Writer
	GosBin    *PathProvider
	GosConfig *PathProvider
	GosCert   *PathProvider
	Dir       *PathProvider
	env       []string
}

func NewSetup() *Setup {
	return &Setup{
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
		logOutput: os.Stdout,
		GosBin:    &PathProvider{},
		GosConfig: &PathProvider{},
		GosCert:   &PathProvider{},
		Dir:       &PathProvider{},
	}
}

func (s *Setup) SetEnv(envMap map[string]string) {
	env := make([]string, 0, len(envMap))
	for k, v := range envMap {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	s.env = env
}

func (s *Setup) NewLogger() *log.Logger {
	return log.New(s.logOutput, "", log.Ldate|log.Ltime|log.Lmicroseconds)
}

func (s *Setup) cloneLogger(l *log.Logger, prefixExt string) *log.Logger {
	return log.New(s.logOutput, fmt.Sprintf("%s|%s", l.Prefix(), prefixExt), l.Flags())
}

func (s *Setup) Exec(l *log.Logger) error {
	parentPrefix := l.Prefix()
	defer l.SetPrefix(parentPrefix)
	l.SetPrefix(fmt.Sprintf("%s|%v", parentPrefix, s))

	if len(s.Dir.Path()) == 0 {
		dir, err := ioutil.TempDir(os.TempDir(), "GoshawkDBHarness")
		if err != nil {
			l.Printf("Error encountered: %v", err)
			return err
		}
		l.Printf("Created dir in %s", dir)
		return s.Dir.SetPath(dir, false)
	} else {
		return s.Dir.EnsureDir()
	}
}

func (s *Setup) String() string {
	return "Setup"
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

func (cmd *CommandStart) Exec(l *log.Logger) error {
	parentPrefix := l.Prefix()
	defer l.SetPrefix(parentPrefix)
	l.SetPrefix(fmt.Sprintf("%s|%v", parentPrefix, cmd))
	return cmd.start(l)
}

func (cmd *CommandStart) start(l *log.Logger) error {
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
			l.Printf("Error encountered: %v", err)
			var errkill error
			if errkill = eCmd.Process.Kill(); errkill == nil {
				errkill = eCmd.Wait()
			}
			if errkill != nil {
				l.Printf("Supplementary error encountered when killing: %v", errkill)
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
	go cmd.reader(stdout, cmd.setup.cloneLogger(l, "StdOut"))
	go cmd.reader(stderr, cmd.setup.cloneLogger(l, "StdErr"))

	return nil
}

func (cmd *CommandStart) reader(reader io.ReadCloser, l *log.Logger) {
	defer cmd.readersWG.Done()
	lineReader := bufio.NewReader(reader)
	var err error
	var line []byte
	for err == nil {
		line, err = lineReader.ReadBytes('\n')
		if len(line) > 0 {
			l.Printf("%s", string(line))
		}
	}
	if err != nil && err != io.EOF {
		l.Printf("Error encountered: %v", err)
	} else {
		l.Print("Reader finished")
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

func (cmds *CommandSignal) Exec(l *log.Logger) error {
	parentPrefix := l.Prefix()
	defer l.SetPrefix(parentPrefix)
	l.SetPrefix(fmt.Sprintf("%s|%v", parentPrefix, cmds))
	l.Printf("Sending signal %v...", cmds.sig)
	if err := cmds.cmd.Process.Signal(cmds.sig); err != nil {
		l.Printf("Error encountered: %v", err)
		return err
	}
	l.Printf("Sending signal %v...done", cmds.sig)
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

func (cmdw *CommandWait) Exec(l *log.Logger) error {
	parentPrefix := l.Prefix()
	defer l.SetPrefix(parentPrefix)
	l.SetPrefix(fmt.Sprintf("%s|%v", parentPrefix, cmdw))
	l.Print("Waiting for process end...")
	if err := cmdw.cmd.Wait(); err != nil {
		l.Printf("Error encountered: %v", err)
		return err
	}
	cmdw.readersWG.Wait()
	cmdw.cmd = nil
	cmdw.stdout = nil
	cmdw.stderr = nil
	cmdw.readersWG = nil
	l.Print("Waiting for process end...done")
	return nil
}

func (cmdw *CommandWait) String() string {
	return "Wait"
}

// RM

type RM struct {
	setup *Setup
	*Command
	name       string
	port       uint16
	certPath   *PathProvider
	configPath *PathProvider
}

func (s *Setup) NewRM(name string, port uint16, certPath, configPath *PathProvider) *RM {
	if certPath == nil {
		certPath = s.GosCert
	}
	if configPath == nil {
		configPath = s.GosConfig
	}
	return &RM{
		setup:      s,
		Command:    s.NewCmd(s.GosBin, nil, &PathProvider{}, nil),
		name:       name,
		port:       port,
		certPath:   certPath,
		configPath: configPath,
	}
}

func (rm *RM) Start() *RMStart {
	return (*RMStart)(rm)
}

// RMStart

type RMStart RM

func (rms *RMStart) Exec(l *log.Logger) error {
	parentPrefix := l.Prefix()
	defer l.SetPrefix(parentPrefix)
	l.SetPrefix(fmt.Sprintf("%s|%v", parentPrefix, rms))

	if rms.Command.args == nil {
		dirPP := rms.Command.cwd
		err := dirPP.SetPath(filepath.Join(rms.setup.Dir.Path(), rms.name), false)
		if err != nil {
			l.Printf("Error encountered: %v", err)
			return err
		}
		if err = dirPP.EnsureDir(); err != nil {
			l.Printf("Error encountered: %v", err)
			return err
		}

		rms.Command.args = []string{
			"-dir", dirPP.Path(),
			"-port", fmt.Sprintf("%d", rms.port),
			"-cert", rms.certPath.Path(),
			"-config", rms.configPath.Path(),
		}

		rms.Command.env = rms.setup.env
	}

	return rms.Command.Start().start(l)
}

func (rms *RMStart) String() string {
	return fmt.Sprintf("RMStart:%v", rms.name)
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

func (s *Sleep) Exec(l *log.Logger) error {
	d := s.min
	if diff := s.max - s.min; diff > 0 {
		d = s.min + time.Duration(s.setup.rng.Int63n(int64(diff)))
	}
	parentPrefix := l.Prefix()
	defer l.SetPrefix(parentPrefix)
	l.SetPrefix(fmt.Sprintf("%s|%v", parentPrefix, s))
	l.Printf("Sleeping for %v...", d)
	time.Sleep(d)
	l.Printf("Sleeping for %v...done", d)
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

func (ae AbsorbError) Exec(l *log.Logger) error {
	parentPrefix := l.Prefix()
	defer l.SetPrefix(parentPrefix)
	l.SetPrefix(fmt.Sprintf("%s|%v", parentPrefix, ae))
	err := ae.wrapped.Exec(l)
	l.Printf("Absorbed: %v", err)
	return nil
}

func (ae AbsorbError) String() string {
	return "AbsorbError"
}

// programs

type Program []Instruction

func (p Program) Exec(l *log.Logger) error {
	parentPrefix := l.Prefix()
	defer l.SetPrefix(parentPrefix)
	for idx, instr := range p {
		l.SetPrefix(fmt.Sprintf("%s|Program(%d)", parentPrefix, idx))
		if err := instr.Exec(l); err != nil {
			l.Printf("Error encountered: %v", err)
			return err
		}
	}
	return nil
}

func (p Program) String() string {
	return fmt.Sprintf("Program %v", len(p))
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

func (ip *InParallel) Exec(l *log.Logger) error {
	wg := new(sync.WaitGroup)
	wg.Add(len(ip.instrs))
	errChan := make(chan error, len(ip.instrs))
	for idx, instr := range ip.instrs {
		instrCopy := instr
		loggerClone := ip.setup.cloneLogger(l, fmt.Sprintf("InParallel(%d)", idx))
		go func() {
			defer wg.Done()
			if err := instrCopy.Exec(loggerClone); err != nil {
				loggerClone.Printf("Error encountered: %v", err)
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
	return fmt.Sprintf("InParallel %v", len(ip.instrs))
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

func (ue *UntilError) Exec(l *log.Logger) error {
	parentPrefix := l.Prefix()
	defer l.SetPrefix(parentPrefix)
	for idx := 0; true; idx++ {
		l.SetPrefix(fmt.Sprintf("%s|%v(%d)", parentPrefix, ue, idx))
		if err := ue.wrapped.Exec(l); err != nil {
			l.Printf("Error encountered: %v", err)
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

func (po *PickOne) Exec(l *log.Logger) error {
	parentPrefix := l.Prefix()
	defer l.SetPrefix(parentPrefix)
	picked := po.setup.rng.Intn(len(po.instrs))
	instr := po.instrs[picked]
	l.SetPrefix(fmt.Sprintf("%s|%v(%d)", parentPrefix, po, picked))
	if err := instr.Exec(l); err != nil {
		l.Printf("Error encountered: %v", err)
		return err
	}
	return nil
}

func (po *PickOne) String() string {
	return fmt.Sprintf("PickOne %v", len(po.instrs))
}

type LogMsg string

func (s *Setup) Log(msg string) LogMsg {
	return LogMsg(msg)
}

func (s LogMsg) Exec(l *log.Logger) error {
	parentPrefix := l.Prefix()
	defer l.SetPrefix(parentPrefix)
	l.SetPrefix(fmt.Sprintf("%s|%v", parentPrefix, s))
	l.Print(string(s))
	return nil
}

func (s LogMsg) String() string {
	return "Log"
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
