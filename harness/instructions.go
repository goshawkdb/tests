package harness

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

type Instruction interface {
	Exec(*log.Logger) error
}

type Setup struct {
	gosBin    string
	rng       *rand.Rand
	logOutput io.Writer
	dir       string
}

func NewSetup(pathToGoshawkDBbinary string) *Setup {
	return &Setup{
		gosBin:    pathToGoshawkDBbinary,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
		logOutput: os.Stdout,
	}
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
	dir, err := ioutil.TempDir(os.TempDir(), "GoshawkDBHarness")
	if err != nil {
		l.Printf("Error encountered: %v", err)
		return err
	}
	l.Printf("Created dir in %s", dir)
	s.dir = dir
	return nil
}

func (s *Setup) String() string {
	return "Setup"
}

// Command

func (s *Setup) NewCmd(exePath string, args []string, cwd string, env []string) *Command {
	return &Command{
		Setup:   s,
		exePath: exePath,
		args:    args,
		cwd:     cwd,
		env:     env,
	}
}

type Command struct {
	*Setup
	exePath   string
	args      []string
	cwd       string
	env       []string
	cmd       *exec.Cmd
	stdout    io.ReadCloser
	stderr    io.ReadCloser
	readersWG *sync.WaitGroup
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
	eCmd := exec.Command(cmd.exePath, cmd.args...)
	eCmd.Env = cmd.env
	eCmd.Dir = cmd.cwd

	stdout, err := eCmd.StdoutPipe()
	var errkill error
	if err != nil {
		l.Printf("Error encountered: %v", err)
		if errkill = eCmd.Process.Kill(); errkill == nil {
			errkill = eCmd.Wait()
		}
		if errkill != nil {
			l.Printf("Supplementary error encountered when killing: %v", errkill)
		}
		return err
	}
	stderr, err := eCmd.StderrPipe()
	if err != nil {
		l.Printf("Error encountered: %v", err)
		if errkill = eCmd.Process.Kill(); errkill == nil {
			errkill = eCmd.Wait()
		}
		if errkill != nil {
			l.Printf("Supplementary error encountered when killing: %v", errkill)
		}
		return err
	}

	err = eCmd.Start()
	if err != nil {
		l.Printf("Error encountered: %v", err)
		if errkill = eCmd.Process.Kill(); errkill == nil {
			errkill = eCmd.Wait()
		}
		if errkill != nil {
			l.Printf("Supplementary error encountered when killing: %v", errkill)
		}
		return err
	}

	cmd.cmd = eCmd
	cmd.stdout = stdout
	cmd.stderr = stderr
	cmd.readersWG = new(sync.WaitGroup)
	cmd.readersWG.Add(2)
	go cmd.reader(stdout, cmd.cloneLogger(l, "StdOut"))
	go cmd.reader(stderr, cmd.cloneLogger(l, "StdErr"))

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

type commandSignal struct {
	*Command
	sig os.Signal
}

func (cmd *Command) Signal(sig os.Signal) Instruction {
	return &commandSignal{
		Command: cmd,
		sig:     sig,
	}
}

func (cmds *commandSignal) Exec(l *log.Logger) error {
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

func (cmds *commandSignal) String() string {
	return "Signal"
}

func (cmd *Command) Terminate() Instruction {
	return cmd.Signal(syscall.SIGTERM)
}

func (cmd *Command) Kill() Instruction {
	return cmd.Signal(syscall.SIGKILL)
}

// CommandWait

type commandWait Command

func (cmd *Command) Wait() Instruction {
	return (*commandWait)(cmd)
}

func (cmdw *commandWait) Exec(l *log.Logger) error {
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

func (cmdw *commandWait) String() string {
	return "Wait"
}

// RM

func (s *Setup) NewRM(name string, port uint16, certPath, configPath string) *RM {
	return &RM{
		Setup:      s,
		Command:    s.NewCmd(s.gosBin, nil, "", []string{}),
		name:       name,
		port:       port,
		certPath:   certPath,
		configPath: configPath,
	}
}

type RM struct {
	*Setup
	*Command
	name       string
	port       uint16
	certPath   string
	configPath string
}

func (rm *RM) Start() Instruction {
	return (*rmStart)(rm)
}

// rmStart

type rmStart RM

func (rms *rmStart) Exec(l *log.Logger) error {
	parentPrefix := l.Prefix()
	defer l.SetPrefix(parentPrefix)
	l.SetPrefix(fmt.Sprintf("%s|%v", parentPrefix, rms))

	if len(rms.Command.cwd) == 0 {
		dir, err := ioutil.TempDir(rms.dir, rms.name)
		if err != nil {
			l.Printf("Error encountered: %v", err)
			return err
		}
		rms.cwd = dir

		rms.args = []string{
			"-dir", dir,
			"-port", fmt.Sprintf("%d", rms.port),
			"-cert", rms.certPath,
			"-config", rms.configPath,
		}
	}

	return rms.Command.Start().start(l)
}

func (rms *rmStart) String() string {
	return fmt.Sprintf("RMStart:%v", rms.name)
}

// sleepy

type sleep struct {
	*Setup
	min, max time.Duration
}

func (s *sleep) Exec(l *log.Logger) error {
	d := s.min
	if diff := s.max - s.min; diff > 0 {
		d = s.min + time.Duration(s.rng.Int63n(int64(diff)))
	}
	parentPrefix := l.Prefix()
	defer l.SetPrefix(parentPrefix)
	l.SetPrefix(fmt.Sprintf("%s|%v", parentPrefix, s))
	l.Printf("Sleeping for %v...", d)
	time.Sleep(d)
	l.Printf("Sleeping for %v...done", d)
	return nil
}

func (s *Setup) Sleep(d time.Duration) Instruction {
	return &sleep{
		Setup: s,
		min:   d,
		max:   d,
	}
}

func (s *Setup) SleepRandom(min, max time.Duration) Instruction {
	return &sleep{
		Setup: s,
		min:   min,
		max:   max,
	}
}

func (s sleep) String() string {
	return "Sleep"
}

// absorbing errors

type absorbError struct {
	wrapped Instruction
}

func (s *Setup) AbsorbError(instr Instruction) Instruction {
	return &absorbError{wrapped: instr}
}

func (ae absorbError) Exec(l *log.Logger) error {
	parentPrefix := l.Prefix()
	defer l.SetPrefix(parentPrefix)
	l.SetPrefix(fmt.Sprintf("%s|%v", parentPrefix, ae))
	err := ae.wrapped.Exec(l)
	l.Printf("Absorbed: %v", err)
	return nil
}

func (ae absorbError) String() string {
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

// inParallel. This waits for the end of all of them

type inParallel struct {
	*Setup
	instrs []Instruction
}

func (s *Setup) InParallel(instrs ...Instruction) Instruction {
	return &inParallel{
		Setup:  s,
		instrs: instrs,
	}
}

func (ip *inParallel) Exec(l *log.Logger) error {
	wg := new(sync.WaitGroup)
	wg.Add(len(ip.instrs))
	errChan := make(chan error, len(ip.instrs))
	for idx, instr := range ip.instrs {
		instrCopy := instr
		loggerClone := ip.cloneLogger(l, fmt.Sprintf("InParallel(%d)", idx))
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

func (ip *inParallel) String() string {
	return fmt.Sprintf("InParallel %v", len(ip.instrs))
}

// untilError

type untilError struct {
	wrapped Instruction
}

func (s *Setup) UntilError(instr Instruction) Instruction {
	return &untilError{
		wrapped: instr,
	}
}

func (ue *untilError) Exec(l *log.Logger) error {
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

func (ue *untilError) String() string {
	return "UntilError"
}

// pickOne

type pickOne struct {
	*Setup
	instrs []Instruction
}

func (s *Setup) PickOne(instrs ...Instruction) Instruction {
	return &pickOne{
		Setup:  s,
		instrs: instrs,
	}
}

func (po *pickOne) Exec(l *log.Logger) error {
	parentPrefix := l.Prefix()
	defer l.SetPrefix(parentPrefix)
	picked := po.rng.Intn(len(po.instrs))
	instr := po.instrs[picked]
	l.SetPrefix(fmt.Sprintf("%s|%v(%d)", parentPrefix, po, picked))
	if err := instr.Exec(l); err != nil {
		l.Printf("Error encountered: %v", err)
		return err
	}
	return nil
}

func (po *pickOne) String() string {
	return fmt.Sprintf("PickOne %v", len(po.instrs))
}

type logMsg string

func (s *Setup) Log(msg string) Instruction {
	return logMsg(msg)
}

func (s logMsg) Exec(l *log.Logger) error {
	parentPrefix := l.Prefix()
	defer l.SetPrefix(parentPrefix)
	l.SetPrefix(fmt.Sprintf("%s|%v", parentPrefix, s))
	l.Print(string(s))
	return nil
}

func (s logMsg) String() string {
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
