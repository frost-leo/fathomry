//go:build linux || darwin

/**
 * fathomry
 * Copyright (C) 2026  Frost Leo
 * SPDX-License-Identifier: GPL-3.0-or-later
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package build

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func buildFixture(t *testing.T, directory, document string, changeSource bool) {
	t.Helper()
	mutation := ""
	if changeSource {
		mutation = `if err:=os.WriteFile("main.go",[]byte("invalid source after inspection"),0600);err!=nil {panic(err)}`
	}
	source := fmt.Sprintf(`package main
import ("fmt";"os")
func main() {
 if len(os.Args)==2 && os.Args[1]==%q {
  %s
  fmt.Print(%q)
  return
 }
 fmt.Print("business-result")
}
`, InspectionArgument, mutation, document)
	if err := os.WriteFile(filepath.Join(directory, "main.go"), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestNativeBuildGate(t *testing.T) {
	if testing.Short() {
		t.Skip("real Go compiler integration")
	}
	if !supportedHost {
		t.Skip("native build host not supported")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	directory := filepath.Join(t.TempDir(), "project with spaces")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "go.mod"), []byte("module example.org/health\ngo 1.27.0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "artifact")
	document := `{"schema":"fathomry.project-inspection/v1","name":"health","bundles":[]}`
	buildFixture(t, directory, document, false)
	result, err := Run(ctx, Options{Directory: directory, Output: output})
	if err != nil || !result.Published || result.Workflows != 0 || result.Name != "health" {
		t.Fatal(result, err)
	}
	if hash, err := executableHash(ctx, output); err != nil || hash != result.SHA256 {
		t.Fatal("published different bytes", err)
	}
	if raw, err := exec.CommandContext(ctx, output).Output(); err != nil || string(raw) != "business-result" {
		t.Fatal("artifact not runnable", err)
	}
	original := result.SHA256
	for _, bad := range []string{"{}", `{"schema":"fathomry.project-inspection/v1","name":"health","bundles":[{}]}`} {
		buildFixture(t, directory, bad, false)
		result, err := Run(ctx, Options{Directory: directory, Output: output})
		if err == nil || result.Published {
			t.Fatal("invalid declaration published")
		}
		if hash, _ := executableHash(ctx, output); hash != original {
			t.Fatal("old artifact lost")
		}
	}
	if err := os.WriteFile(filepath.Join(directory, "main.go"), []byte("broken source"), 0600); err != nil {
		t.Fatal(err)
	}
	if result, err := Run(ctx, Options{Directory: directory, Output: output}); !errors.Is(err, ErrCompile) || result.Published {
		t.Fatal(result, err)
	}
	if hash, _ := executableHash(ctx, output); hash != original {
		t.Fatal("failed compilation reused old program")
	}
	buildFixture(t, directory, document, true)
	result, err = Run(ctx, Options{Directory: directory, Output: output})
	if err != nil || !result.Published {
		t.Fatal("inspected binary was recompiled from changed source", err)
	}
	if hash, _ := executableHash(ctx, output); hash != result.SHA256 {
		t.Fatal("publication hash disagrees")
	}
}

func TestCrossTargetBuild(t *testing.T) {
	if !supportedHost {
		t.Skip("native build host not supported")
	}
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "go.mod"), []byte("module example.org/health\ngo 1.27.0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	target := "windows"
	if runtime.GOOS == "windows" {
		target = "linux"
	}
	t.Setenv("GOOS", target)
	if _, err := Run(context.Background(), Options{Directory: directory}); !errors.Is(err, ErrPlatform) {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(directory, "bin")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("cross build created output")
	}
}

func TestCommandLifecycle(t *testing.T) {
	for _, test := range []struct {
		name   string
		script string
		status int
	}{
		{"success", "printf output; printf diagnostic >&2", 0},
		{"native-exit", "printf output; printf diagnostic >&2; exit 9", 9},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			process := exec.Command("/bin/sh", "-c", test.script)
			var stdout, stderr bytes.Buffer
			process.Stdout, process.Stderr = &stdout, &stderr
			process.WaitDelay = time.Second
			err := runCommand(ctx, process)
			var exited *exec.ExitError
			if test.status == 0 && err != nil || test.status != 0 && (!errors.As(err, &exited) || exited.ExitCode() != test.status) {
				t.Fatal("native exit changed", err)
			}
			if stdout.String() != "output" || stderr.String() != "diagnostic" {
				t.Fatal("output collection changed", stdout.String(), stderr.String())
			}
		})
	}
	t.Run("canceled-before-start", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		process := exec.Command("/bin/sleep", "60")
		if err := runCommand(ctx, process); !errors.Is(err, context.Canceled) || process.Process != nil {
			t.Fatal("canceled invocation started a child", err)
		}
	})
	t.Run("start-failure", func(t *testing.T) {
		process := exec.Command(filepath.Join(t.TempDir(), "missing"))
		if err := runCommand(context.Background(), process); !errors.Is(err, os.ErrNotExist) || process.Process != nil {
			t.Fatal("native start failure changed", err)
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		process := exec.Command("/bin/sleep", "60")
		began := time.Now()
		err := runCommand(ctx, process)
		var exited *exec.ExitError
		if ctx.Err() == nil || !errors.As(err, &exited) || time.Since(began) > 3*time.Second {
			t.Fatal("cancellation did not terminate and reap the child", err)
		}
	})
}

func TestProcessExitObservation(t *testing.T) {
	process := exec.Command("/bin/sh", "-c", "kill -STOP $$; exit 7")
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = process.Process.Kill()
		_ = process.Wait()
	})
	var status unix.WaitStatus
	for {
		_, err := unix.Wait4(process.Process.Pid, &status, unix.WUNTRACED, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil || !status.Stopped() {
			t.Fatal("stopped-child control failed", status, err)
		}
		break
	}
	observed := make(chan error, 1)
	go func() { observed <- waitProcessExit(process.Process.Pid) }()
	select {
	case err := <-observed:
		t.Fatal("stopped child mistaken for an exited child", err)
	case <-time.After(30 * time.Millisecond):
	}
	if err := process.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-observed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("exit observation did not terminate")
	}
	if err := waitProcessExit(process.Process.Pid); err != nil {
		t.Fatal("first observation reaped the child", err)
	}
	var exited *exec.ExitError
	if err := process.Wait(); !errors.As(err, &exited) || exited.ExitCode() != 7 {
		t.Fatal("native wait status was consumed", err)
	}
}

func TestSealedProcessGroupCannotSignal(t *testing.T) {
	process := exec.Command("/bin/sleep", "60")
	process.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	go func() {
		_ = process.Wait()
		close(exited)
	}()
	t.Cleanup(func() {
		_ = process.Process.Kill()
		<-exited
	})
	group := &processGroup{pid: process.Process.Pid}
	if err := group.finish(false); err != nil {
		t.Fatal(err)
	}
	var callers sync.WaitGroup
	for range 20 {
		callers.Go(group.signal)
		callers.Go(func() {
			if err := group.finish(true); err != nil {
				t.Error(err)
			}
		})
	}
	callers.Wait()
	select {
	case <-exited:
		t.Fatal("sealed group killed a live process")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestProcessFixture(t *testing.T) {
	mode := os.Getenv("FATHOMRY_BUILD_FIXTURE")
	if mode == "" {
		return
	}
	switch mode {
	case "ok":
		os.Stdout.WriteString("ok")
	case "exit":
		os.Stderr.WriteString("private-compiler-detail")
		os.Exit(9)
	case "stdout", "stderr":
		stream := os.Stdout
		if mode == "stderr" {
			stream = os.Stderr
		}
		for {
			if _, err := stream.Write(bytes.Repeat([]byte("x"), 8192)); err != nil {
				os.Exit(2)
			}
		}
	case "hang":
		time.Sleep(time.Hour)
	case "orphan":
		child := exec.Command(os.Args[0], "-test.run=^TestProcessFixture$")
		child.Env = append(os.Environ(), "FATHOMRY_BUILD_FIXTURE=hang")
		child.Stdout, child.Stderr = os.Stdout, os.Stderr
		if err := child.Start(); err != nil {
			os.Exit(3)
		}
		if err := os.WriteFile(os.Getenv("FATHOMRY_BUILD_PID"), []byte(strconv.Itoa(child.Process.Pid)), 0600); err != nil {
			os.Exit(4)
		}
	}
	os.Exit(0)
}

func TestProcessBudgetsAndCauses(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"ok", "exit", "stdout", "stderr", "hang", "orphan"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "orphan" && runtime.GOOS != "linux" {
				t.Skip("Linux /proc descendant-state control")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			pidFile := filepath.Join(t.TempDir(), "pid")
			environment := append(os.Environ(), "FATHOMRY_BUILD_FIXTURE="+mode, "FATHOMRY_BUILD_PID="+pidFile, "GORACE=atexit_sleep_ms=0")
			began := time.Now()
			raw, err := runProcess(ctx, t.TempDir(), environment, 1024, executable, "-test.run=^TestProcessFixture$")
			if time.Since(began) > 5*time.Second {
				t.Fatal("process ownership exceeded bound")
			}
			switch mode {
			case "ok":
				if err != nil || string(raw) != "ok" {
					t.Fatal(string(raw), err)
				}
			case "exit":
				var exited *exec.ExitError
				if !errors.As(err, &exited) || exited.ExitCode() != 9 || string(exited.Stderr) != "private-compiler-detail" {
					t.Fatal("native evidence lost", err)
				}
			case "stdout", "stderr":
				if !errors.Is(err, ErrLimit) {
					t.Fatal("output budget bypassed", err)
				}
			case "hang":
				if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrCanceled) {
					t.Fatal(err)
				}
			case "orphan":
				if err != nil {
					t.Fatal("group termination did not release inherited pipes", err)
				}
				pid, problem := os.ReadFile(pidFile)
				if problem != nil {
					t.Fatal(problem)
				}
				deadline := time.Now().Add(time.Second)
				for {
					stat, problem := os.ReadFile("/proc/" + string(pid) + "/stat")
					if errors.Is(problem, os.ErrNotExist) {
						break
					}
					if problem != nil {
						t.Fatal(problem)
					}
					tail := string(stat[strings.LastIndexByte(string(stat), ')')+1:])
					fields := strings.Fields(tail)
					if len(fields) != 0 && (fields[0] == "Z" || fields[0] == "X") {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("descendant still running after group termination", string(pid))
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
		})
	}
}

type cancelOnObservation struct {
	context.Context
	cancel context.CancelCauseFunc
	cause  error
}

func (ctx *cancelOnObservation) Err() error {
	ctx.cancel(ctx.cause)
	return ctx.Context.Err()
}

func TestProcessCancellationRetainsOutputFailure(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, stream := range []string{"stdout", "stderr"} {
		t.Run(stream, func(t *testing.T) {
			parent, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			cause := errors.New("caller canceled after output termination")
			ctx := &cancelOnObservation{Context: parent, cancel: cancel, cause: cause}
			environment := append(os.Environ(), "FATHOMRY_BUILD_FIXTURE="+stream, "GORACE=atexit_sleep_ms=0")
			_, err := runProcess(ctx, t.TempDir(), environment, 1024, executable, "-test.run=^TestProcessFixture$")
			var exited *exec.ExitError
			if !errors.Is(err, ErrCanceled) || !errors.Is(err, cause) || !errors.Is(err, ErrLimit) || !errors.As(err, &exited) {
				t.Fatal("cancellation discarded observed output or native exit evidence", err)
			}
		})
	}
}
