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

package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/cli/internal/testdata/commandfamily"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type announcingWriter struct {
	writer io.Writer
	once   sync.Once
	ready  func()
}

func (writer *announcingWriter) Write(data []byte) (int, error) {
	writer.once.Do(writer.ready)
	return writer.writer.Write(data)
}

func TestProcessChild(t *testing.T) {
	mode := os.Getenv("FATHOMRY_CLI_CHILD")
	if mode == "" {
		return
	}
	directory := os.Getenv("FATHOMRY_CLI_SCRATCH")
	ready := os.NewFile(3, "ready")
	announce := func(message string) { _, _ = fmt.Fprintln(ready, message) }
	if mode == "ambient-flags" {
		pflag.CommandLine.Bool("ambient", false, "")
		status, err := Run(context.Background(), nil, Streams{os.Stdin, os.Stdout, os.Stderr})
		if status != 1 || !errors.Is(err, errDefinition) {
			os.Exit(99)
		}
		os.Exit(status)
	}
	execute := func(ctx context.Context, _ []string, streams Streams) (int, error) {
		backend := &commandfamily.Backend{EffectPath: filepath.Join(directory, "effect"), CleanupPath: filepath.Join(directory, "cleanup")}
		switch mode {
		case "wait", "mixed-operation", "mixed-cleanup", "term-epipe", "neutral-int":
			backend.Wait = true
			backend.AfterEffect = func() { announce("ready") }
		case "blocked-out":
			streams.Stdout = &announcingWriter{writer: streams.Stdout, ready: func() { announce("ready") }}
		case "blocked-err":
			backend.OperationError = errors.New("operation")
			streams.Stderr = &announcingWriter{writer: streams.Stderr, ready: func() { announce("ready") }}
		}
		if mode == "mixed-operation" {
			backend.OperationError = errors.New("independent operation")
		}
		if mode == "mixed-cleanup" {
			backend.CleanupError = errors.New("independent cleanup")
		}
		if strings.HasPrefix(mode, "blocked-") {
			done := make(chan struct{})
			joined := make(chan struct{})
			go func() {
				defer close(joined)
				select {
				case <-ctx.Done():
					announce("canceled")
				case <-done:
				}
			}()
			defer func() { close(done); <-joined }()
		}
		construct := withFamily(backend)
		if mode == "term-epipe" {
			base := construct
			construct = func(words *text) *cobra.Command {
				root := base(words)
				record, _, err := root.Find([]string{"sample", "nested", "record"})
				if err != nil {
					panic(err)
				}
				operation := record.RunE
				record.RunE = func(command *cobra.Command, args []string) error {
					err := operation(command, args)
					command.Print("tail\n")
					return err
				}
				return root
			}
		}
		return run(ctx, []string{"sample", "nested", "record", "--value=accepted"}, streams, construct)
	}
	streams := Streams{os.Stdin, os.Stdout, os.Stderr}
	status := 0
	if strings.HasPrefix(mode, "neutral-") {
		status, _ = execute(context.Background(), nil, streams)
	} else {
		status = process(nil, streams, execute)
	}
	_ = os.WriteFile(filepath.Join(directory, "returned"), []byte("returned\n"), 0600)
	os.Exit(status)
}

type processChild struct {
	command   *exec.Cmd
	done      chan struct{}
	err       error
	ready     *os.File
	messages  *bufio.Reader
	directory string
}

func startProcessChild(t *testing.T, mode string, stdout, stderr io.Writer) *processChild {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	child := &processChild{done: make(chan struct{}), ready: reader, messages: bufio.NewReader(reader), directory: t.TempDir()}
	child.command = exec.Command(executable, "-test.run=^TestProcessChild$")
	child.command.Env = append(os.Environ(), "FATHOMRY_CLI_CHILD="+mode, "FATHOMRY_CLI_SCRATCH="+child.directory)
	child.command.Stdout = stdout
	child.command.Stderr = stderr
	child.command.ExtraFiles = []*os.File{writer}
	if err := child.command.Start(); err != nil {
		reader.Close()
		writer.Close()
		t.Fatal(err)
	}
	_ = writer.Close()
	go func() { child.err = child.command.Wait(); close(child.done) }()
	t.Cleanup(func() { _ = child.command.Process.Kill(); <-child.done; _ = reader.Close() })
	return child
}

func (child *processChild) message(t *testing.T, want string) {
	t.Helper()
	if err := child.ready.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	message, err := child.messages.ReadString('\n')
	if err != nil || message != want+"\n" {
		t.Fatalf("message got=%q want=%q err=%v", message, want, err)
	}
}

func (child *processChild) signal(t *testing.T, signal os.Signal) {
	t.Helper()
	if err := child.command.Process.Signal(signal); err != nil {
		t.Fatal(err)
	}
}

func (child *processChild) wait(t *testing.T, want int) {
	t.Helper()
	select {
	case <-child.done:
	case <-time.After(8 * time.Second):
		t.Fatal("child did not terminate")
	}
	status := child.command.ProcessState.ExitCode()
	if status != want {
		t.Fatalf("exit=%d want=%d error=%v", status, want, child.err)
	}
	if state := child.command.ProcessState.Sys().(syscall.WaitStatus); state.Signaled() {
		t.Fatalf("unexpected signal exit: %v", state)
	}
}

func (child *processChild) file(t *testing.T, name string, present bool) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(child.directory, name))
	if present {
		if err != nil {
			t.Fatal(err)
		}
		if name == "effect" && string(data) != "accepted\n" {
			t.Fatalf("effect not exactly once: %q", data)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected %s: %q %v", name, data, err)
	}
}

func closedPipe(t *testing.T) *os.File {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	t.Cleanup(func() { _ = writer.Close() })
	return writer
}

func fullPipe(t *testing.T) *os.File {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
	descriptor := int(writer.Fd())
	if err := syscall.SetNonblock(descriptor, true); err != nil {
		t.Fatal(err)
	}
	for {
		_, err := syscall.Write(descriptor, make([]byte, 4096))
		if errors.Is(err, syscall.EAGAIN) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := syscall.SetNonblock(descriptor, false); err != nil {
		t.Fatal(err)
	}
	return writer
}

func TestProcessSignalsAndMixedFailures(t *testing.T) {
	for _, test := range []struct {
		name, mode string
		signal     os.Signal
		status     int
	}{
		{"int", "wait", os.Interrupt, 130},
		{"term", "wait", syscall.SIGTERM, 143},
		{"term-operation", "mixed-operation", syscall.SIGTERM, 1},
		{"term-cleanup", "mixed-cleanup", syscall.SIGTERM, 1},
		{"term-epipe", "term-epipe", syscall.SIGTERM, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout io.Writer = io.Discard
			if test.mode == "term-epipe" {
				stdout = closedPipe(t)
			}
			child := startProcessChild(t, test.mode, stdout, io.Discard)
			child.message(t, "ready")
			child.file(t, "effect", true)
			child.signal(t, test.signal)
			child.wait(t, test.status)
			child.file(t, "effect", true)
			child.file(t, "cleanup", true)
			child.file(t, "returned", true)
		})
	}
}

func TestRealDescriptorsSIGPIPE(t *testing.T) {
	for _, descriptor := range []string{"stdout", "stderr"} {
		t.Run(descriptor, func(t *testing.T) {
			var stdout, stderr io.Writer = io.Discard, io.Discard
			mode := "finite"
			if descriptor == "stdout" {
				stdout = closedPipe(t)
			} else {
				stderr = closedPipe(t)
				mode = "blocked-err"
			}
			child := startProcessChild(t, mode, stdout, stderr)
			child.wait(t, 1)
			child.file(t, "effect", true)
			child.file(t, "cleanup", true)
			child.file(t, "returned", true)
		})
	}
	binary := buildBinary(t)
	for _, args := range [][]string{nil, {"--invalid"}} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		command := exec.CommandContext(ctx, binary, args...)
		command.Stdout = closedPipe(t)
		command.Stderr = closedPipe(t)
		err := command.Run()
		cancel()
		if err == nil || command.ProcessState.ExitCode() != 1 || command.ProcessState.Sys().(syscall.WaitStatus).Signaled() {
			t.Fatalf("actual binary fd pipe: %v %v", err, command.ProcessState)
		}
	}
}

func TestBlockedDescriptorsRequireLaterForce(t *testing.T) {
	for _, test := range []struct {
		mode          string
		first, second os.Signal
		status        int
		cleaned       bool
	}{
		{"blocked-out", os.Interrupt, syscall.SIGTERM, 143, false},
		{"blocked-err", syscall.SIGTERM, os.Interrupt, 130, true},
	} {
		t.Run(test.mode, func(t *testing.T) {
			var stdout, stderr io.Writer = io.Discard, io.Discard
			if test.mode == "blocked-out" {
				stdout = fullPipe(t)
			} else {
				stderr = fullPipe(t)
			}
			child := startProcessChild(t, test.mode, stdout, stderr)
			child.message(t, "ready")
			child.file(t, "effect", true)
			child.signal(t, test.first)
			child.message(t, "canceled")
			select {
			case <-child.done:
				t.Fatal("first signal falsely interrupted blocked I/O")
			default:
			}
			child.file(t, "returned", false)
			child.signal(t, test.second)
			child.wait(t, test.status)
			child.file(t, "cleanup", test.cleaned)
			child.file(t, "returned", false)
		})
	}
}

func TestRunDoesNotOwnProcessSignals(t *testing.T) {
	for _, mode := range []string{"neutral-int", "neutral-pipe"} {
		t.Run(mode, func(t *testing.T) {
			var stdout io.Writer = io.Discard
			want := syscall.SIGINT
			if mode == "neutral-pipe" {
				stdout = closedPipe(t)
				want = syscall.SIGPIPE
			}
			child := startProcessChild(t, mode, stdout, io.Discard)
			if mode == "neutral-int" {
				child.message(t, "ready")
				child.signal(t, os.Interrupt)
			}
			select {
			case <-child.done:
			case <-time.After(5 * time.Second):
				t.Fatal("signal-neutral child did not terminate")
			}
			state := child.command.ProcessState.Sys().(syscall.WaitStatus)
			if !state.Signaled() || state.Signal() != want {
				t.Fatalf("Run changed signal policy: %v %v", state, child.err)
			}
			child.file(t, "effect", true)
			child.file(t, "cleanup", false)
			child.file(t, "returned", false)
		})
	}
}

func TestAmbientParserGlobalRefused(t *testing.T) {
	child := startProcessChild(t, "ambient-flags", io.Discard, io.Discard)
	child.wait(t, 1)
}
