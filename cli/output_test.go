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
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/cli/internal/testdata/commandfamily"
	"github.com/spf13/cobra"
)

type failedWriter struct {
	err   error
	short bool
	calls int
}

func (writer *failedWriter) Write(data []byte) (int, error) {
	writer.calls++
	if writer.short {
		return len(data) - 1, writer.err
	}
	return 0, writer.err
}

func TestCheckedOutputAndSafeDiagnostics(t *testing.T) {
	for _, test := range []struct {
		name               string
		args               []string
		output, diagnostic *failedWriter
		cause              error
		status             int
	}{
		{"help-pipe", nil, &failedWriter{err: syscall.EPIPE}, nil, syscall.EPIPE, 1},
		{"help-short", nil, &failedWriter{short: true}, nil, io.ErrShortWrite, 1},
		{"usage-diagnostic", []string{"bad\x1b[31mSECRET"}, nil, &failedWriter{err: syscall.EPIPE}, syscall.EPIPE, 1},
		{"both-streams", nil, &failedWriter{err: io.ErrClosedPipe}, &failedWriter{err: syscall.EPIPE}, io.ErrClosedPipe, 1},
		{"output-cancellation", nil, &failedWriter{err: context.Canceled}, nil, context.Canceled, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output, diagnostic bytes.Buffer
			streams := Streams{strings.NewReader(""), &output, &diagnostic}
			if test.output != nil {
				streams.Stdout = test.output
			}
			if test.diagnostic != nil {
				streams.Stderr = test.diagnostic
			}
			status, err := Run(context.Background(), test.args, streams)
			if status != test.status || !errors.Is(err, test.cause) {
				t.Fatalf("%d %v", status, err)
			}
			if test.diagnostic != nil && test.diagnostic.calls != 1 {
				t.Fatalf("recursive diagnostics: %d", test.diagnostic.calls)
			}
			if test.output != nil && test.output.calls != 1 {
				t.Fatalf("retried output: %d", test.output.calls)
			}
			if test.name == "usage-diagnostic" {
				var usage usageError
				if !errors.As(err, &usage) {
					t.Fatal("lost invocation error")
				}
			}
			if test.name == "both-streams" && !errors.Is(err, syscall.EPIPE) {
				t.Fatal("lost diagnostic error")
			}
			if strings.Contains(diagnostic.String(), "SECRET") || strings.Contains(diagnostic.String(), "\x1b") {
				t.Fatal("unsafe diagnostic")
			}
		})
	}
}

func TestAcceptedEffectSurvivesIgnoredOutputFailure(t *testing.T) {
	cleanup := errors.New("cleanup")
	effect := filepath.Join(t.TempDir(), "accepted")
	backend := &commandfamily.Backend{EffectPath: effect, CleanupError: cleanup}
	output := &failedWriter{err: syscall.EPIPE}
	var diagnostics bytes.Buffer
	status, err := run(context.Background(), []string{"sample", "nested", "record", "--value=one"}, Streams{strings.NewReader(""), output, &diagnostics}, withFamily(backend))
	if status != 1 || !errors.Is(err, syscall.EPIPE) || !errors.Is(err, cleanup) {
		t.Fatalf("%d %v", status, err)
	}
	data, readErr := os.ReadFile(effect)
	if readErr != nil || string(data) != "one\n" || backend.Calls.Load() != 1 || backend.Closes.Load() != 1 {
		t.Fatalf("effect %q %v", data, readErr)
	}
	if strings.Contains(diagnostics.String(), "no effect") || !strings.Contains(diagnostics.String(), "may have occurred") {
		t.Fatalf("dishonest effect diagnostic: %q", diagnostics.String())
	}
}

func TestNativeHelpUsageAndIgnoredDiagnosticWrites(t *testing.T) {
	for _, mode := range []string{"help", "usage", "diagnostic"} {
		t.Run(mode, func(t *testing.T) {
			writer := &failedWriter{err: syscall.EPIPE}
			streams := Streams{strings.NewReader(""), writer, io.Discard}
			if mode == "diagnostic" {
				streams.Stdout = io.Discard
				streams.Stderr = writer
			}
			status, err := run(context.Background(), []string{"leaf"}, streams, func(words *text) *cobra.Command {
				root := commands(words)
				root.AddCommand(&cobra.Command{Use: "leaf", Args: cobra.NoArgs, RunE: func(command *cobra.Command, _ []string) error {
					switch mode {
					case "help":
						_ = command.Help()
					case "usage":
						_ = command.Usage()
					case "diagnostic":
						command.PrintErr("machine-progress\n")
					}
					return nil
				}})
				return root
			})
			if status != 1 || !errors.Is(err, syscall.EPIPE) || writer.calls != 1 {
				t.Fatalf("%d %v calls=%d", status, err, writer.calls)
			}
		})
	}
}

func TestContextCausePreserved(t *testing.T) {
	cause := errors.New("caller reason")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	status, err, _, _ := capture(ctx, nil, commands)
	if status != 1 || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
		t.Fatalf("%d %v", status, err)
	}
}

type gatedDiagnosticWriter struct {
	entered chan struct{}
	release chan struct{}
	err     error
	calls   int
}

func (writer *gatedDiagnosticWriter) Write(data []byte) (int, error) {
	writer.calls++
	if writer.calls == 1 {
		close(writer.entered)
	}
	<-writer.release
	if writer.err != nil {
		return 0, writer.err
	}
	return len(data), nil
}

func TestCancellationDuringDiagnosticRetained(t *testing.T) {
	callerCause := errors.New("caller cancellation reason")
	for _, test := range []struct {
		name              string
		cancel            bool
		cause, writeError error
		status            int
	}{
		{"no-cancellation", false, nil, nil, 2},
		{"caller-cancel", true, nil, nil, 1},
		{"caller-cause", true, callerCause, nil, 1},
		{"caller-cause-and-pipe", true, callerCause, syscall.EPIPE, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			writer := &gatedDiagnosticWriter{entered: make(chan struct{}), release: make(chan struct{}), err: test.writeError}
			var release sync.Once
			finished := make(chan struct{})
			var status int
			var err error
			go func() {
				defer close(finished)
				status, err = Run(ctx, []string{"--unknown"}, Streams{strings.NewReader(""), io.Discard, writer})
			}()
			t.Cleanup(func() {
				cancel(nil)
				release.Do(func() { close(writer.release) })
				select {
				case <-finished:
				case <-time.After(3 * time.Second):
					t.Error("diagnostic invocation did not join")
				}
			})
			select {
			case <-writer.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("diagnostic did not start")
			}
			if test.cancel {
				cancel(test.cause)
				<-ctx.Done()
			}
			release.Do(func() { close(writer.release) })
			select {
			case <-finished:
			case <-time.After(3 * time.Second):
				t.Fatal("diagnostic did not finish")
			}
			var usage usageError
			if status != test.status || writer.calls != 1 || !errors.As(err, &usage) {
				t.Errorf("status=%d error=%v writes=%d", status, err, writer.calls)
			}
			if errors.Is(err, context.Canceled) != test.cancel {
				t.Errorf("cancellation lost or invented: %v", err)
			}
			for _, cause := range []error{test.cause, test.writeError} {
				if cause != nil && !errors.Is(err, cause) {
					t.Errorf("cause %v lost in %v", cause, err)
				}
			}
		})
	}
}

type invalidCountWriter int

func (count invalidCountWriter) Write([]byte) (int, error) { return int(count), nil }

func TestImpossibleWriterCounts(t *testing.T) {
	for _, count := range []int{-1, 100} {
		writer := &checkedWriter{writer: invalidCountWriter(count)}
		written, err := writer.Write([]byte("x"))
		if written != 0 || !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("count=%d err=%v", written, err)
		}
	}
}
