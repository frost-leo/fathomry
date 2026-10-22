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

package vet

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/failure/v1"
	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/spf13/cobra"
)

func catalogs(t testing.TB) command.Catalogs {
	t.Helper()
	components := append(i18n.CoreComponents(), command.Component(), wf.Component(), Component())
	var definitions []failure.Definition
	for _, component := range components {
		definitions = append(definitions, component.Definitions...)
	}
	atlas, err := failure.Prepare(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := i18n.Prepare(components...)
	if err != nil {
		t.Fatal(err)
	}
	return command.Catalogs{Errors: atlas, Messages: messages}
}

func invoke(t testing.TB, ctx context.Context, args []string, writer io.Writer) (string, error) {
	t.Helper()
	var diagnostics bytes.Buffer
	err := command.Run(ctx, args, command.Options{Input: strings.NewReader(""), Output: writer, ErrorOutput: &diagnostics}, catalogs(t), func(invocation *command.Invocation) *cobra.Command {
		root := invocation.Group("fathomry", "fathomry.command_line.root")
		root.AddCommand(New(invocation))
		return root
	})
	return diagnostics.String(), err
}

func fixture(t testing.TB, changed bool) string {
	t.Helper()
	input := health.Declarations()
	if changed {
		input.Workflows[0].Modes[0].Values = map[string]wf.Decimal{"attempt_timeout": "1200"}
	}
	bundle, err := wf.Compile(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := bundle.BytesCopy()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "workflow.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestVetCommand(t *testing.T) {
	first, changed := fixture(t, false), fixture(t, true)
	bad := filepath.Join(t.TempDir(), "invalid.json")
	if err := os.WriteFile(bad, []byte(`{"secret":"private-payload-canary"`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name              string
		args              []string
		completed, passed bool
	}{
		{"valid", []string{first}, true, true},
		{"equal", []string{first, first}, true, true},
		{"conflict", []string{first, changed}, true, false},
		{"invalid", []string{bad}, true, false},
		{"mixed", []string{first, bad}, true, false},
		{"missing", []string{first, filepath.Join(t.TempDir(), "private-path-canary")}, false, false},
		{"directory", []string{t.TempDir()}, false, false},
		{"empty", nil, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			args := append([]string{"vet"}, test.args...)
			args = append(args, "--output", "json")
			diagnostics, err := invoke(t, context.Background(), args, &output)
			if strings.Contains(diagnostics, "canary") || strings.Contains(output.String(), "canary") {
				t.Fatal("payload/native path leaked")
			}
			if !test.completed {
				if err == nil || output.Len() != 0 || errors.Is(err, command.ErrCheck) {
					t.Fatal("acquisition claimed completed", err, output.String())
				}
				return
			}
			if test.passed && err != nil || !test.passed && !errors.Is(err, command.ErrCheck) {
				t.Fatal(err, diagnostics)
			}
			var envelope struct {
				Schema  string
				Command string
				Data    wf.Report
			}
			if err := json.Unmarshal(output.Bytes(), &envelope); err != nil {
				t.Fatal(err)
			}
			if envelope.Command != "workflow.vet" || envelope.Schema != command.Schema || envelope.Data.Passed != test.passed || envelope.Data.Inputs != len(test.args) {
				t.Fatal(envelope)
			}
			if test.name == "mixed" && (envelope.Data.Workflows != 1 || len(envelope.Data.Diagnostics) != 1) {
				t.Fatal("mixed input accounting", envelope.Data)
			}
		})
	}
	for _, locale := range []string{"en", "zh-CN", "fr"} {
		for _, args := range [][]string{{"vet", "--help"}, {"vet", first}, {"vet", bad}} {
			var output bytes.Buffer
			_, err := invoke(t, context.Background(), append(args, "--lang", locale), &output)
			if err != nil && !errors.Is(err, command.ErrCheck) || output.Len() == 0 {
				t.Fatal(locale, args, err)
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var output bytes.Buffer
	_, err := invoke(t, ctx, []string{"vet", first}, &output)
	if !errors.Is(err, command.ErrCanceled) || output.Len() != 0 {
		t.Fatal(err)
	}
	_, err = invoke(t, context.Background(), []string{"vet", first}, brokenOutput{})
	if !errors.Is(err, command.ErrOutput) || errors.Is(err, command.ErrCheck) {
		t.Fatal("output failure became check", err)
	}
}

type brokenOutput struct{}

func (brokenOutput) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

type faultyFile struct {
	*os.File
	readErr, closeErr error
	cancel            context.CancelFunc
	closed            *bool
	closes            *int
}

func (file faultyFile) Read(value []byte) (int, error) {
	if file.cancel != nil {
		file.cancel()
	}
	if file.readErr != nil {
		return 0, file.readErr
	}
	return file.File.Read(value)
}
func (file faultyFile) Close() error {
	*file.closed = true
	if file.closes != nil {
		*file.closes += 1
	}
	return errors.Join(file.File.Close(), file.closeErr)
}

func TestInputOwnership(t *testing.T) {
	path := fixture(t, false)
	sentinel := errors.New("private-io-canary")
	for _, mode := range []string{"read", "close", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			closed := false
			raw, err := readFile(ctx, path, wf.MaxBundleBytes, func(path string) (inputFile, error) {
				file, err := os.Open(path)
				if err != nil {
					return nil, err
				}
				fault := faultyFile{File: file, closed: &closed}
				switch mode {
				case "read":
					fault.readErr = sentinel
				case "close":
					fault.closeErr = sentinel
				case "cancel":
					fault.cancel = cancel
				}
				return fault, nil
			})
			if err == nil || raw != nil || !closed {
				t.Fatal("owned file escaped", closed, err)
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if mode != "cancel" && !errors.Is(err, sentinel) {
				t.Fatal("cause lost", err)
			}
			if strings.Contains(err.Error(), "canary") {
				t.Fatal("native error leaked")
			}
		})
	}
	if _, err := readInput(context.Background(), path, 1); !errors.Is(err, command.ErrLimit) {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "selected.json")
	if err := os.Symlink(path, link); err == nil {
		if _, err := readInput(context.Background(), link, wf.MaxBundleBytes); err != nil {
			t.Fatal("selected regular symlink failed", err)
		}
	}
}

func TestAggregateInputBound(t *testing.T) {
	raw := bytes.Repeat([]byte{' '}, wf.MaxBundleBytes)
	reads := 0
	_, err := inspect(context.Background(), []string{"1", "2", "3", "4", "5"}, func(_ context.Context, _ string, maximum int) ([]byte, error) {
		reads++
		if len(raw) > maximum {
			return nil, command.Fail(command.ErrLimit)
		}
		return raw, nil
	})
	if !errors.Is(err, command.ErrLimit) || reads != 5 {
		t.Fatal(reads, err)
	}
}

func TestSimultaneousInputFailures(t *testing.T) {
	path := fixture(t, false)
	readCause := errors.New("private-read-canary")
	cancelCause := errors.New("private-cancel-canary")
	closeCause := errors.New("private-close-canary")
	for _, closeFails := range []bool{false, true} {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		closed, closes := false, 0
		raw, err := readFile(ctx, path, wf.MaxBundleBytes, func(path string) (inputFile, error) {
			file, err := os.Open(path)
			if err != nil {
				return nil, err
			}
			fault := faultyFile{File: file, closed: &closed, closes: &closes,
				readErr: &os.PathError{Op: "read", Path: "private-path-canary", Err: readCause},
				cancel:  func() { cancel(cancelCause) },
			}
			if closeFails {
				fault.closeErr = closeCause
			}
			return fault, nil
		})
		code, exit := command.ErrCanceled, 130
		if closeFails {
			code, exit = command.ErrCleanup, 1
		}
		core, found := failure.Inspect(err)
		if raw != nil || !closed || closes != 1 || !found || core.Diagnostic().Definition.Code != code || command.ExitCode(err) != exit {
			t.Fatal("incorrect ownership or failure precedence", err, closes)
		}
		for _, cause := range []error{context.Canceled, cancelCause, readCause} {
			if !errors.Is(err, cause) {
				t.Fatal("simultaneous input failure lost a cause", closeFails)
			}
		}
		var native *os.PathError
		if !errors.As(err, &native) || native.Err != readCause || closeFails && !errors.Is(err, closeCause) {
			t.Fatal("native read or cleanup evidence lost", closeFails)
		}
		if strings.Contains(err.Error(), "canary") {
			t.Fatal("native input failure leaked")
		}
	}
}

type boundaryCancellation struct {
	context.Context
	calls    atomic.Int64
	cancelAt int64
	frame    string
	cancel   context.CancelCauseFunc
	cause    error
}

func (ctx *boundaryCancellation) Err() error {
	if ctx.calls.Add(1) == ctx.cancelAt {
		ctx.cancel(ctx.cause)
	}
	if ctx.frame != "" {
		var callers [24]uintptr
		frames := runtime.CallersFrames(callers[:runtime.Callers(2, callers[:])])
		for {
			frame, more := frames.Next()
			if frame.Function == ctx.frame {
				ctx.cancel(ctx.cause)
				break
			}
			if !more {
				break
			}
		}
	}
	return ctx.Context.Err()
}

func newBoundaryContext(t *testing.T, cause error) *boundaryCancellation {
	t.Helper()
	parent, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })
	return &boundaryCancellation{Context: parent, cancel: cancel, cause: cause}
}

func TestCancellationRetainsWorkflowFailures(t *testing.T) {
	valid, err := os.ReadFile(fixture(t, false))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name       string
		raw        []byte
		checkEntry bool
		nativeJSON bool
	}{
		{"native_parse", []byte(`{"private-value-canary":]}`), false, true},
		{"unsupported_profile", []byte(`{"format":"unsupported","workflows":[]}`), false, false},
		{"check_entry", valid, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			cause := errors.New("private-cancellation-canary")
			control := newBoundaryContext(t, cause)
			bundle, baseline := wf.Parse(control, test.raw)
			if test.checkEntry {
				if baseline != nil || bundle == nil {
					t.Fatal("valid parse control failed", baseline)
				}
				canceled := newBoundaryContext(t, cause)
				canceled.cancel(cause)
				_, baseline = wf.Check(canceled, bundle)
			} else if bundle != nil {
				t.Fatal("invalid parse control succeeded")
			}
			lower, found := failure.Inspect(baseline)
			if !found || lower.Diagnostic().Definition.Component != "workflow" {
				t.Fatal("control lacks a workflow occurrence", baseline)
			}
			var native *jsontext.SyntacticError
			if test.nativeJSON && !errors.As(baseline, &native) {
				t.Fatal("control lacks a native parsing error", baseline)
			}
			ctx := newBoundaryContext(t, cause)
			// Early failures occur before serialization. Valid serialization can
			// vary its polling count, so Check uses a function-entry trigger.
			if test.checkEntry {
				ctx.frame = runtime.FuncForPC(reflect.ValueOf(wf.Check).Pointer()).Name()
			} else {
				ctx.cancelAt = control.calls.Load() + 1
			}
			report, err := inspect(ctx, []string{"synthetic"}, func(context.Context, string, int) ([]byte, error) {
				return test.raw, nil
			})
			core, found := failure.Inspect(err)
			if !found || core.Diagnostic().Definition.Code != command.ErrCanceled || command.ExitCode(err) != 130 || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
				t.Fatal("incorrect cancellation policy", err)
			}
			if report.Inputs != 0 || report.Passed || len(report.Diagnostics) != 0 || len(report.Entries) != 0 {
				t.Fatal("cancellation returned a completed report", report)
			}
			if !errors.Is(err, lower.Diagnostic().Definition.Code) || test.nativeJSON && !errors.As(err, &native) {
				t.Fatal("cancellation discarded a lower-layer failure", test.name)
			}
			if _, completed := wf.Diagnose(err); completed || strings.Contains(err.Error(), "canary") {
				t.Fatal("retained failure changed completion or presentation", err)
			}
		})
	}
}
