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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/cli/internal/testdata/commandfamily"
	"github.com/spf13/cobra"
)

func withFamily(backend *commandfamily.Backend) func(*text) *cobra.Command {
	return func(words *text) *cobra.Command {
		root := commands(words)
		family := commandfamily.New(backend)
		root.AddCommand(family)
		return root
	}
}

func capture(ctx context.Context, args []string, construct func(*text) *cobra.Command) (int, error, string, string) {
	var output, diagnostics bytes.Buffer
	status, err := run(ctx, args, Streams{strings.NewReader(""), &output, &diagnostics}, construct)
	return status, err, output.String(), diagnostics.String()
}

func TestFirstPartyCompositionAndFamilyLocalLeaf(t *testing.T) {
	backend := &commandfamily.Backend{}
	args := []string{"sample", "nested", "record", "--value", "one"}
	status, err, _, _ := capture(context.Background(), args, commands)
	if status != 2 || err == nil {
		t.Fatalf("omitted: %d %v", status, err)
	}
	status, err, output, _ := capture(context.Background(), args, withFamily(backend))
	if status != 0 || err != nil || output != "{\"accepted\":true}\n" {
		t.Fatalf("included: %d %v %q", status, err, output)
	}
	if backend.Opens.Load() != 1 || backend.Calls.Load() != 1 || backend.Closes.Load() != 1 {
		t.Fatal("wrong operation lifecycle")
	}
	status, err, output, _ = capture(context.Background(), []string{"sample", "second"}, withFamily(backend))
	if status != 0 || err != nil || output != "{\"leaf\":2}\n" {
		t.Fatalf("family-local leaf: %d %v %q", status, err, output)
	}
}

func TestHelpAndInvalidInputsAreOffline(t *testing.T) {
	cases := []struct {
		args   []string
		status int
	}{
		{nil, 0}, {[]string{"-h"}, 0}, {[]string{"help"}, 0},
		{[]string{"help", "sample", "nested", "record"}, 0},
		{[]string{"help", "s", "nested", "record"}, 0},
		{[]string{"sample", "nested", "record", "--help"}, 0},
		{[]string{"sample", "nested", "record", "bad", "--help"}, 0},
		{[]string{"sample", "nested", "record", "--left", "--right", "--help"}, 0},
		{[]string{"--help", "sample", "nested"}, 0},
		{[]string{"missing", "--help"}, 2}, {[]string{"help", "missing"}, 2},
		{[]string{"help", "sample", "nested", "record", "extra"}, 2},
		{[]string{"sample", "missing", "--help"}, 2},
		{[]string{"sample", "nested", "record"}, 2},
		{[]string{"sample", "nested", "record", "--value"}, 2},
		{[]string{"sample", "nested", "record", "--value=x", "--left", "--right"}, 2},
		{[]string{"sample", "nested", "record", "--value=x", "extra"}, 2},
		{[]string{"sample", "nested", "record", "--unknown", "--help"}, 2},
		{[]string{"--", "--help"}, 2}, {[]string{"help", "--", "--lang", "zh-CN"}, 2},
		{[]string{"completion"}, 2}, {[]string{"__complete"}, 2},
		{[]string{"__completeNoDesc"}, 2}, {[]string{"help", "__complete"}, 2},
		{[]string{"__complete", "--help"}, 2}, {[]string{"sample", "__complete", "--help"}, 2},
	}
	for _, test := range cases {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			backend := &commandfamily.Backend{}
			var output, diagnostics bytes.Buffer
			status, err := run(context.Background(), test.args, Streams{poisonReader{}, &output, &diagnostics}, withFamily(backend))
			if status != test.status || (err == nil) != (status == 0) {
				t.Fatalf("status=%d err=%v out=%q diag=%q", status, err, output.String(), diagnostics.String())
			}
			if backend.Opens.Load()+backend.Closes.Load()+backend.Calls.Load() != 0 {
				t.Fatal("offline path acquired capability")
			}
			if test.status == 0 && backend.Validations.Load() != 0 {
				t.Fatal("help ran semantic validator")
			}
		})
	}
	backend := &commandfamily.Backend{}
	_ = withFamily(backend)(newText())
	if backend.Opens.Load()+backend.Calls.Load()+backend.Closes.Load()+backend.Validations.Load() != 0 {
		t.Fatal("constructor executed business work")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	constructed := false
	status, err, _, _ := capture(ctx, []string{"sample", "nested", "record", "--value=x"}, func(words *text) *cobra.Command { constructed = true; return withFamily(backend)(words) })
	if status != 130 || !errors.Is(err, context.Canceled) || constructed {
		t.Fatalf("pre-cancel: %d %v constructed=%v", status, err, constructed)
	}
}

type poisonReader struct{}

func (poisonReader) Read([]byte) (int, error) { panic("unexpected stdin read") }

type borrowedBuffer struct {
	bytes.Buffer
	closed  bool
	flushed bool
}

func (buffer *borrowedBuffer) Close() error { buffer.closed = true; return nil }
func (buffer *borrowedBuffer) Flush() error { buffer.flushed = true; return nil }

func TestExplicitInputsAndBorrowedStreams(t *testing.T) {
	previous := os.Args
	os.Args = []string{"poison", "missing", "--unknown"}
	defer func() { os.Args = previous }()
	for _, args := range [][]string{nil, {}} {
		var input, output, diagnostics borrowedBuffer
		status, err := Run(context.Background(), args, Streams{&input, &output, &diagnostics})
		if status != 0 || err != nil || input.closed || output.closed || diagnostics.closed || output.flushed || diagnostics.flushed {
			t.Fatalf("%d %v", status, err)
		}
	}
	var typedNil *bytes.Buffer
	good := Streams{strings.NewReader(""), io.Discard, io.Discard}
	for _, streams := range []Streams{{}, {nil, io.Discard, io.Discard}, {typedNil, io.Discard, io.Discard}, {good.Stdin, nil, io.Discard}, {good.Stdin, typedNil, io.Discard}, {good.Stdin, io.Discard, nil}} {
		if status, err := Run(context.Background(), nil, streams); status != 1 || !errors.Is(err, errInputs) {
			t.Fatalf("invalid streams: %d %v", status, err)
		}
	}
	if status, err := Run(nil, nil, good); status != 1 || !errors.Is(err, errInputs) {
		t.Fatalf("nil context: %d %v", status, err)
	}
}

func TestRawAndIncrementalOutput(t *testing.T) {
	backend := &commandfamily.Backend{}
	var output, diagnostics bytes.Buffer
	payload := []byte{0, 1, 2, 255, '\n'}
	status, err := run(context.Background(), []string{"sample", "raw", "--", "--lang", "zh-CN"}, Streams{bytes.NewReader(payload), &output, &diagnostics}, withFamily(backend))
	if status != 0 || err != nil || !bytes.Equal(output.Bytes(), payload) {
		t.Fatalf("raw: %d %v %q", status, err, output.Bytes())
	}
	gate := make(chan struct{})
	backend = &commandfamily.Backend{Started: make(chan struct{}), Continue: gate}
	reader, writer := io.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	defer reader.Close()
	done := make(chan error, 1)
	go func() {
		status, err := run(ctx, []string{"sample", "stream"}, Streams{strings.NewReader(""), writer, io.Discard}, withFamily(backend))
		if status != 0 {
			err = fmt.Errorf("status %d: %w", status, err)
		}
		_ = writer.CloseWithError(err)
		done <- err
	}()
	prefix := make([]byte, len("prefix\n"))
	if _, err := io.ReadFull(reader, prefix); err != nil {
		t.Fatal(err)
	}
	if string(prefix) != "prefix\n" {
		t.Fatalf("prefix %q", prefix)
	}
	select {
	case <-backend.Started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-done:
		t.Fatalf("finished before gate: %v", err)
	default:
	}
	close(gate)
	suffix, err := io.ReadAll(reader)
	if err != nil || string(suffix) != "suffix\n" {
		t.Fatalf("suffix %q %v", suffix, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestOperationCleanupAndCancellationCauses(t *testing.T) {
	operation := errors.New("operation-CANARY\x1b[31m")
	cleanup := errors.New("cleanup-CANARY")
	acquisition := errors.New("acquisition-CANARY")
	for _, test := range []struct {
		name                        string
		acquire, operation, cleanup error
		status                      int
	}{
		{"operation", nil, operation, nil, 1},
		{"cleanup", nil, nil, cleanup, 1},
		{"partial", acquisition, nil, cleanup, 1},
		{"cancel", nil, context.Canceled, nil, 130},
		{"mixed-operation", nil, errors.Join(context.Canceled, operation), nil, 1},
		{"mixed-cleanup", nil, context.Canceled, cleanup, 1},
		{"deadline", nil, context.DeadlineExceeded, nil, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &commandfamily.Backend{AcquireError: test.acquire, OperationError: test.operation, CleanupError: test.cleanup}
			status, err, _, diagnostic := capture(context.Background(), []string{"sample", "nested", "record", "--value=x"}, withFamily(backend))
			if status != test.status {
				t.Fatalf("%d %v", status, err)
			}
			for _, cause := range []error{test.acquire, test.operation, test.cleanup} {
				if cause != nil && !errors.Is(err, cause) {
					t.Fatalf("lost %v in %v", cause, err)
				}
			}
			if backend.Opens.Load() != 1 || backend.Closes.Load() != 1 || test.acquire != nil && backend.Calls.Load() != 0 {
				t.Fatal("incorrect cleanup")
			}
			if strings.Contains(diagnostic, "CANARY") || strings.Contains(diagnostic, "\x1b") {
				t.Fatalf("unsafe diagnostic: %q", diagnostic)
			}
		})
	}
}

func TestAcceptedEffectBeforeCancellationAndCleanupJoin(t *testing.T) {
	cleanupGate := make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(cleanupGate) })
	started := make(chan struct{})
	effect := filepath.Join(t.TempDir(), "effect")
	backend := &commandfamily.Backend{EffectPath: effect, Started: started, Wait: true, CleanupGate: cleanupGate}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		status int
		err    error
	}
	done := make(chan result, 1)
	go func() {
		status, err, _, _ := capture(ctx, []string{"sample", "nested", "record", "--value=accepted"}, withFamily(backend))
		done <- result{status, err}
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("operation not started")
	}
	cancel()
	select {
	case <-done:
		t.Fatal("returned before cleanup joined")
	default:
	}
	data, err := os.ReadFile(effect)
	if err != nil || string(data) != "accepted\n" {
		t.Fatalf("effect %q %v", data, err)
	}
	release.Do(func() { close(cleanupGate) })
	select {
	case result := <-done:
		if result.status != 130 || !errors.Is(result.err, context.Canceled) {
			t.Fatalf("%d %v", result.status, result.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup did not finish")
	}
	if backend.Calls.Load() != 1 || backend.Closes.Load() != 1 {
		t.Fatal("effect retried or cleanup lost")
	}
}

func TestInvocationIsolation(t *testing.T) {
	var wait sync.WaitGroup
	for index := 0; index < 40; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			language := "en"
			expected := "Usage"
			if index%2 == 1 {
				language = "zh-CN"
				expected = "用法"
			}
			backend := &commandfamily.Backend{}
			status, err, output, _ := capture(context.Background(), []string{"help", "sample", "--lang", language}, withFamily(backend))
			if status != 0 || err != nil || !strings.Contains(output, expected) || backend.Opens.Load() != 0 {
				t.Errorf("isolation: %d %v %q", status, err, output)
			}
		}()
	}
	wait.Wait()
}

func FuzzExplicitRun(f *testing.F) {
	f.Add("new\x00target\x00--module=example.org/app\x00--fathomry-source=..")
	for _, seed := range []string{"", "help", "--lang\x00zh-CN\x00--bad", "--bad\x00--lang\x00zh-CN", "--\x00--lang=zh-CN", "missing\x00--help", "--help=UNTRUSTED-CANARY\x1b[31m", "-htest.unknown=x"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 2048 {
			t.Skip()
		}
		args := strings.Split(input, "\x00")
		original := slices.Clone(args)
		status, err, output, diagnostic := capture(context.Background(), args, func(words *text) *cobra.Command {
			// Keep arbitrary fuzz argv on the original metadata-only root/help surface.
			root := commands(words)
			root.RemoveCommand(root.Commands()...)
			return root
		})
		if status != 0 && status != 2 || (err == nil) != (status == 0) {
			t.Fatalf("unexpected outcome: %d %v", status, err)
		}
		if !slices.Equal(args, original) {
			t.Fatal("caller arguments mutated")
		}
		if strings.Contains(output+diagnostic, "UNTRUSTED-CANARY") || strings.ContainsAny(output+diagnostic, "\x1b\r") {
			t.Fatal("unsafe presentation")
		}
	})
}
