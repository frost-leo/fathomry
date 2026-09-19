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

package cli_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/cli"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/cli/testdata/query"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/root"
	"github.com/frost-leo/fathomry/failure"
	"github.com/spf13/cobra"
)

func execute(args ...string) (int, string, string) {
	var out, diagnostic bytes.Buffer
	status := root.Run(context.Background(), args, strings.NewReader(""), &out, &diagnostic)
	return status, out.String(), diagnostic.String()
}

func TestSeparateCommandPackageAndLazyOperation(t *testing.T) {
	opened, closed := 0, 0
	options := query.Options{
		Open: func(ctx context.Context) (func(), error) {
			if ctx == nil {
				t.Fatal("caller Context not propagated")
			}
			opened++
			return func() { closed++ }, nil
		},
		Bytes: 20 << 10, Fail: true,
	}
	build := func(call *cli.Invocation) *cobra.Command {
		command := root.New(call)
		call.Add(command, query.New(call, options), query.GroupHelpID, query.GroupSummaryID, query.Resources()...)
		return command
	}
	for _, test := range []struct {
		args []string
		want string
		exit int
	}{
		{[]string{"--help"}, "query    Test-only query group", 0},
		{[]string{"help", "query", "run"}, "Nested query help.", 0},
		{[]string{"help", "query"}, "run    Run a test-only query", 0},
		{[]string{"query", "--help"}, "run    Run a test-only query", 0},
		{[]string{"--lang=zh-CN", "help", "query", "run"}, "嵌套查询帮助。", 0},
		{[]string{"--lang=zh-CN", "help", "query"}, "run    运行仅用于测试的查询", 0},
		{[]string{"query", "run", "--count=0"}, "", 2},
		{[]string{"query", "run", "--unknown"}, "", 2},
		{[]string{"query", "missing", "--help"}, "", 2},
	} {
		var out, diagnostic bytes.Buffer
		status := cli.Run(context.Background(), test.args, strings.NewReader(""), &out, &diagnostic, build)
		if status != test.exit || !strings.Contains(out.String(), test.want) ||
			test.exit == 0 && diagnostic.Len() != 0 || test.exit != 0 && out.Len() != 0 {
			t.Fatalf("args=%q status=%d stdout=%q stderr=%q", test.args, status, out.String(), diagnostic.String())
		}
		if opened != 0 || closed != 0 {
			t.Fatal("construction, help or syntax failure opened a capability")
		}
	}
	var out, diagnostic bytes.Buffer
	status := cli.Run(context.Background(), []string{"query", "run"}, strings.NewReader(""), &out, &diagnostic, build)
	if status != 1 || out.Len() != 20<<10 || !strings.Contains(diagnostic.String(), "Test query failed") || opened != 1 || closed != 1 {
		t.Fatalf("operation status=%d stdout-bytes=%d stderr=%q open=%d close=%d", status, out.Len(), diagnostic.String(), opened, closed)
	}
	options.Fail, options.IgnoreWriteError = false, true
	opened, closed = 0, 0
	var failureDiagnostic bytes.Buffer
	status = cli.Run(context.Background(), []string{"query", "run"}, strings.NewReader(""), shortWriter{}, &failureDiagnostic, build)
	if status != 1 || !strings.Contains(failureDiagnostic.String(), "Could not write") || opened != 1 || closed != 1 {
		t.Fatalf("ignored short write status=%d stderr=%q open=%d close=%d", status, failureDiagnostic.String(), opened, closed)
	}
}

func TestRawActionErrorIsExecutionFailure(t *testing.T) {
	for _, phase := range []string{"run", "pre", "persistent-pre", "post", "persistent-post", "missing-resource"} {
		t.Run(phase, func(t *testing.T) {
			build := func(call *cli.Invocation) *cobra.Command {
				command := root.New(call)
				raw := &cobra.Command{Use: "raw", RunE: func(*cobra.Command, []string) error { return nil }}
				rawFailure := func(*cobra.Command, []string) error { return errors.New("private-canary") }
				switch phase {
				case "run":
					raw.RunE = rawFailure
				case "pre":
					raw.PreRunE = rawFailure
				case "persistent-pre":
					raw.PersistentPreRunE = rawFailure
				case "post":
					raw.PostRunE = rawFailure
				case "persistent-post":
					raw.PersistentPostRunE = rawFailure
				case "missing-resource":
					raw.RunE = func(*cobra.Command, []string) error {
						return cli.Execution(failure.Code("fathomry.cli.test.missing_resource"), errors.New("private-canary"))
					}
				}
				call.Add(command, raw, "fathomry.cli.help.help", "fathomry.cli.command.help")
				return command
			}
			var out, diagnostic bytes.Buffer
			status := cli.Run(context.Background(), []string{"raw"}, strings.NewReader(""), &out, &diagnostic, build)
			if status != 1 || out.Len() != 0 || !strings.Contains(diagnostic.String(), "Command execution failed") || strings.Contains(diagnostic.String(), "private-canary") || strings.Contains(diagnostic.String(), "missing_resource") {
				t.Fatalf("phase=%s status=%d stdout=%q stderr=%q", phase, status, out.String(), diagnostic.String())
			}
		})
	}
}

func TestManyIndependentCommandCatalogs(t *testing.T) {
	build := func(call *cli.Invocation) *cobra.Command {
		command := root.New(call)
		for index := range 40 {
			child := &cobra.Command{Use: fmt.Sprintf("extra%d", index)}
			call.Add(command, child, query.GroupHelpID, query.GroupSummaryID, query.Resources()...)
		}
		return command
	}
	var out, diagnostic bytes.Buffer
	status := cli.Run(context.Background(), []string{"--help"}, strings.NewReader(""), &out, &diagnostic, build)
	if status != 0 || diagnostic.Len() != 0 || !strings.Contains(out.String(), "extra39    Test-only query group") {
		t.Fatalf("many command help status=%d stdout=%q stderr=%q", status, out.String(), diagnostic.String())
	}
}

type shortWriter struct{}

func (shortWriter) Write([]byte) (int, error) { return 1, nil }

type rejectWriter struct{}

func (rejectWriter) Write([]byte) (int, error) { return 0, errors.New("synthetic") }

type partialErrorWriter struct{}

func (partialErrorWriter) Write([]byte) (int, error) { return 1, errors.New("synthetic partial") }

func TestCheckedWritersAndPrimaryStatus(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"version", "--help"}, {"version"}, {"version", "--output=json"}} {
		for _, writer := range []io.Writer{shortWriter{}, rejectWriter{}, partialErrorWriter{}} {
			var diagnostic bytes.Buffer
			status := root.Run(context.Background(), args, strings.NewReader(""), writer, &diagnostic)
			if status != 1 || !strings.Contains(diagnostic.String(), "Could not write") {
				t.Fatalf("args=%q status=%d stderr=%q", args, status, diagnostic.String())
			}
		}
	}
	if status := root.Run(context.Background(), []string{"missing"}, strings.NewReader(""), io.Discard, rejectWriter{}); status != 2 {
		t.Fatalf("diagnostic failure changed usage status: %d", status)
	}
}

func TestContextAndInvocationIsolation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if status := root.Run(ctx, []string{"version"}, strings.NewReader(""), io.Discard, io.Discard); status != 130 {
		t.Fatalf("cancel status=%d", status)
	}
	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if status := root.Run(ctx, []string{"version"}, strings.NewReader(""), io.Discard, io.Discard); status != 1 {
		t.Fatalf("deadline status=%d", status)
	}
	for _, args := range [][]string{nil, {}} {
		var out, diagnostic bytes.Buffer
		status := root.Run(context.Background(), args, panicReader{}, &out, &diagnostic)
		if status != 0 || !strings.Contains(out.String(), "Usage:") || diagnostic.Len() != 0 {
			t.Fatalf("nil/empty args status=%d stdout=%q stderr=%q", status, out.String(), diagnostic.String())
		}
	}
	var wait sync.WaitGroup
	for _, locale := range []string{"en", "zh-CN"} {
		wait.Go(func() {
			for range 20 {
				status, out, diagnostic := execute("--lang="+locale, "--help")
				if status != 0 || diagnostic != "" || (locale == "en") != strings.Contains(out, "Usage:") {
					t.Errorf("locale=%s status=%d stdout=%q stderr=%q", locale, status, out, diagnostic)
				}
			}
		})
	}
	wait.Wait()
}

func TestNoInputConsumptionAndInvalidInjection(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"version"}, {"version", "--output=json"}} {
		if status := root.Run(context.Background(), args, panicReader{}, io.Discard, io.Discard); status != 0 {
			t.Fatalf("args=%q status=%d", args, status)
		}
	}
	for _, test := range []struct {
		ctx    context.Context
		input  io.Reader
		output io.Writer
		errOut io.Writer
	}{
		{nil, strings.NewReader(""), io.Discard, io.Discard},
		{context.Background(), nil, io.Discard, io.Discard},
		{context.Background(), strings.NewReader(""), nil, io.Discard},
		{context.Background(), strings.NewReader(""), io.Discard, nil},
	} {
		if status := root.Run(test.ctx, nil, test.input, test.output, test.errOut); status != 1 {
			t.Fatalf("invalid injection status=%d", status)
		}
	}
}

type panicReader struct{}

func (panicReader) Read([]byte) (int, error) { panic("CLI unexpectedly read stdin") }
