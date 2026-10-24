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
package command

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/spf13/cobra"
)

type brokenMutationWriter struct{ cause error }

func TestExecutionCleanupRequiresScopeShutdown(t *testing.T) {
	scope, err := resource.New(context.Background(), resource.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close(context.Background())
	value := &ExecutionError{scope: scope}
	if value.CleanupComplete() {
		t.Fatal("an open empty scope was reported as fully shut down")
	}
	if err := value.Close(context.Background()); err != nil || !value.CleanupComplete() {
		t.Fatal("actual scope shutdown was not reported", err)
	}
}

func (writer brokenMutationWriter) Write([]byte) (int, error) { return 0, writer.cause }

func TestMutationRetainsEffectAcrossFailures(t *testing.T) {
	for _, mode := range []string{"success", "execution", "cleanup", "output", "diagnostic", "render"} {
		for _, format := range []string{"text", "json"} {
			if mode == "render" && format == "json" {
				continue
			}
			t.Run(mode+"/"+format, func(t *testing.T) {
				var output, diagnostic bytes.Buffer
				operationCause := errors.New("private-operation-canary")
				cleanupCause := errors.New("private-cleanup-canary")
				outputCause := errors.New("private-output-canary")
				var target io.Writer = &output
				var errorTarget io.Writer = &diagnostic
				if mode == "output" {
					target = brokenMutationWriter{outputCause}
				}
				if mode == "diagnostic" {
					errorTarget = brokenMutationWriter{outputCause}
				}
				err := Run(context.Background(), []string{"work", "--output=" + format}, Options{Input: strings.NewReader(""), Output: target, ErrorOutput: errorTarget},
					fixtureCatalogs(t), func(invocation *Invocation) *cobra.Command {
						return fixtureCommand(invocation, func(invocation *Invocation) error {
							if mode == "cleanup" {
								if _, _, err := bindFixture(t, invocation, func(context.Context) resource.ReleaseResult {
									return resource.ReleaseResult{Complete: true, Err: cleanupCause}
								}, nil); err != nil {
									return err
								}
							}
							if err := invocation.MutationResult("schema.apply", map[string]string{"effect": "applied", "state": "current"}, MutationSummary{Effect: "applied", State: "current"}, func(writer io.Writer) error {
								if mode == "render" {
									return outputCause
								}
								_, err := io.WriteString(writer, "applied current\n")
								return err
							}); err != nil {
								return err
							}
							if mode == "execution" || mode == "diagnostic" {
								return Fail(ErrExecution, operationCause)
							}
							return nil
						})
					})
				if mode == "success" {
					if err != nil || output.Len() == 0 || diagnostic.Len() != 0 {
						t.Fatal("success changed", err)
					}
					return
				}
				var retained *ExecutionError
				if !errors.As(err, &retained) || !retained.CleanupComplete() {
					t.Fatal("online outcome not retained", err)
				}
				core, known := failure.Inspect(err)
				if !known {
					t.Fatal("direct core lost")
				}
				switch mode {
				case "execution", "render":
					if core.Diagnostic().Definition.Code != ErrExecution {
						t.Fatal("wrong primary failure", err)
					}
				case "cleanup":
					if core.Diagnostic().Definition.Code != ErrCleanup || !errors.Is(err, cleanupCause) {
						t.Fatal("cleanup identity lost", err)
					}
				case "output", "diagnostic":
					if core.Diagnostic().Definition.Code != ErrOutput || !errors.Is(err, outputCause) {
						t.Fatal("delivery failure lost", err)
					}
				}
				if (mode == "execution" || mode == "diagnostic") && !errors.Is(err, operationCause) {
					t.Fatal("original cause lost")
				}
				var result struct {
					Command string
					Data    map[string]string
				}
				original := retained.ReportCopy()
				if json.Unmarshal(original, &result) != nil || result.Command != "schema.apply" || result.Data["effect"] != "applied" || result.Data["state"] != "current" {
					t.Fatal("acknowledged effect lost")
				}
				original[0] = '!'
				if bytes.Equal(original, retained.ReportCopy()) {
					t.Fatal("report aliases retained custody")
				}
				if mode != "output" && mode != "render" && output.Len() == 0 {
					t.Fatal("known effect suppressed by failure")
				}
				if mode != "diagnostic" && (!strings.Contains(diagnostic.String(), "applied") || !strings.Contains(diagnostic.String(), "current")) {
					t.Fatal("effect missing from surviving diagnostic")
				}
				if strings.Contains(output.String()+diagnostic.String(), "private-") {
					t.Fatal("native text exposed")
				}
				if err := retained.Close(context.Background()); mode != "cleanup" && err != nil {
					t.Fatal(err)
				}
				if _, err := json.Marshal(retained); err == nil {
					t.Fatal("runtime error serialized")
				}
				if strings.Contains(fmt.Sprintf("%+v %#v", retained, retained), "private-") {
					t.Fatal("formatted error leaked")
				}
				var logs bytes.Buffer
				slog.New(slog.NewJSONHandler(&logs, nil)).Info("failure", "value", retained)
				if strings.Contains(logs.String(), "private-") {
					t.Fatal("logged error leaked")
				}
			})
		}
	}
}

func TestMutationCleanupContinuation(t *testing.T) {
	var held resource.Lease[int]
	var released atomic.Int32
	var output, diagnostic bytes.Buffer
	err := Run(context.Background(), []string{"work", "--output=json"}, Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic, CleanupTimeout: 5 * time.Millisecond},
		fixtureCatalogs(t), func(invocation *Invocation) *cobra.Command {
			return fixtureCommand(invocation, func(invocation *Invocation) error {
				_, ref, err := bindFixture(t, invocation, func(context.Context) resource.ReleaseResult {
					released.Add(1)
					return resource.ReleaseResult{Complete: true}
				}, nil)
				if err != nil {
					return err
				}
				held, err = ref.Acquire(context.Background())
				if err != nil {
					return err
				}
				return invocation.MutationResult("schema.apply", map[string]string{"effect": "unknown", "state": "indeterminate"}, MutationSummary{Effect: "unknown", State: "indeterminate"}, nil)
			})
		})
	var retained *ExecutionError
	if !errors.Is(err, ErrCleanup) || !errors.As(err, &retained) || retained.CleanupComplete() || released.Load() != 0 || output.Len() == 0 {
		t.Fatal("incomplete cleanup abandoned", err)
	}
	before := retained.ReportCopy()
	if err := held.Release(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := retained.Close(ctx); err != nil || !retained.CleanupComplete() || released.Load() != 1 {
		t.Fatal("same cleanup could not continue", err)
	}
	if !bytes.Equal(before, retained.ReportCopy()) || !errors.Is(retained, ErrCleanup) {
		t.Fatal("original facts rewritten after cleanup")
	}
}

func TestMutationSummaryAdmission(t *testing.T) {
	for _, summary := range []MutationSummary{{}, {"applied", "private target"}, {"applied", "CURRENT"}, {strings.Repeat("a", 65), "current"}} {
		invocation := &Invocation{}
		if err := invocation.MutationResult("schema.apply", nil, summary, nil); !errors.Is(err, ErrOptions) {
			t.Fatal("unsafe summary admitted", summary)
		}
	}
}
