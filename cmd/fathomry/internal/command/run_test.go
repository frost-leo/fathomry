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
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	"github.com/spf13/cobra"
)

func fixtureCatalogs(t testing.TB) Catalogs {
	t.Helper()
	components := append(i18n.CoreComponents(), Component())
	var definitions []failure.Definition
	for _, component := range components {
		definitions = append(definitions, component.Definitions...)
	}
	errors, err := failure.Prepare(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := i18n.Prepare(components...)
	if err != nil {
		t.Fatal(err)
	}
	return Catalogs{Errors: errors, Messages: messages}
}

func fixtureCommand(invocation *Invocation, action func(*Invocation) error) *cobra.Command {
	root := invocation.Group("fathomry", "fathomry.command_line.root")
	work := &cobra.Command{Use: "work", Short: "fathomry.command_line.error_list", Args: cobra.NoArgs}
	invocation.Bind(work, func(context.Context, []string) error { return action(invocation) })
	root.AddCommand(work)
	return root
}

func bindFixture(t *testing.T, invocation *Invocation, release func(context.Context) resource.ReleaseResult, buildError error) (*resource.Scope, resource.Ref[int], error) {
	t.Helper()
	scope, err := invocation.Scope()
	if err != nil {
		return nil, resource.Ref[int]{}, err
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = scope.Close(ctx)
	})
	ref, err := resource.Bind(scope, resource.Binding[int, int]{
		Name: "fixture", Policy: resource.Fixed,
		Select: func(view settings.View) (int, error) {
			snapshot, err := settings.As[int](view)
			if err != nil {
				return 0, err
			}
			return snapshot.ValueCopy()
		},
		Clone: func(value int) int { return value },
		Build: func(context.Context, int) (*resource.Instance[int], error) {
			return &resource.Instance[int]{Value: 42, Release: release}, buildError
		},
	})
	if err != nil {
		return scope, ref, err
	}
	snapshot, err := settings.New(1, func(value int) int { return value })
	if err != nil {
		return scope, ref, err
	}
	update, err := scope.Apply(context.Background(), snapshot.View())
	if err != nil {
		return scope, ref, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return scope, ref, update.Wait(ctx)
}

func TestInvocationOwnership(t *testing.T) {
	catalogs := fixtureCatalogs(t)
	for _, mode := range []string{"success", "failure", "partial_build", "canceled", "cleanup_error"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var output, diagnostic bytes.Buffer
			var released atomic.Int32
			var freshCleanup atomic.Bool
			operationCause := errors.New("private-operation-canary")
			cleanupCause := errors.New("private-cleanup-canary")
			err := Run(ctx, []string{"work"}, Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic},
				catalogs, func(invocation *Invocation) *cobra.Command {
					return fixtureCommand(invocation, func(invocation *Invocation) error {
						var construction error
						if mode == "partial_build" {
							construction = operationCause
						}
						_, _, err := bindFixture(t, invocation, func(cleanup context.Context) resource.ReleaseResult {
							freshCleanup.Store(cleanup.Err() == nil)
							released.Add(1)
							var failure error
							if mode == "cleanup_error" {
								failure = cleanupCause
							}
							return resource.ReleaseResult{Complete: true, Err: failure}
						}, construction)
						if err != nil {
							return err
						}
						if err := invocation.Result("work", map[string]int{"value": 42}, func(writer io.Writer) error {
							_, err := io.WriteString(writer, "success\n")
							return err
						}); err != nil {
							return err
						}
						switch mode {
						case "failure":
							return operationCause
						case "canceled":
							cancel()
							return ctx.Err()
						default:
							return nil
						}
					})
				})
			if released.Load() != 1 || !freshCleanup.Load() {
				t.Fatal("cleanup absent, duplicated or passed a canceled context")
			}
			if strings.Contains(diagnostic.String(), "private-") {
				t.Fatal("native error text leaked")
			}
			switch mode {
			case "success":
				if err != nil || output.String() != "success\n" || diagnostic.Len() != 0 {
					t.Fatal("successful outcome lost", err)
				}
			case "failure":
				if !errors.Is(err, operationCause) || !errors.Is(err, ErrExecution) || ExitCode(err) != 1 {
					t.Fatal("operation identity lost", err)
				}
			case "partial_build":
				core, ok := failure.Inspect(err)
				if !errors.Is(err, operationCause) || !ok || core.Diagnostic().Definition.Code != resource.ErrBuild {
					t.Fatal("partial construction replaced the original public failure", err)
				}
			case "canceled":
				if !errors.Is(err, context.Canceled) || ExitCode(err) != 130 {
					t.Fatal("cancellation lost", err)
				}
			case "cleanup_error":
				if !errors.Is(err, cleanupCause) || !errors.Is(err, ErrCleanup) || ExitCode(err) != 1 {
					t.Fatal("cleanup identity lost", err)
				}
			}
			if mode != "success" && output.Len() != 0 {
				t.Fatal("success response escaped before final outcome")
			}
		})
	}
	t.Run("held_borrow_is_not_reported_as_cleaned", func(t *testing.T) {
		var output, diagnostic bytes.Buffer
		var held resource.Lease[int]
		var scope *resource.Scope
		var released atomic.Int32
		err := Run(context.Background(), []string{"work"}, Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic, CleanupTimeout: 10 * time.Millisecond},
			catalogs, func(invocation *Invocation) *cobra.Command {
				return fixtureCommand(invocation, func(invocation *Invocation) error {
					var ref resource.Ref[int]
					var err error
					scope, ref, err = bindFixture(t, invocation, func(context.Context) resource.ReleaseResult {
						released.Add(1)
						return resource.ReleaseResult{Complete: true}
					}, nil)
					if err != nil {
						return err
					}
					held, err = ref.Acquire(context.Background())
					return err
				})
			})
		if !errors.Is(err, ErrCleanup) || !errors.Is(err, resource.ErrWait) || released.Load() != 0 || output.Len() != 0 {
			t.Fatal("timed-out cleanup was hidden or abandoned the borrow", err)
		}
		value, err := held.Value()
		if err != nil || value != 42 {
			t.Fatal("borrow invalidated")
		}
		if err := held.Release(); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := scope.Close(ctx); err != nil || released.Load() != 1 {
			t.Fatal("retained cleanup could not be joined", err)
		}
	})
	t.Run("canceled_admission_does_not_build_commands", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		called := false
		err := Run(ctx, []string{"work"}, Options{Input: strings.NewReader(""), Output: io.Discard, ErrorOutput: io.Discard},
			catalogs, func(invocation *Invocation) *cobra.Command {
				called = true
				return fixtureCommand(invocation, func(*Invocation) error { return nil })
			})
		if called || !errors.Is(err, context.Canceled) || ExitCode(err) != 130 {
			t.Fatal("canceled invocation entered construction")
		}
	})
	t.Run("panic_still_closes_owned_resources", func(t *testing.T) {
		var released atomic.Int32
		defer func() {
			if recover() != "fixture-panic" || released.Load() != 1 {
				t.Error("panic bypassed deferred cleanup")
			}
		}()
		_ = Run(context.Background(), []string{"work"}, Options{Input: strings.NewReader(""), Output: io.Discard, ErrorOutput: io.Discard},
			catalogs, func(invocation *Invocation) *cobra.Command {
				return fixtureCommand(invocation, func(invocation *Invocation) error {
					_, _, err := bindFixture(t, invocation, func(context.Context) resource.ReleaseResult {
						released.Add(1)
						return resource.ReleaseResult{Complete: true}
					}, nil)
					if err != nil {
						return err
					}
					panic("fixture-panic")
				})
			})
	})
}

type failingWriter struct {
	cause  error
	short  bool
	calls  int
	before func()
}

func (writer *failingWriter) Write(data []byte) (int, error) {
	writer.calls++
	if writer.before != nil {
		writer.before()
	}
	if writer.short {
		return len(data) - 1, nil
	}
	return 0, writer.cause
}

func TestOutputAndAdmission(t *testing.T) {
	catalogs := fixtureCatalogs(t)
	for _, short := range []bool{false, true} {
		t.Run(fmt.Sprintf("writer_short=%t", short), func(t *testing.T) {
			var released atomic.Bool
			cause := errors.New("private-writer-canary")
			writer := &failingWriter{cause: cause, short: short, before: func() {
				if !released.Load() {
					t.Error("output preceded cleanup")
				}
			}}
			var diagnostic bytes.Buffer
			err := Run(context.Background(), []string{"work"}, Options{Input: strings.NewReader(""), Output: writer, ErrorOutput: &diagnostic},
				catalogs, func(invocation *Invocation) *cobra.Command {
					return fixtureCommand(invocation, func(invocation *Invocation) error {
						_, _, err := bindFixture(t, invocation, func(context.Context) resource.ReleaseResult {
							released.Store(true)
							return resource.ReleaseResult{Complete: true}
						}, nil)
						if err != nil {
							return err
						}
						return invocation.Result("work", 42, func(output io.Writer) error { _, err := io.WriteString(output, "success"); return err })
					})
				})
			expected := cause
			if short {
				expected = io.ErrShortWrite
			}
			if !errors.Is(err, ErrOutput) || !errors.Is(err, expected) || ExitCode(err) != 1 || writer.calls != 1 || strings.Contains(diagnostic.String(), "private-") {
				t.Fatal("output failure contract lost", err)
			}
		})
	}
	t.Run("diagnostic_failure_retains_original", func(t *testing.T) {
		writer := &failingWriter{cause: io.ErrClosedPipe}
		cause := errors.New("private-native-canary")
		err := Run(context.Background(), []string{"work"}, Options{Input: strings.NewReader(""), Output: io.Discard, ErrorOutput: writer},
			catalogs, func(invocation *Invocation) *cobra.Command {
				return fixtureCommand(invocation, func(*Invocation) error { return cause })
			})
		if !errors.Is(err, cause) || !errors.Is(err, io.ErrClosedPipe) || writer.calls != 1 || ExitCode(err) != 1 {
			t.Fatal("failed error output retried or lost identity")
		}
	})
	t.Run("bounded_text_does_not_expose_embedded_writer_bypasses", func(t *testing.T) {
		buffer := &boundedBuffer{}
		for range MaxOutputBytes / (32 << 10) {
			if _, err := io.WriteString(buffer, strings.Repeat("x", 32<<10)); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := io.WriteString(buffer, "x"); !errors.Is(err, ErrLimit) {
			t.Fatal("StringWriter bypassed output ceiling")
		}
		if len(buffer.Bytes()) != MaxOutputBytes {
			t.Fatal("over-limit partial buffer write")
		}
	})
	t.Run("invalid_declarations", func(t *testing.T) {
		base := Options{Input: strings.NewReader(""), Output: io.Discard, ErrorOutput: io.Discard}
		for _, modify := range []func(*Options){
			func(value *Options) { value.Input = nil }, func(value *Options) { value.Output = nil },
			func(value *Options) { value.ErrorOutput = nil }, func(value *Options) { value.CleanupTimeout = -time.Second },
		} {
			options := base
			modify(&options)
			called := false
			if err := Run(context.Background(), nil, options, catalogs, func(invocation *Invocation) *cobra.Command {
				called = true
				return fixtureCommand(invocation, func(*Invocation) error { return nil })
			}); !errors.Is(err, ErrOptions) || called {
				t.Fatal("invalid invocation admitted")
			}
		}
	})
	for name, args := range map[string][]string{"count": make([]string, MaxArguments+1), "bytes": {strings.Repeat("x", MaxArgumentBytes+1)}, "nul": {"\x00"}, "utf8": {string([]byte{0xff})}} {
		t.Run(name, func(t *testing.T) {
			called := false
			err := Run(context.Background(), args, Options{Input: strings.NewReader(""), Output: io.Discard, ErrorOutput: io.Discard}, catalogs,
				func(invocation *Invocation) *cobra.Command {
					called = true
					return fixtureCommand(invocation, func(*Invocation) error { return nil })
				})
			if err == nil || called || ExitCode(err) != 2 {
				t.Fatal("invalid argument admission performed work")
			}
		})
	}
}
