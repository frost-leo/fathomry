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

package source_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/source"
)

func TestInitializationCancellationRetainsCause(t *testing.T) {
	for _, phase := range []string{"before-construction", "after-construction", "after-readiness"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			cause := errors.New("review initialization cancellation reason")
			selected := source.Select(prepared(t, "review", "example.reader", ""), func(context.Context, settings) (source.Resource[reader], error) {
				if phase == "after-construction" {
					cancel(cause)
				}
				return source.Resource[reader]{
					Acquired: true, Capability: readerFacade{}, Release: complete,
					Check: func(context.Context) error {
						if phase == "after-readiness" {
							cancel(cause)
						}
						return nil
					},
				}, nil
			})
			if phase == "before-construction" {
				cancel(cause)
			}
			assembly, err := source.Assemble(ctx, context.Background(), "review", selected)
			if assembly == nil || !errors.Is(err, context.Canceled) || context.Cause(ctx) != cause {
				t.Fatal("control case did not reach the intended canceled initialization")
			}
			if closeErr := assembly.Close(context.Background()); closeErr != nil {
				t.Fatal("fixture cleanup failed")
			}
			t.Logf("cancellation identity preserved=%t, explicit cause preserved=%t",
				errors.Is(err, context.Canceled), errors.Is(err, cause))
			if !errors.Is(err, cause) || !errors.Is(assembly.Snapshot().Primary, cause) {
				t.Error("initialization error and primary report dropped the caller's explicit cancellation cause")
			}
		})
	}
}

type initializationCause struct{ message string }

func (cause *initializationCause) Error() string { return cause.message }

func TestInitializationCancellationEvidenceAndOrder(t *testing.T) {
	for _, kind := range []string{"explicit", "parent", "deadline", "parent-deadline", "ordinary", "nil-cause"} {
		for _, phase := range []string{"before-construction", "after-construction", "after-readiness"} {
			t.Run(kind+"/"+phase, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					cause := &initializationCause{message: "initialization-cause-canary"}
					var ctx context.Context
					var trigger func()
					identity, expectedCause := error(context.Canceled), error(cause)
					switch kind {
					case "explicit", "parent", "nil-cause":
						parent, cancel := context.WithCancelCause(context.Background())
						defer cancel(nil)
						ctx = parent
						trigger = func() { cancel(cause) }
						if kind == "parent" {
							child, cancelChild := context.WithCancel(parent)
							defer cancelChild()
							ctx = child
						}
						if kind == "nil-cause" {
							trigger = func() { cancel(nil) }
							expectedCause = context.Canceled
						}
					case "deadline", "parent-deadline":
						parent, cancel := context.WithDeadlineCause(context.Background(), time.Now().Add(time.Second), cause)
						defer cancel()
						ctx = parent
						if kind == "parent-deadline" {
							child, cancelChild := context.WithCancel(parent)
							defer cancelChild()
							ctx = child
						}
						trigger = func() { <-ctx.Done() }
						identity = context.DeadlineExceeded
					case "ordinary":
						parent, cancel := context.WithCancel(context.Background())
						defer cancel()
						ctx, trigger, expectedCause = parent, cancel, context.Canceled
					}
					cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), time.Minute)
					defer cancelCleanup()
					var order []string
					selected := source.Select(prepared(t, "one", "example.reader", ""), func(input context.Context, _ settings) (source.Resource[reader], error) {
						if input != ctx || input.Err() != nil {
							t.Error("construction lost the live caller-owned context")
						}
						order = append(order, "construct")
						if phase == "after-construction" {
							trigger()
						}
						return source.Resource[reader]{Acquired: true, Capability: readerFacade{},
							Check: func(input context.Context) error {
								if input != ctx || input.Err() != nil {
									t.Error("readiness ran with an expired or substituted context")
								}
								order = append(order, "check")
								if phase == "after-readiness" {
									trigger()
								}
								return nil
							}, Release: func(input context.Context) source.ReleaseResult {
								if input != cleanupCtx || input.Err() != nil {
									t.Error("cleanup did not use the separate caller-owned budget")
								}
								order = append(order, "release")
								return complete(input)
							}}, nil
					})
					if phase == "before-construction" {
						trigger()
					}
					assembly, err := source.Assemble(ctx, cleanupCtx, "cancellation", selected)
					if assembly == nil || !errors.Is(err, source.ErrAssembly) || ctx.Err() != identity || context.Cause(ctx) != expectedCause {
						t.Fatal("did not reach the intended initialization cancellation")
					}
					report := assembly.Snapshot()
					for _, result := range []error{err, report.Primary} {
						if !errors.Is(result, identity) || !errors.Is(result, expectedCause) || !errors.Is(result, source.ErrInitialization) {
							t.Error("initialization lost context identity or explicit cause")
						}
						if expectedCause == cause {
							conformance.Cause(t, result, func(original *initializationCause) bool { return original == cause })
						}
						conformance.Private(t, result, cause.message)
					}
					conformance.Private(t, report, cause.message)
					var expectedOrder []string
					operation := "construct"
					if phase == "after-construction" {
						expectedOrder, operation = []string{"construct", "release"}, "check"
					} else if phase == "after-readiness" {
						expectedOrder, operation = []string{"construct", "check", "release"}, "assemble"
					}
					var primary *failure.Error
					if !errors.As(report.Primary, &primary) || primary.Diagnostic().Attribution.Operation != operation ||
						report.Ready || !reflect.DeepEqual(order, expectedOrder) {
						t.Error("cancellation phase, readiness or callback ordering changed")
					}
					if _, _, bindErr := source.Bind(assembly, selected); !errors.Is(bindErr, source.ErrSelection) {
						t.Error("canceled initialization exposed a capability")
					}
					if closeErr := assembly.Close(cleanupCtx); closeErr != nil || !reflect.DeepEqual(order, expectedOrder) {
						t.Error("completed cleanup was retried")
					}
					assertNoPending(t, assembly)
				})
			})
		}
	}
}

func TestInitializationFailureCancellationAndCleanupRemainSeparate(t *testing.T) {
	for _, path := range []string{"framework", "constructor-error", "readiness-error"} {
		for _, cleanup := range []string{"continue", "no-continuation", "released"} {
			t.Run(path+"/"+cleanup, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				cancelCause := &initializationCause{message: "cancellation-canary"}
				native := &initializationCause{message: "callback-canary"}
				callbackIdentity := failure.MustDefine(failure.Definition{Code: "example.reader.initialization", Component: "reader", Version: 1})
				callbackErr := callbackIdentity.New(failure.Attribution{}, native)
				cleanupCause := errors.New("cleanup-canary")
				cleanupCtx, cancelCleanup := context.WithCancel(context.Background())
				defer cancelCleanup()
				var order []string
				dependency := selection(t, "dependency", func(input context.Context) source.ReleaseResult {
					if input != cleanupCtx || input.Err() != nil {
						t.Error("dependency cleanup context replaced")
					}
					order = append(order, "dependency")
					return complete(input)
				})
				consumer := source.Select(prepared(t, "consumer", "example.reader", ""), func(context.Context, settings) (source.Resource[reader], error) {
					resource := source.Resource[reader]{Acquired: true, Capability: readerFacade{},
						Release: func(input context.Context) source.ReleaseResult {
							if input != cleanupCtx || input.Err() != nil {
								t.Error("consumer cleanup inherited cancellation or lost its caller context")
							}
							order = append(order, "consumer")
							result := source.ReleaseResult{Err: cleanupCause}
							if cleanup == "released" {
								result.Quiescent, result.Released = true, true
							} else if cleanup == "continue" {
								result.Continue = func(input context.Context) source.ReleaseResult {
									if input != cleanupCtx {
										t.Error("continuation context replaced")
									}
									order = append(order, "continue")
									return complete(input)
								}
							}
							return result
						}}
					if path == "readiness-error" {
						resource.Check = func(context.Context) error { cancel(cancelCause); return callbackErr }
						return resource, nil
					}
					cancel(cancelCause)
					if path == "constructor-error" {
						return resource, callbackErr
					}
					return resource, nil
				})
				assembly, err := source.Assemble(ctx, cleanupCtx, "failed", dependency, consumer)
				if assembly == nil || !errors.Is(err, source.ErrAssembly) || !errors.Is(err, cleanupCause) {
					t.Fatal("primary/cleanup failure or acquired assembly lost")
				}
				report := assembly.Snapshot()
				if report.Ready || len(report.Sources) != 2 || len(report.Sources[1].CleanupErrors) != 1 ||
					!errors.Is(report.Sources[1].CleanupErrors[0], cleanupCause) || errors.Is(report.Primary, cleanupCause) {
					t.Fatal("primary and cleanup report evidence conflated")
				}
				for _, result := range []error{err, report.Primary} {
					if path == "framework" {
						if !errors.Is(result, context.Canceled) || !errors.Is(result, cancelCause) {
							t.Error("cleanup failure hid initialization cancellation")
						}
						conformance.Cause(t, result, func(original *initializationCause) bool { return original == cancelCause })
					} else {
						if !errors.Is(result, callbackErr) || !errors.Is(result, callbackIdentity) || errors.Is(result, cancelCause) || errors.Is(result, context.Canceled) {
							t.Error("coincident cancellation replaced or relabeled an observed callback error")
						}
						conformance.Cause(t, result, func(original *initializationCause) bool { return original == native })
					}
					conformance.Private(t, result, cancelCause.message, native.message, "cleanup-canary")
				}
				conformance.Private(t, report, cancelCause.message, native.message, "cleanup-canary")
				pending := cleanup != "released"
				expectedOrder := []string{"consumer"}
				if !pending {
					expectedOrder = append(expectedOrder, "dependency")
				}
				if errors.Is(err, source.ErrIncomplete) != pending || report.Sources[0].Pending != pending ||
					report.Sources[1].Pending != pending || report.Sources[1].Quiescent == pending || report.Sources[1].Released == pending ||
					report.Sources[1].CanContinue != (cleanup == "continue") || !reflect.DeepEqual(order, expectedOrder) {
					t.Fatal("incomplete cleanup lost resource/dependency responsibility")
				}
				if cleanup == "continue" {
					expectedOrder = []string{"consumer", "continue", "dependency"}
				}
				for range 2 {
					closeErr := assembly.Close(cleanupCtx)
					if !errors.Is(closeErr, cleanupCause) || errors.Is(closeErr, source.ErrIncomplete) != (cleanup == "no-continuation") ||
						!reflect.DeepEqual(order, expectedOrder) || assembly.Snapshot().Primary != report.Primary {
						t.Error("cleanup retry fabricated completion or erased failure history")
					}
				}
				if cleanup != "no-continuation" {
					assertNoPending(t, assembly)
				}
			})
		}
	}
}

func TestInitializationCancellationWithExpiredCleanupRetainsResponsibility(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	initCause, cleanupCause := errors.New("init-canary"), errors.New("cleanup-budget-canary")
	cleanupCtx, cancelCleanup := context.WithDeadlineCause(context.Background(), time.Time{}, cleanupCause)
	defer cancelCleanup()
	var releases int
	selected := source.Select(prepared(t, "one", "example.reader", ""), func(context.Context, settings) (source.Resource[reader], error) {
		cancel(initCause)
		return source.Resource[reader]{Acquired: true, Capability: readerFacade{}, Release: func(input context.Context) source.ReleaseResult {
			if input.Err() != nil {
				t.Error("cleanup ran with an expired budget")
			}
			releases++
			return complete(input)
		}}, nil
	})
	assembly, err := source.Assemble(ctx, cleanupCtx, "expired-cleanup", selected)
	if assembly == nil {
		t.Fatal("acquired responsibility lost")
	}
	for _, cause := range []error{context.Canceled, initCause, context.DeadlineExceeded, cleanupCause, source.ErrIncomplete} {
		if !errors.Is(err, cause) {
			t.Error("initialization or cleanup-budget cause lost")
		}
	}
	report := assembly.Snapshot()
	if !errors.Is(report.Primary, initCause) || errors.Is(report.Primary, cleanupCause) || releases != 0 ||
		!report.Sources[0].Pending || !report.Sources[0].CanContinue || report.Sources[0].Released || report.Sources[0].Quiescent {
		t.Fatal("expired cleanup lost primary attribution or unattempted responsibility")
	}
	conformance.Private(t, err, "init-canary", "cleanup-budget-canary")
	if closeErr := assembly.Close(context.Background()); closeErr != nil || releases != 1 {
		t.Fatal("unattempted release could not resume with a fresh caller budget")
	}
	assertNoPending(t, assembly)
}
