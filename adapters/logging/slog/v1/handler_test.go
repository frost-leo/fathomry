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

package slog_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	stdslog "log/slog"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	ingress "github.com/frost-leo/fathomry/adapters/logging/slog/v1"
	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/resource/v1"
)

func options() ingress.Options {
	return ingress.Options{Limits: logging.Limits{MaxFields: 64, MaxNodes: 256, MaxDepth: 8, MaxBytes: 64 << 10},
		Timeout: time.Second, MaxMessageBytes: 64 << 10, MaxViews: 128, MaxActive: 16, MaxRetainedBytes: 8 << 20, Levels: []int{-4, 0, 4, 8}}
}
func known(t *testing.T) *failure.Error {
	t.Helper()
	value, err := failure.New(logging.Definitions()[0], failure.Location{Operation: "test"}, errors.New("private-cause"))
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func newHandler(t *testing.T, binding ingress.Binding) *ingress.Handler {
	t.Helper()
	if binding.Lifetime == nil {
		binding.Lifetime = context.Background()
	}
	value, err := ingress.New(options(), binding)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = value.Close(ctx)
	})
	return value
}
func fieldMap(fields []logging.Field) map[string]any {
	result := map[string]any{}
	for _, field := range fields {
		value := field.Value
		switch value.Kind() {
		case logging.GroupKind:
			result[field.Key] = fieldMap(value.FieldsCopy())
		case logging.Int64Kind:
			result[field.Key] = float64(value.Int64())
		case logging.StringKind:
			result[field.Key] = value.StringValue()
		case logging.BoolKind:
			result[field.Key] = value.Bool()
		case logging.NullKind:
			result[field.Key] = nil
		}
	}
	return result
}

func TestRestrictedGroupChronologyMatchesStandardControl(t *testing.T) {
	var captured ingress.Record
	handler := newHandler(t, ingress.Binding{Emit: func(_ context.Context, value ingress.Record) ingress.Attempt {
		captured = value
		return ingress.Attempt{Admitted: true}
	}})
	actual := stdslog.New(handler).With("before", 1).WithGroup("g").With("within", 2).WithGroup("h")
	var output bytes.Buffer
	reference := stdslog.New(stdslog.NewJSONHandler(&output, nil)).With("before", 1).WithGroup("g").With("within", 2).WithGroup("h")
	actual.LogAttrs(context.Background(), stdslog.LevelInfo, "record", stdslog.Int("current", 3))
	reference.LogAttrs(context.Background(), stdslog.LevelInfo, "record", stdslog.Int("current", 3))
	var expected map[string]any
	if err := json.Unmarshal(output.Bytes(), &expected); err != nil {
		t.Fatal(err)
	}
	delete(expected, "time")
	delete(expected, "level")
	delete(expected, "msg")
	got := fieldMap(captured.Fields)
	actualJSON, _ := json.Marshal(got)
	expectedJSON, _ := json.Marshal(expected)
	if string(actualJSON) != string(expectedJSON) {
		t.Fatal("WithAttrs moved beneath a later WithGroup", string(actualJSON), string(expectedJSON))
	}
	empty := handler.WithGroup("").WithAttrs([]stdslog.Attr{})
	if empty != handler {
		t.Fatal("standard empty derivation no-op changed")
	}
	var record stdslog.Record
	record.Level = stdslog.LevelInfo
	record.Message = "empty"
	record.AddAttrs(stdslog.Attr{}, stdslog.Group("ignored"), stdslog.Group("", stdslog.String("inline", "ok")))
	if err := handler.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if got := fieldMap(captured.Fields); len(got) != 1 || got["inline"] != "ok" {
		t.Fatal("safe empty/inline group semantics changed", got)
	}
}

func TestRecordTimeCallerContextAndClosedValues(t *testing.T) {
	type key struct{}
	caller, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "association"))
	cancel()
	var pcs [1]uintptr
	runtime.Callers(1, pcs[:])
	when := time.Unix(42, 123).In(time.FixedZone("original", 3600))
	calls := 0
	handler := newHandler(t, ingress.Binding{Emit: func(ctx context.Context, record ingress.Record) ingress.Attempt {
		calls++
		if ctx.Err() != nil || ctx.Value(key{}) != "association" {
			t.Error("caller cancellation suppressed association/attempt")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("execution has no owner-selected timeout")
		}
		if calls == 1 {
			if record.PC != pcs[0] || record.Time != when {
				t.Error("record time or original PC rewritten")
			}
			values := record.Fields
			if len(values) != 2 || values[0].Value.Uint64() != math.MaxUint64 || values[1].Value.Kind() != logging.Float32Kind {
				t.Error("closed uint64/float32 representation lost")
			}
		} else if !record.Time.IsZero() || record.PC != 0 {
			t.Error("absent time/caller invented")
		}
		return ingress.Attempt{Admitted: true}
	}})
	record := stdslog.NewRecord(when, stdslog.LevelInfo, "closed", pcs[0])
	record.AddAttrs(stdslog.Any("unsigned", logging.Uint64(math.MaxUint64)), stdslog.Any("float32", logging.Float32(1.25)))
	if err := handler.Handle(caller, record); err != nil {
		t.Fatal(err)
	}
	if err := handler.Handle(caller, stdslog.NewRecord(time.Time{}, stdslog.LevelInfo, "absent", 0)); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("missing attempts")
	}
}

type forbidden struct{ calls *atomic.Int64 }

func (value forbidden) LogValue() stdslog.Value { value.calls.Add(1); panic("forbidden LogValue") }
func (value forbidden) String() string          { value.calls.Add(1); panic("forbidden String") }

type foreignError struct{ calls *atomic.Int64 }

func (value foreignError) Error() string { value.calls.Add(1); panic("forbidden Error") }

func TestPoisonedDerivationsAndHiddenHandleErrorsAreObservable(t *testing.T) {
	var invoked atomic.Int64
	emitted := 0
	handler := newHandler(t, ingress.Binding{Emit: func(context.Context, ingress.Record) ingress.Attempt {
		emitted++
		return ingress.Attempt{Admitted: true}
	}})
	logger := stdslog.New(handler)
	bad := logger.With("safe", 1, "bad", forbidden{&invoked})
	bad.Info("must not emit valid subset")
	logger.Info("bad event", "error", foreignError{&invoked})
	logger.Log(context.Background(), stdslog.Level(12), "unsupported severity")
	logger.Info("healthy sibling", "valid", true)
	status, err := handler.Status()
	if err != nil || status.InvalidDerivations != 1 || status.Refused != 3 || status.Admitted != 1 || emitted != 1 || invoked.Load() != 0 {
		t.Fatalf("invalid inputs were hidden or evaluated: derivations=%d refused=%d admitted=%d emitted=%d callbacks=%d err=%v", status.InvalidDerivations, status.Refused, status.Admitted, emitted, invoked.Load(), err)
	}
	child := handler.WithAttrs([]stdslog.Attr{stdslog.Int("duplicate", 1)}).WithAttrs([]stdslog.Attr{stdslog.Int("duplicate", 2)}).(*ingress.Handler)
	if status, err := child.Status(); err != nil || !status.Invalid {
		t.Fatal("invalid derived object has no observable state")
	}
}

func TestDerivationCountsAndRetainedBytesHaveNoFreshAllowance(t *testing.T) {
	config := options()
	config.MaxViews = 2
	handler, err := ingress.New(config, ingress.Binding{Lifetime: context.Background(), Emit: func(context.Context, ingress.Record) ingress.Attempt { return ingress.Attempt{Admitted: true} }})
	if err != nil {
		t.Fatal(err)
	}
	defer handler.Close(context.Background())
	_ = handler.WithAttrs([]stdslog.Attr{stdslog.Int("x", 1)})
	first := handler.WithGroup("next")
	second := handler.WithGroup("another")
	if first != second {
		t.Fatal("view exhaustion allocated distinct poison storage")
	}
	status, _ := handler.Status()
	if status.Views != 2 || status.InvalidDerivations != 2 {
		t.Fatal("derivation granted new allowance", status.Views, status.InvalidDerivations)
	}
	config = options()
	config.MaxRetainedBytes = 300
	bounded, err := ingress.New(config, ingress.Binding{Lifetime: context.Background(), Emit: func(context.Context, ingress.Record) ingress.Attempt { return ingress.Attempt{Admitted: true} }})
	if err != nil {
		t.Fatal(err)
	}
	defer bounded.Close(context.Background())
	poison := bounded.WithAttrs([]stdslog.Attr{stdslog.String("long", "data")}).(*ingress.Handler)
	if status, _ := poison.Status(); !status.Invalid || status.Views != 1 || status.RetainedBytes > 300 {
		t.Fatal("retained byte bound was bypassed")
	}
}

func TestCloseJoinsDerivationValidationAndBoundsPreparation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config := options()
		config.MaxActive = 1
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		var released atomic.Int64
		handler, err := ingress.New(config, ingress.Binding{Lifetime: context.Background(),
			Validate: func([]logging.Field) error { once.Do(func() { close(entered) }); <-release; return nil },
			Emit:     func(context.Context, ingress.Record) ingress.Attempt { return ingress.Attempt{Admitted: true} },
			Release: func(context.Context) resource.ReleaseResult {
				released.Add(1)
				return resource.ReleaseResult{Complete: true}
			}})
		if err != nil {
			t.Fatal(err)
		}
		derived := make(chan stdslog.Handler, 1)
		go func() { derived <- handler.WithAttrs([]stdslog.Attr{stdslog.Int("held", 1)}) }()
		<-entered
		if !handler.Enabled(context.Background(), stdslog.LevelInfo) {
			t.Fatal("capacity refusal would silently filter")
		}
		if err := handler.Handle(context.Background(), stdslog.NewRecord(time.Time{}, stdslog.LevelInfo, "limited", 0)); !errors.Is(err, logging.ErrLimit) {
			t.Fatal("concurrent preparation exceeded allowance", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := handler.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("Close passed active validation", err)
		}
		cancel()
		if released.Load() != 0 || handler.ShutdownComplete() {
			t.Fatal("retained provider view released during validation")
		}
		close(release)
		if status, _ := (<-derived).(*ingress.Handler).Status(); !status.Invalid {
			t.Fatal("derivation completed after family closed")
		}
		if err := handler.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		canceled, stop := context.WithCancel(context.Background())
		stop()
		if err := handler.Close(canceled); err != nil || released.Load() != 1 || !handler.ShutdownComplete() {
			t.Fatal("repeated joined cleanup changed authority", err)
		}
	})
}

func TestCloseContinuationAndCompletedFailureAreSeparate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		callbackDone := make(chan error, 1)
		primary := known(t)
		releases := 0
		handler := newHandler(t, ingress.Binding{Emit: func(ctx context.Context, _ ingress.Record) ingress.Attempt {
			close(entered)
			<-release
			return ingress.Attempt{Admitted: true, Err: ctx.Err()}
		}, Release: func(context.Context) resource.ReleaseResult {
			releases++
			return resource.ReleaseResult{Complete: true, Err: primary}
		}})
		go func() {
			callbackDone <- handler.Handle(context.Background(), stdslog.NewRecord(time.Time{}, stdslog.LevelInfo, "held", 0))
		}()
		<-entered
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := handler.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("Close joined an active callback", err)
		}
		cancel()
		if releases != 0 {
			t.Fatal("Release ran before producer termination")
		}
		close(release)
		if err := <-callbackDone; !errors.Is(err, context.Canceled) {
			t.Fatal("owner cancellation lost", err)
		}
		if err := handler.Close(context.Background()); !errors.Is(err, primary) || !handler.ShutdownComplete() {
			t.Fatal("completed cleanup error treated as pending", err)
		}
		if err := handler.Close(context.Background()); !errors.Is(err, primary) || releases != 1 {
			t.Fatal("completed Release retried", err)
		}
	})
}

func TestIncompleteReleaseStatusAndLaterJoin(t *testing.T) {
	calls := 0
	handler := newHandler(t, ingress.Binding{
		Emit: func(context.Context, ingress.Record) ingress.Attempt { return ingress.Attempt{Admitted: true} },
		Release: func(context.Context) resource.ReleaseResult {
			calls++
			return resource.ReleaseResult{Complete: calls > 1}
		},
	})
	if err := handler.Close(context.Background()); !errors.Is(err, logging.ErrState) || handler.ShutdownComplete() {
		t.Fatal("incomplete release was declared complete", err)
	}
	status, err := handler.Status()
	if err != nil || !errors.Is(status.LastError, logging.ErrState) {
		t.Fatal("incomplete release was not observable", err, status.LastError)
	}
	if err := handler.Close(context.Background()); err != nil || !handler.ShutdownComplete() || calls != 2 {
		t.Fatal("explicit cleanup continuation failed", err)
	}
	status, _ = handler.Status()
	if !errors.Is(status.LastError, logging.ErrState) {
		t.Fatal("later join erased historical incomplete cleanup")
	}
}

func TestFrameworkSafeLocalizedAndJoinedErrors(t *testing.T) {
	catalog, err := i18n.Prepare(i18n.Component{Module: "fathomry", Name: "logging", BaseLocale: "en", Resources: logging.Resources(), Directory: "resources", Definitions: logging.Definitions()})
	if err != nil {
		t.Fatal(err)
	}
	presenter, err := i18n.NewPresenter(catalog)
	if err != nil {
		t.Fatal(err)
	}
	presenter, err = presenter.WithLocale("zh-CN")
	if err != nil {
		t.Fatal(err)
	}
	var captured []ingress.Record
	handler := newHandler(t, ingress.Binding{Emit: func(_ context.Context, value ingress.Record) ingress.Attempt {
		captured = append(captured, value)
		return ingress.Attempt{Admitted: true}
	}})
	logger := stdslog.New(handler)
	if emission := framework.NewErrorLog(logger, presenter).Emit(context.Background(), known(t)); !emission.Recognized {
		t.Fatal("Framework failure not recognized")
	}
	logger.Error("joined", "presentation_issue", errors.Join(known(t), known(t)))
	if len(captured) != 2 {
		status, _ := handler.Status()
		t.Fatal("known Framework/public errors refused", status.LastError)
	}
	errorValue := captured[0].Fields[1].Value
	fields := fieldMap(errorValue.FieldsCopy())
	if fields["locale"] != "zh-CN" || fields["message"] == logging.Definitions()[0].Message {
		t.Fatal("localized projection lost")
	}
}
