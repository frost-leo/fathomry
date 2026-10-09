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

package zapotel_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	bridge "github.com/frost-leo/fathomry/adapters/logging/zap/otel/v1"
	zap "github.com/frost-leo/fathomry/adapters/logging/zap/v1"
	otel "github.com/frost-leo/fathomry/adapters/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	"go.uber.org/zap/zapcore"
)

type runtimeCheckedSink struct {
	sink   *bridge.Sink
	writes atomic.Int64
	syncs  atomic.Int64
}

func (sink *runtimeCheckedSink) CheckRuntime(runtime *adapters.Runtime) error {
	return sink.sink.CheckRuntime(runtime)
}
func (sink *runtimeCheckedSink) Write(ctx context.Context, record zap.Record) error {
	sink.writes.Add(1)
	return sink.sink.Write(ctx, record)
}
func (sink *runtimeCheckedSink) Sync(ctx context.Context) error {
	sink.syncs.Add(1)
	return sink.sink.Sync(ctx)
}

func TestUsingAndRetainRecheckActualBridgeRuntimeBeforeEffects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var requests atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		_, _ = io.Copy(io.Discard, request.Body)
		writer.Header().Set("Content-Type", "application/x-protobuf")
	}))
	t.Cleanup(peer.Close)
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	loggerSettings := zap.Settings{Name: "origin", Version: 1, Structured: true, Outputs: []zap.Output{{Name: "local", Kind: "file", Directory: directory}}}
	loggerPolicy, err := zap.Recommend(loggerSettings)
	if err != nil {
		t.Fatal(err)
	}
	active := 1
	telemetrySettings := otel.Settings{Name: "target", ServiceName: "review", LogsEndpoint: peer.URL + "/logs", ActiveCalls: &active}
	telemetryPolicy, err := otel.Recommend(telemetrySettings)
	if err != nil {
		t.Fatal(err)
	}
	telemetryPolicy.Runtime.Name = "same-display-name"
	// Admit a retained-family envelope so its source/runtime fence is exercised;
	// the source-plus-one-root count remains saturated by any nested root.
	telemetryPolicy.Runtime.MaxWorkBytes += loggerPolicy.FamilyWorkBytes
	telemetryRuntime, err := adapters.New(ctx, telemetryPolicy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	telemetryInbox, err := adapters.NewInbox[otel.Result](telemetryPolicy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	var telemetryOwner *otel.Owner
	var loggerOwner *zap.Owner
	var loggerRuntime *adapters.Runtime
	var loggerInbox *adapters.Inbox[zap.Result]
	var scope *resource.Scope
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if scope != nil {
			if err := scope.Close(cleanup); err != nil {
				t.Error(err)
			}
		}
		if loggerOwner != nil {
			if err := loggerOwner.Close(cleanup); err != nil {
				t.Error(err)
			}
		}
		if loggerRuntime != nil {
			if err := loggerRuntime.Close(cleanup); err != nil {
				t.Error(err)
			}
		}
		if telemetryOwner != nil {
			if err := telemetryOwner.Close(cleanup); err != nil {
				t.Error(err)
			}
		}
		if err := telemetryRuntime.Close(cleanup); err != nil {
			t.Error(err)
		}
		if loggerInbox != nil {
			if err := loggerInbox.Seal(); err != nil {
				t.Error(err)
			}
			for {
				delivery, err := loggerInbox.NextReleased(cleanup)
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Error(err)
					break
				}
				if err := delivery.Ack(); err != nil {
					t.Error(err)
				}
			}
		}
		if err := telemetryInbox.Seal(); err != nil {
			t.Error(err)
		}
		for {
			delivery, err := telemetryInbox.NextReleased(cleanup)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Error(err)
				break
			}
			if err := delivery.Ack(); err != nil {
				t.Error(err)
			}
		}
	})
	telemetryOwner, err = otel.Open(ctx, telemetrySettings, otel.Dependencies{Runtime: telemetryRuntime, Evidence: telemetryInbox})
	if err != nil {
		t.Fatal(err)
	}
	sink, err := bridge.New(telemetryOwner.Client())
	if err != nil {
		t.Fatal(err)
	}
	checked := &runtimeCheckedSink{sink: sink}
	loggerPolicy.Runtime.Name = "same-display-name"
	loggerRuntime, err = adapters.New(ctx, loggerPolicy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	loggerInbox, err = adapters.NewInbox[zap.Result](loggerPolicy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	loggerOwner, err = zap.Open(ctx, loggerSettings, zap.Dependencies{Runtime: loggerRuntime, Evidence: loggerInbox, Structured: checked})
	if err != nil {
		t.Fatal("independent construction Runtime was rejected", err)
	}
	scope, err = resource.New(ctx, resource.Options{Name: "runtime-review"})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := resource.Bind(scope, resource.Binding[int, zap.Handle]{Name: "logging", Policy: resource.Fixed,
		Select: func(view settings.View) (int, error) {
			value, err := settings.As[int](view)
			if err != nil {
				return 0, err
			}
			return value.ValueCopy()
		}, Clone: func(value int) int { return value },
		Build: func(context.Context, int) (*resource.Instance[zap.Handle], error) {
			return &resource.Instance[zap.Handle]{Value: loggerOwner.Handle()}, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := settings.New(0, func(value int) int { return value })
	if err != nil {
		t.Fatal(err)
	}
	update, err := scope.Apply(ctx, configuration.View())
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	alias := *telemetryRuntime
	using, err := zap.Using(ctx, ref, loggerPolicy.Budget, zap.Dependencies{Runtime: &alias, Evidence: loggerInbox})
	if err != nil {
		t.Fatal("Using should validate the actual captured generation at dispatch", err)
	}
	// Replacing the caller's wrapper must not rewrite the already-bound identity.
	alias = *loggerRuntime
	for _, operation := range []func(context.Context) (*adapters.Receipt[zap.Result], error){
		func(call context.Context) (*adapters.Receipt[zap.Result], error) {
			return using.Log(call, zapcore.InfoLevel, "must-not-write")
		}, using.Sync,
	} {
		call, stop := context.WithTimeout(context.Background(), 250*time.Millisecond)
		receipt, submit := operation(call)
		if submit != nil || receipt == nil {
			stop()
			t.Fatal("call did not retain independent pre-native refusal", submit)
		}
		snapshot, waitErr := receipt.WaitReleased(call)
		stop()
		if waitErr != nil || !errors.Is(snapshot.Err(), zap.ErrUnsupported) || errors.Is(snapshot.Err(), context.DeadlineExceeded) {
			t.Fatal("actual Runtime substitution was not refused promptly", waitErr, snapshot.Err())
		}
		if _, present := snapshot.ValueCopy(); present {
			t.Fatal("pre-native runtime refusal fabricated sink effects")
		}
	}
	retained, err := using.Retain(ctx)
	if retained != nil {
		_ = retained.Close(ctx)
		t.Fatal("unsafe Runtime substitution acquired a retained family")
	}
	if !errors.Is(err, zap.ErrUnsupported) {
		t.Fatal("Retain did not fence the captured source's composition", err)
	}
	data, err := os.ReadFile(filepath.Join(directory, "current.log"))
	if err != nil || len(data) != 0 || checked.writes.Load() != 0 || checked.syncs.Load() != 0 || requests.Load() != 0 {
		t.Fatal("unsafe Using entered a local or structured output", err, len(data), checked.writes.Load(), checked.syncs.Load(), requests.Load())
	}
	if status, err := telemetryInbox.Inspect(); err != nil || status.Outstanding != 1 {
		t.Fatal("refused logger dispatched telemetry despite composition guard", status, err)
	}
	receipt, err := loggerOwner.Client().Log(ctx, zapcore.InfoLevel, "healthy-independent-runtime")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil || snapshot.Err() != nil || checked.writes.Load() != 1 {
		t.Fatal("unsafe binding refusal corrupted the independent positive control", err, snapshot.Err())
	}
	delivery, err := telemetryInbox.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	flushed, err := telemetryOwner.Client().Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}
	exported, err := flushed.WaitReleased(ctx)
	if err != nil || exported.Err() != nil || requests.Load() != 1 {
		t.Fatal("independent target did not preserve healthy export progress", err, exported.Err(), requests.Load())
	}
}
