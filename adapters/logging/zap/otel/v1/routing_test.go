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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	bridge "github.com/frost-leo/fathomry/adapters/logging/zap/otel/v1"
	zap "github.com/frost-leo/fathomry/adapters/logging/zap/v1"
	otel "github.com/frost-leo/fathomry/adapters/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	collector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

func TestBridgeRefusesFollowingTelemetryBeforeAnyLoggingEffect(t *testing.T) {
	policy, err := otel.Recommend(otel.Settings{Name: "routing", Version: 1, ServiceName: "test", LogsEndpoint: "http://127.0.0.1:1/logs"})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[otel.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := resource.New(context.Background(), resource.Options{Name: "routing"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := scope.Close(ctx); err != nil {
			t.Error(err)
		}
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	clients := make(map[string]*otel.Client)
	var following resource.Ref[otel.Handle]
	for _, item := range []struct {
		name   string
		policy resource.Policy
		valid  bool
	}{{"follow", resource.Follow, false}, {"fixed", resource.Fixed, true}} {
		ref, err := resource.Bind(scope, resource.Binding[int, otel.Handle]{Name: item.name, Policy: item.policy,
			Select: func(settings.View) (int, error) { return 0, nil }, Clone: func(value int) int { return value }, Equal: func(left, right int) bool { return left == right },
			Build: func(context.Context, int) (*resource.Instance[otel.Handle], error) {
				return nil, errors.New("inert bridge unexpectedly constructed telemetry")
			}})
		if err != nil {
			t.Fatal(err)
		}
		if item.policy == resource.Follow {
			following = ref
		}
		client, err := otel.Using(context.Background(), ref, policy.Budget, otel.Dependencies{Runtime: runtime, Evidence: inbox})
		if err != nil {
			t.Fatal(err)
		}
		clients[item.name] = client
		sink, err := bridge.New(client)
		if item.valid {
			if err != nil || sink == nil {
				t.Error("stable Fixed routing refused", err)
			}
		} else if sink != nil || !errors.Is(err, zap.ErrUnsupported) {
			t.Error("Follow telemetry could silently retarget a retained logger", err)
		}
	}
	for _, client := range []*otel.Client{nil, new(otel.Client)} {
		if sink, err := bridge.New(client); sink != nil || !errors.Is(err, zap.ErrUnsupported) {
			t.Error("invalid telemetry capability acquired a bridge", err)
		}
	}
	otherRuntime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := otherRuntime.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	other, err := otel.Using(context.Background(), following, policy.Budget, otel.Dependencies{Runtime: otherRuntime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	supplied := *clients["fixed"]
	sink, err := bridge.New(&supplied)
	if err != nil {
		t.Fatal(err)
	}
	supplied = *other
	if !errors.Is(sink.CheckRuntime(runtime), zap.ErrUnsupported) || sink.CheckRuntime(otherRuntime) != nil {
		t.Error("caller facade overwrite changed frozen bridge routing/runtime identity")
	}
	if state, err := runtime.Inspect(); err != nil || state.Accepted != 0 || state.Active != 0 {
		t.Fatal("inert routing check acquired runtime work", err)
	}
	if state, err := inbox.Inspect(); err != nil || state.Outstanding != 0 {
		t.Fatal("inert routing check created accepted evidence", err)
	}
}

func TestFollowingLoggerRetainsDirectOrFixedTelemetryDestination(t *testing.T) {
	for _, targetMode := range []string{"direct", "fixed"} {
		t.Run(targetMode, func(t *testing.T) {
			type observed struct{ peer, message string }
			var mu sync.Mutex
			var records []observed
			peer := func(name string) *httptest.Server {
				server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					raw, err := io.ReadAll(request.Body)
					var decoded collector.ExportLogsServiceRequest
					if err != nil || proto.Unmarshal(raw, &decoded) != nil {
						t.Error("receiver could not independently decode OTLP", err)
						writer.WriteHeader(http.StatusBadRequest)
						return
					}
					mu.Lock()
					for _, resourceLogs := range decoded.ResourceLogs {
						for _, scope := range resourceLogs.ScopeLogs {
							for _, record := range scope.LogRecords {
								records = append(records, observed{name, record.Body.GetStringValue()})
							}
						}
					}
					mu.Unlock()
					writer.Header().Set("Content-Type", "application/x-protobuf")
				}))
				t.Cleanup(server.Close)
				return server
			}
			firstPeer, secondPeer := peer("first"), peer("second")
			var telemetryPreparations []otel.Prepared
			for index, endpoint := range []string{firstPeer.URL, secondPeer.URL} {
				prepared, err := otel.Prepare(otel.Settings{Name: fmt.Sprintf("telemetry-%d", index), Version: 1, ServiceName: "routing", LogsEndpoint: endpoint + "/logs"})
				if err != nil {
					t.Fatal(err)
				}
				telemetryPreparations = append(telemetryPreparations, prepared)
			}
			telemetryPolicy, err := otel.Compose(telemetryPreparations...)
			if err != nil {
				t.Fatal(err)
			}
			telemetryRuntime, err := adapters.New(context.Background(), telemetryPolicy.Runtime)
			if err != nil {
				t.Fatal(err)
			}
			telemetryInbox, err := adapters.NewInbox[otel.Result](telemetryPolicy.Evidence)
			if err != nil {
				t.Fatal(err)
			}
			telemetryDone := receiver(telemetryInbox)
			telemetryDeps := otel.Dependencies{Runtime: telemetryRuntime, Evidence: telemetryInbox}
			var telemetryOwners []*otel.Owner
			var fixedScope, loggerScope *resource.Scope
			var loggerRuntime *adapters.Runtime
			var loggerInbox *adapters.Inbox[zap.Result]
			var loggerDone <-chan error
			var held *zap.View
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if held != nil {
					if err := held.Close(ctx); err != nil {
						t.Error(err)
					}
				}
				if loggerScope != nil {
					if err := loggerScope.Close(ctx); err != nil {
						t.Error(err)
					}
				}
				if loggerRuntime != nil {
					if err := loggerRuntime.Close(ctx); err != nil {
						t.Error(err)
					}
				}
				if fixedScope != nil {
					if err := fixedScope.Close(ctx); err != nil {
						t.Error(err)
					}
				}
				for _, owner := range telemetryOwners {
					if err := owner.Close(ctx); err != nil {
						t.Error(err)
					}
				}
				if err := telemetryRuntime.Close(ctx); err != nil {
					t.Error(err)
				}
				if loggerInbox != nil {
					_ = loggerInbox.Seal()
					select {
					case err := <-loggerDone:
						if err != nil {
							t.Error(err)
						}
					case <-ctx.Done():
						t.Error("logger evidence did not finish")
					}
				}
				_ = telemetryInbox.Seal()
				select {
				case err := <-telemetryDone:
					if err != nil {
						t.Error(err)
					}
				case <-ctx.Done():
					t.Error("telemetry evidence did not finish")
				}
			})
			for _, prepared := range telemetryPreparations {
				owner, err := prepared.Open(context.Background(), telemetryDeps)
				if owner != nil {
					telemetryOwners = append(telemetryOwners, owner)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			clients := []*otel.Client{telemetryOwners[0].Client(), telemetryOwners[1].Client()}
			apply := func(scope *resource.Scope, revision int) {
				t.Helper()
				view, err := settings.New(revision, func(value int) int { return value })
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				update, err := scope.Apply(ctx, view.View())
				if err != nil {
					t.Fatal(err)
				}
				if err := update.Wait(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if targetMode == "fixed" {
				fixedScope, err = resource.New(context.Background(), resource.Options{Name: "fixed-telemetry"})
				if err != nil {
					t.Fatal(err)
				}
				for index := range telemetryOwners {
					ref, err := resource.Bind(fixedScope, resource.Binding[int, otel.Handle]{Name: fmt.Sprintf("target-%d", index), Policy: resource.Fixed,
						Select: func(view settings.View) (int, error) {
							snapshot, err := settings.As[int](view)
							if err != nil {
								return 0, err
							}
							return snapshot.ValueCopy()
						}, Clone: func(value int) int { return value },
						Build: func(_ context.Context, revision int) (*resource.Instance[otel.Handle], error) {
							owner := telemetryOwners[(index+revision)%len(telemetryOwners)]
							return &resource.Instance[otel.Handle]{Value: owner.Handle()}, nil
						}})
					if err != nil {
						t.Fatal(err)
					}
					clients[index], err = otel.Using(context.Background(), ref, telemetryPolicy.Budget, telemetryDeps)
					if err != nil {
						t.Fatal(err)
					}
				}
				apply(fixedScope, 0)
			}
			var sinks []*bridge.Sink
			for _, client := range clients {
				if !client.StableDestination() {
					t.Fatal("positive control did not select stable telemetry")
				}
				sink, err := bridge.New(client)
				if err != nil {
					t.Fatal(err)
				}
				sinks = append(sinks, sink)
			}
			loggerPrepared, err := zap.Prepare(zap.Settings{Name: "logging", Version: 1, Structured: true})
			if err != nil {
				t.Fatal(err)
			}
			loggerPolicy, err := zap.Compose(loggerPrepared, loggerPrepared)
			if err != nil {
				t.Fatal(err)
			}
			loggerRuntime, err = adapters.New(context.Background(), loggerPolicy.Runtime)
			if err != nil {
				t.Fatal(err)
			}
			loggerInbox, err = adapters.NewInbox[zap.Result](loggerPolicy.Evidence)
			if err != nil {
				t.Fatal(err)
			}
			loggerDone = receiver(loggerInbox)
			loggerScope, err = resource.New(context.Background(), resource.Options{Name: "following-logger"})
			if err != nil {
				t.Fatal(err)
			}
			ref, err := resource.Bind(loggerScope, resource.Binding[int, zap.Handle]{Name: "logging", Policy: resource.Follow,
				Select: func(view settings.View) (int, error) {
					snapshot, err := settings.As[int](view)
					if err != nil {
						return 0, err
					}
					return snapshot.ValueCopy()
				}, Clone: func(value int) int { return value }, Equal: func(left, right int) bool { return left == right },
				Build: func(ctx context.Context, revision int) (*resource.Instance[zap.Handle], error) {
					owner, err := loggerPrepared.Open(ctx, zap.Dependencies{Runtime: loggerRuntime, Evidence: loggerInbox, Structured: sinks[revision]})
					if owner == nil {
						return nil, err
					}
					return &resource.Instance[zap.Handle]{Value: owner.Handle(), Release: owner.Release}, err
				}})
			if err != nil {
				t.Fatal(err)
			}
			apply(loggerScope, 0)
			client, err := zap.Using(context.Background(), ref, loggerPolicy.Budget, zap.Dependencies{Runtime: loggerRuntime, Evidence: loggerInbox})
			if err != nil {
				t.Fatal(err)
			}
			held, err = client.Retain(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			complete := func(receipt *adapters.Receipt[zap.Result], err error) zap.Result {
				t.Helper()
				if err != nil || receipt == nil {
					t.Fatal("logger admission", err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				snapshot, err := receipt.WaitReleased(ctx)
				if err != nil || snapshot.Err() != nil {
					t.Fatal("logger outcome", err, snapshot.Err())
				}
				result, ok := snapshot.ValueCopy()
				if !ok || result.SinksCopy()[0].State != zap.Written {
					t.Fatal("structured destination did not accept the record")
				}
				return result
			}
			original := complete(held.Log(context.Background(), zapcore.InfoLevel, "held-before"))
			apply(loggerScope, 1)
			if fixedScope != nil {
				apply(fixedScope, 1)
			}
			retained := complete(held.Log(context.Background(), zapcore.InfoLevel, "held-after"))
			current := complete(client.Log(context.Background(), zapcore.InfoLevel, "new-after"))
			if retained.Attribution().Source.Generation != original.Attribution().Source.Generation || current.Attribution().Source.Generation == retained.Attribution().Source.Generation {
				t.Fatal("logger family did not retain its own source generation")
			}
			for _, owner := range telemetryOwners {
				receipt, err := owner.Client().Flush(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := receipt.WaitReleased(context.Background())
				if err != nil || snapshot.Err() != nil {
					t.Fatal("export", err, snapshot.Err())
				}
			}
			mu.Lock()
			defer mu.Unlock()
			want := map[observed]bool{{"first", "held-before"}: true, {"first", "held-after"}: true, {"second", "new-after"}: true}
			for _, record := range records {
				if !want[record] {
					t.Fatal("retained logger silently changed telemetry destination", record)
				}
				delete(want, record)
			}
			if len(records) != 3 || len(want) != 0 {
				t.Fatal("independent OTLP peers did not observe the expected destination history")
			}
		})
	}
}
