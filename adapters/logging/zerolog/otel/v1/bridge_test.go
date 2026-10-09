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

package zerologotel_test

import (
	"context"
	"errors"
	bridge "github.com/frost-leo/fathomry/adapters/logging/zerolog/otel/v1"
	zerolog "github.com/frost-leo/fathomry/adapters/logging/zerolog/v1"
	otel "github.com/frost-leo/fathomry/adapters/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	"reflect"
	"testing"
	"time"
)

func TestAllSevenSeverityWireValuesWithoutTerminalActions(t *testing.T) {
	fixture := publicRecoverySetup(t, otel.Settings{LogsEndpoint: "enabled"}, false)
	prepared, err := zerolog.Prepare(zerolog.Settings{Name: "logging", Version: 1, MinLevel: recoveryPointer(zerolog.Trace), Sinks: []zerolog.Sink{{Name: "remote", Kind: "managed-record"}, {Name: "local", Kind: "writer"}}})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := prepared.Adopt(context.Background(), fixture.logger.Handle())
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(context.Background())
	levels := []zerolog.Level{zerolog.Trace, zerolog.Debug, zerolog.Info, zerolog.Warn, zerolog.Error, zerolog.Fatal, zerolog.Panic}
	for _, level := range levels {
		receipt, err := owner.Client().Log(context.Background(), level, string(level))
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := receipt.WaitReleased(context.Background())
		if err != nil || snapshot.Err() != nil {
			t.Fatal(err, snapshot.Err())
		}
		value, ok := snapshot.ValueCopy()
		if !ok || !value.SinksCopy()[0].Accepted || !value.SinksCopy()[1].Accepted {
			t.Fatal("severity silently filtered", level)
		}
		publicRecoveryDrainFinite(t, fixture.telemetryInbox, 1)
		publicRecoveryDrainFinite(t, fixture.loggerInbox, 3)
	}
	fixture.flush(t)
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if len(fixture.received) != 7 {
		t.Fatal("seven severity records missing")
	}
	numbers := []int32{1, 5, 9, 13, 17, 21, 24}
	for index, record := range fixture.received {
		if int32(record.SeverityNumber) != numbers[index] || record.SeverityText != string(levels[index]) {
			t.Fatal("severity mapping", index)
		}
	}
}

func TestBridgeRuntimeFenceUsesActualBorrowedBindingAndNeverMaintainsSink(t *testing.T) {
	fixture := publicRecoverySetup(t, otel.Settings{LogsEndpoint: "enabled", ActiveCalls: recoveryPointer(1)}, false)
	sink, err := bridge.New(fixture.telemetry.Client())
	if err != nil {
		t.Fatal(err)
	}
	if sink.CheckRuntime(fixture.telemetryRuntime) == nil || sink.CheckRuntime(fixture.loggerRuntime) != nil {
		t.Fatal("Runtime identity check")
	}
	kind := reflect.TypeOf(sink)
	for _, method := range []string{"Sync", "Close", "Rotate"} {
		if _, present := kind.MethodByName(method); present {
			t.Fatal("borrowed sink gained maintenance authority")
		}
	}
	scope, err := resource.New(context.Background(), resource.Options{Name: "borrow-review"})
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close(context.Background())
	ref, err := resource.Bind(scope, resource.Binding[int, zerolog.Handle]{Name: "logger", Policy: resource.Fixed,
		Select: func(settings.View) (int, error) { return 0, nil }, Clone: func(value int) int { return value },
		Build: func(context.Context, int) (*resource.Instance[zerolog.Handle], error) {
			return &resource.Instance[zerolog.Handle]{Value: fixture.logger.Handle()}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := settings.New(0, func(value int) int { return value })
	if err != nil {
		t.Fatal(err)
	}
	update, err := scope.Apply(context.Background(), snapshot.View())
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	policy, err := zerolog.Recommend(zerolog.Settings{Name: "logging", Version: 1, Sinks: []zerolog.Sink{{Name: "remote", Kind: "managed-record"}, {Name: "local", Kind: "writer"}}})
	if err != nil {
		t.Fatal(err)
	}
	alias := *fixture.telemetryRuntime
	client, err := zerolog.Using(context.Background(), ref, policy.Budget, zerolog.Dependencies{Runtime: &alias, Evidence: fixture.loggerInbox})
	if err != nil {
		t.Fatal(err)
	}
	alias = *fixture.loggerRuntime
	before := fixture.local.String()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	receipt, err := client.Log(ctx, zerolog.Info, "must-not-enter")
	if err != nil || receipt == nil {
		t.Fatal("public refusal evidence", err)
	}
	result, err := receipt.WaitReleased(ctx)
	if err != nil || !errors.Is(result.Err(), zerolog.ErrUnsupported) {
		t.Fatal("runtime substitution allowed", err, result.Err())
	}
	if _, present := result.ValueCopy(); present {
		t.Fatal("guard fabricated sink facts")
	}
	if fixture.local.String() != before {
		t.Fatal("guard allowed local effect")
	}
	status, _ := fixture.telemetryInbox.Inspect()
	if status.Outstanding != 1 {
		t.Fatal("guard entered telemetry")
	}
	publicRecoveryDrainFinite(t, fixture.loggerInbox, 2)
}

func TestBridgeCapturesStableFacadeAndRefusesFollow(t *testing.T) {
	fixture := publicRecoverySetup(t, otel.Settings{LogsEndpoint: "enabled"}, false)
	scope, err := resource.New(context.Background(), resource.Options{Name: "routing"})
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close(context.Background())
	ref, err := resource.Bind(scope, resource.Binding[int, otel.Handle]{Name: "target", Policy: resource.Follow, Select: func(settings.View) (int, error) { return 0, nil }, Clone: func(value int) int { return value }, Equal: func(left, right int) bool { return left == right }, Build: func(context.Context, int) (*resource.Instance[otel.Handle], error) {
		return &resource.Instance[otel.Handle]{Value: fixture.telemetry.Handle()}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := otel.Recommend(otel.Settings{Name: "target", ServiceName: "test", LogsEndpoint: "http://127.0.0.1:1/logs"})
	if err != nil {
		t.Fatal(err)
	}
	following, err := otel.Using(context.Background(), ref, policy.Budget, otel.Dependencies{Runtime: fixture.telemetryRuntime, Evidence: fixture.telemetryInbox})
	if err != nil {
		t.Fatal(err)
	}
	if sink, err := bridge.New(following); sink != nil || !errors.Is(err, zerolog.ErrUnsupported) {
		t.Fatal("following telemetry silently accepted")
	}
	original := *fixture.telemetry.Client()
	sink, err := bridge.New(&original)
	if err != nil {
		t.Fatal(err)
	}
	original = *following
	if sink.CheckRuntime(fixture.telemetryRuntime) == nil {
		t.Fatal("captured facade changed Runtime identity")
	}
	var zero otel.Client
	if sink, err := bridge.New(&zero); sink != nil || err == nil {
		t.Fatal("zero telemetry capability accepted")
	}
}
