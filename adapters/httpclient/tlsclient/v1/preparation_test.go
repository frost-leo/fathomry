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

package tlsclient

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bogdanfinn/tls-client/profiles"
	tls "github.com/bogdanfinn/utls"
	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func pointer[T any](value T) *T { return &value }
func testNative() NativeOptions {
	profile := profiles.Chrome_144
	return NativeOptions{Profile: &profile}
}
func testMechanisms(t *testing.T, prepared Prepared) Dependencies {
	t.Helper()
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return Dependencies{Runtime: runtime, Evidence: inbox}
}
func TestPreparationOfflineExactNativeAndExplicitValues(t *testing.T) {
	var calls atomic.Int64
	dependencies := testNative()
	base := *dependencies.Profile
	id := base.GetClientHelloId()
	id.SpecFactory = func() (tls.ClientHelloSpec, error) { calls.Add(1); return base.GetClientHelloSpec() }
	profile := profiles.NewClientProfile(id, base.GetSettings(), base.GetSettingsOrder(), base.GetPseudoHeaderOrder(), base.GetConnectionFlow(), base.GetPriorities(), base.GetHeaderPriority(),
		base.GetStreamID(), base.GetAllowHTTP(), base.GetHttp3Settings(), base.GetHttp3SettingsOrder(), base.GetHttp3PriorityParam(), base.GetHttp3PseudoHeaderOrder(), base.GetHttp3SendGreaseFrames())
	dependencies.Profile = &profile
	dependencies.DialContext = func(context.Context, string, string) (net.Conn, error) {
		calls.Add(1)
		return nil, errors.New("unexpected")
	}
	value := Settings{Name: "prepared", Mode: pointer(HTTP1Only), QueuedCalls: pointer(0), Bandwidth: pointer(false)}
	prepared, err := Prepare(value, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	dependencies.Profile = nil
	dependencies.DialContext = nil
	deps := testMechanisms(t, prepared)
	owner, err := prepared.Open(testContext(t), deps)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(context.Background())
	inspection, err := owner.Client().Profile(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	options := map[string]string{}
	for _, entry := range inspection.Options {
		options[entry.Name] = entry.Value
	}
	if calls.Load() != 0 || options["mode"] != "http1" || options["queued-calls"] != "0" || options["native-tls-tcp-bandwidth"] != "false" {
		t.Fatal("offline native selection or explicit values changed", options)
	}
	if _, err := Prepare(value, NativeOptions{}); !errors.Is(err, ErrInput) {
		t.Fatal("missing mandatory profile silently selected", err)
	}
	if err := Validate(value); err != nil {
		t.Fatal("data-only validation wrongly requires profile", err)
	}
	loaded, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1, Defaults: Settings{Name: "loaded"}, Validate: func(_ context.Context, value Settings) error { return Validate(value) }},
		[]configsource.Layer{{Kind: configsource.Local, Encoding: configsource.JSON, Content: []byte("{\"bandwidth\":false,\"queued_calls\":0}")}})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := loaded.ValueCopy()
	if err != nil || resolved.Bandwidth == nil || *resolved.Bandwidth || resolved.QueuedCalls == nil || *resolved.QueuedCalls != 0 {
		t.Fatal("strict loader lost absent/false/zero", err)
	}
	if _, err := Prepare(Settings{Name: "zero", Timeout: pointer(time.Duration(0))}, testNative()); err == nil {
		t.Fatal("explicit zero defaulted")
	}
	if _, err := Prepare(Settings{Name: "empty-mode", Mode: pointer(ProtocolMode(""))}, testNative()); err == nil {
		t.Fatal("explicit empty mode defaulted")
	}
	if _, err := Prepare(Settings{Name: "huge", MaxActive: pointer(1024)}, testNative()); !errors.Is(err, ErrLimit) {
		t.Fatal("unrepresentable source plus roots accepted", err)
	}
	if _, err := json.Marshal(prepared); !errors.Is(err, ErrSerialization) {
		t.Fatal("runtime selection serialized", err)
	}
}
func TestPolicyRefusesInsufficientCapacityBeforeConstruction(t *testing.T) {
	prepared, err := Prepare(Settings{Name: "policy", MaxActive: pointer(1)}, testNative())
	if err != nil {
		t.Fatal(err)
	}
	policy, _ := prepared.Policy()
	for _, field := range []string{"active", "work", "evidence-count", "evidence-bytes"} {
		t.Run(field, func(t *testing.T) {
			limits, evidence := policy.Runtime, policy.Evidence
			switch field {
			case "active":
				limits.MaxActive--
			case "work":
				limits.MaxWorkBytes--
			case "evidence-count":
				evidence.Capacity--
			case "evidence-bytes":
				evidence.MaxBytes--
			}
			runtime, err := adapters.New(context.Background(), limits)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close(context.Background())
			inbox, err := adapters.NewInbox[Result](evidence)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := prepared.Open(testContext(t), Dependencies{Runtime: runtime, Evidence: inbox})
			if owner != nil || !errors.Is(err, ErrLimit) {
				t.Fatal("insufficient policy constructed", err)
			}
			status, _ := runtime.Inspect()
			custody, _ := inbox.Inspect()
			if status.Accepted != 0 || custody.Outstanding != 0 {
				t.Fatal("refusal retained work")
			}
		})
	}
}
