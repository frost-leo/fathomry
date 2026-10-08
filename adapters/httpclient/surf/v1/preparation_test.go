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

package surf

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	utls "github.com/refraction-networking/utls"
)

func pointer[T any](value T) *T { return &value }
func testNative() NativeOptions { return NativeOptions{} }
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
func TestPreparationOfflineAndExplicitValues(t *testing.T) {
	var calls atomic.Int64
	native := NativeOptions{HelloSpecFactory: func(context.Context) (utls.ClientHelloSpec, error) {
		calls.Add(1)
		return utls.ClientHelloSpec{}, errors.New("unexpected")
	},
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			calls.Add(1)
			return nil, errors.New("unexpected")
		}}
	value := Settings{Name: "prepared", Mode: pointer(HTTP1Only), QueuedCalls: pointer(0), DisableCompression: pointer(false), NativeRetries: pointer(0), RetryDelay: pointer(time.Duration(0))}
	prepared, err := Prepare(value, native)
	if err != nil {
		t.Fatal(err)
	}
	deps := testMechanisms(t, prepared)
	owner, err := prepared.Open(testContext(t), deps)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(context.Background())
	profile, err := owner.Client().Profile(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	options := map[string]string{}
	for _, option := range profile.Options {
		options[option.Name] = option.Value
	}
	if calls.Load() != 0 || options["mode"] != "http1" || options["disable-compression"] != "false" || options["queued-calls"] != "0" || options["native-retry-delay-ns"] != "0" {
		t.Fatal("offline/native explicit values changed", options)
	}
	configured, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1, Defaults: Settings{Name: "loaded"}, Validate: func(_ context.Context, value Settings) error { return Validate(value) }}, []configsource.Layer{{Kind: configsource.Local, Encoding: configsource.JSON, Content: []byte("{\"disable_compression\":false,\"retry_delay_ns\":0,\"queued_calls\":0}")}})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := configured.ValueCopy()
	if err != nil || resolved.DisableCompression == nil || *resolved.DisableCompression || resolved.RetryDelay == nil || *resolved.RetryDelay != 0 || resolved.QueuedCalls == nil || *resolved.QueuedCalls != 0 {
		t.Fatal("strict load lost explicit values", err)
	}
	for _, bad := range []Settings{{Name: "empty", Mode: pointer(ProtocolMode(""))}, {Name: "zero", Timeout: pointer(time.Duration(0))}} {
		if _, err := Prepare(bad, NativeOptions{}); err == nil {
			t.Fatal("explicit invalid value defaulted")
		}
	}
	if _, err := Prepare(Settings{Name: "too-many", MaxActive: pointer(1024)}, NativeOptions{}); !errors.Is(err, ErrLimit) {
		t.Fatal("unrepresentable policy admitted", err)
	}
	if _, err := json.Marshal(prepared); !errors.Is(err, ErrSerialization) {
		t.Fatal("prepared runtime serialized", err)
	}
}
func TestPolicyInsufficiencyRefusesSource(t *testing.T) {
	prepared, err := Prepare(Settings{Name: "policy", MaxActive: pointer(1)}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	policy, _ := prepared.Policy()
	for _, field := range []string{"active", "work", "evidence-count", "evidence-bytes"} {
		t.Run(field, func(t *testing.T) {
			runtimeOptions, evidence := policy.Runtime, policy.Evidence
			switch field {
			case "active":
				runtimeOptions.MaxActive--
			case "work":
				runtimeOptions.MaxWorkBytes--
			case "evidence-count":
				evidence.Capacity--
			case "evidence-bytes":
				evidence.MaxBytes--
			}
			runtime, err := adapters.New(context.Background(), runtimeOptions)
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
				t.Fatal("insufficient policy acquired source", err)
			}
			status, _ := runtime.Inspect()
			if status.Accepted != 0 {
				t.Fatal("preconstruction refusal retained work")
			}
		})
	}
}
