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

package nethttp

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func pointer[T any](value T) *T { return &value }
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
func TestPreparationOfflineNativeAndExplicitZeros(t *testing.T) {
	var calls atomic.Int64
	value := Settings{Name: "prepared", HTTP2: pointer(false), ExpectContinueTimeout: pointer(time.Duration(0)),
		ProxyConnectHeader: map[string][]string{"X-Proxy": {"original"}}}
	native := NativeOptions{TLS: &tls.Config{ServerName: "original.invalid"}, DialContext: func(context.Context, string, string) (net.Conn, error) {
		calls.Add(1)
		return nil, errors.New("unexpected")
	}}
	prepared, err := Prepare(value, native)
	if err != nil {
		t.Fatal(err)
	}
	value.ProxyConnectHeader["X-Proxy"][0] = "changed"
	native.TLS.ServerName = "changed.invalid"
	deps := testMechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), deps)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(context.Background())
	profile, err := owner.Client().Profile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	options := map[string]string{}
	for _, option := range profile.Options {
		options[option.Name] = option.Value
	}
	if calls.Load() != 0 || options["expect-continue-timeout-ns"] != "0" || options["http2"] != "false" {
		t.Fatal("offline/context/native option semantics changed", options)
	}
	configured, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1, Defaults: Settings{Name: "loaded"}, Validate: func(_ context.Context, value Settings) error { return Validate(value) }},
		[]configsource.Layer{{Kind: configsource.Local, Encoding: configsource.JSON, Content: []byte("{\"http2\":false,\"expect_continue_timeout_ns\":0}")}})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := configured.ValueCopy()
	if err != nil || resolved.HTTP2 == nil || *resolved.HTTP2 || resolved.ExpectContinueTimeout == nil || *resolved.ExpectContinueTimeout != 0 {
		t.Fatal("strict loader lost explicit values", err)
	}
	if _, err := Prepare(Settings{Name: "limit", MaxActive: pointer(1024)}, NativeOptions{}); !errors.Is(err, ErrLimit) {
		t.Fatal("unrepresentable source plus roots accepted", err)
	}
	if _, err := Prepare(Settings{Name: "zero", Timeout: pointer(time.Duration(0))}, NativeOptions{}); err == nil {
		t.Fatal("explicit zero positive timeout defaulted")
	}
	if _, err := json.Marshal(prepared); !errors.Is(err, ErrSerialization) {
		t.Fatal("runtime configuration escaped", err)
	}
}
func TestInsufficientPolicyRejectsBeforeSourceConstruction(t *testing.T) {
	prepared, err := Prepare(Settings{Name: "policy", MaxActive: pointer(1)}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	policy, _ := prepared.Policy()
	for _, field := range []string{"active", "work", "tasks", "depth", "holds", "evidence-count", "evidence-bytes"} {
		t.Run(field, func(t *testing.T) {
			want := policy.Runtime
			evidence := policy.Evidence
			switch field {
			case "active":
				want.MaxActive--
			case "work":
				want.MaxWorkBytes--
			case "tasks":
				want.MaxTasks = 1
			case "depth":
				want.MaxDepth = 1
			case "holds":
				want.MaxHolds = 1
			case "evidence-count":
				evidence.Capacity--
			case "evidence-bytes":
				evidence.MaxBytes--
			}
			runtime, err := adapters.New(context.Background(), want)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close(context.Background())
			inbox, err := adapters.NewInbox[Result](evidence)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := prepared.Open(context.Background(), Dependencies{Runtime: runtime, Evidence: inbox})
			if owner != nil || !errors.Is(err, ErrLimit) {
				t.Fatal("insufficient policy acquired source", err)
			}
			stats, _ := runtime.Inspect()
			custody, _ := inbox.Inspect()
			if stats.Accepted != 0 || custody.Outstanding != 0 {
				t.Fatal("policy refusal retained work/evidence")
			}
		})
	}
}
func TestNativePrepareConflictsAndComposition(t *testing.T) {
	if _, err := Prepare(Settings{Name: "conflict", ProxyURL: "http://localhost:1"}, NativeOptions{Proxy: func(*http.Request) (*url.URL, error) { return nil, nil }}); err == nil {
		t.Fatal("ambiguous native proxy accepted")
	}
}

func TestPreparationRejectsInvalidUTF8WithoutRewriting(t *testing.T) {
	for _, value := range []Settings{
		{Name: "invalid", ServerName: "host-\xff.invalid"},
		{Name: "invalid", ProxyURL: "http://user-\xff@localhost:1"},
		{Name: "invalid", ProxyConnectHeader: map[string][]string{"X-Test": {"input-\xff"}}},
		{Name: "invalid", ProxyConnectHeader: map[string][]string{"X-\xff": {"value"}}},
	} {
		if _, err := Prepare(value, NativeOptions{}); !errors.Is(err, ErrInput) {
			t.Fatal("malformed UTF8 was accepted or rewritten", err)
		}
	}
	if _, err := Prepare(Settings{Name: "normal", ProxyConnectHeader: map[string][]string{"X-Test": {"synthetic"}}}, NativeOptions{}); err != nil {
		t.Fatal("valid control rejected", err)
	}
}
