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

package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/database/pgx/v5"
)

func FuzzSettingsPolicy(f *testing.F) {
	for _, seed := range []string{
		"{}",
		`{"max_connections":1,"max_rows":1,"max_result_bytes":1024,"max_message_bytes":1024}`,
		`{"max_connections":32,"queued_calls":64,"max_rows":65536,"max_result_bytes":16777216,"max_message_bytes":4194304}`,
		`{"timeout_ns":0,"max_idle_time_ns":0,"max_lifetime_ns":0}`,
		`{"max_connections":33}`, `{"timeout_ns":-1}`, `{"port":65536}`,
		`{"unknown":1}`, `{"plaintext":null}`, `{"max_rows":1,"max_rows":2}`,
		`{"password":"synthetic\u0000value"}`, `{"root_ca_pem":"invalid"}`,
		"null", "[]", "{} {}",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64<<10 {
			return
		}
		defaults := Settings{Name: "fuzz", Address: "127.0.0.1", Port: 1, Database: "fixture", User: "fixture", Password: "synthetic-only", Plaintext: true}
		prepared, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1, Defaults: defaults},
			[]configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: raw}})
		if err != nil {
			return
		}
		value, err := prepared.ValueCopy()
		if err != nil {
			t.Fatal("accepted settings were not readable")
		}
		expected := defaults
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&expected) != nil || decoder.Decode(new(any)) != io.EOF || !reflect.DeepEqual(value, expected) {
			t.Fatal("strict typed settings disagreed with independent JSON decoding")
		}
		validErr := Validate(value)
		policy, err := Recommend(value)
		if (validErr == nil) != (err == nil) {
			t.Fatal("validation and recommendation disagree")
		}
		if err != nil {
			return
		}
		runtime, err := adapters.New(context.Background(), policy.Runtime)
		if err != nil {
			t.Fatal("validated settings produced invalid runtime limits")
		}
		defer runtime.Close(context.Background())
		inbox, err := adapters.NewInbox[Result](policy.Evidence)
		if err != nil {
			t.Fatal("validated settings produced invalid evidence limits")
		}
		endpoint, err := bind(Dependencies{Runtime: runtime, Evidence: inbox})
		if err != nil {
			t.Fatal(err)
		}
		receipt, err := endpoint.Run(context.Background(), request("fuzz", "", policy.Budget.WorkBytes, policy.Budget.EvidenceBytes), func(call *adapters.Call[Result]) {
			if err := call.Resolve(adapters.Outcome[Result]{}); err != nil {
				t.Fatal(err)
			}
		})
		if err != nil {
			t.Fatal("recommended policy rejected its own declared root")
		}
		if snapshot, _ := receipt.Snapshot(); !snapshot.Info().Released {
			t.Fatal("finite policy probe retained work")
		}
		delivery, err := inbox.NextReleased(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSettings(t *testing.T) {
	peer := newProtocolPeer(t, true)
	original := peer.options()
	original.MaxConnections = 3
	original.MaxIdleTime = 24 * time.Millisecond
	original.MaxLifetime = 25 * time.Millisecond
	original.QueuedCalls = 2
	original.Timeout = 11 * time.Millisecond
	original.CloseTimeout = 12 * time.Millisecond
	original.MaxRows = 17
	original.MaxResultBytes = 8192
	original.MaxMessageBytes = 8192
	variants := []Settings{original}

	for _, expected := range variants {
		raw, err := json.Marshal(expected)
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1, Validate: func(_ context.Context, value Settings) error { return Validate(value) }}, []configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: raw}})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := prepared.ValueCopy()
		if err != nil || !reflect.DeepEqual(decoded, expected) {
			t.Fatal("typed settings changed", err)
		}
		public, internal := reflect.ValueOf(decoded), reflect.ValueOf(options(decoded))
		for index := 0; index < internal.NumField(); index++ {
			field := internal.Type().Field(index)
			if !field.IsExported() {
				continue
			}
			counterpart := public.FieldByName(field.Name)
			if !counterpart.IsValid() || !reflect.DeepEqual(counterpart.Interface(), internal.Field(index).Interface()) {
				t.Fatal("native setting omitted", field.Name)
			}
		}
		for index := 0; index < public.NumField(); index++ {
			field := public.Type().Field(index)
			if field.Tag.Get("json") == "" || field.Tag.Get("mapstructure") != field.Tag.Get("json") || !internal.FieldByName(field.Name).IsValid() {
				t.Fatal("public setting mapping missing", field.Name)
			}
		}
	}
	for _, raw := range []string{`{"unknown":1}`, `{"port":65536}`, `{"port":-1}`, `{"max_rows":1.5}`, `{"timeout_ns":"1s"}`, `{"plaintext":null}`, `{"max_connections":1,"max_connections":2}`} {
		if _, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1, Defaults: original}, []configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: []byte(raw)}}); err == nil {
			t.Fatal("malformed settings accepted")
		}
	}
	yaml, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1, Defaults: original}, []configsource.Layer{{Kind: configsource.Base, Encoding: configsource.YAML, Content: []byte("max_connections: 1\nmax_idle_time_ns: 0\nmax_lifetime_ns: 0\ntimeout_ns: 1000000\n")}})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := yaml.ValueCopy()
	if err != nil || decoded.MaxConnections != 1 || decoded.MaxIdleTime != 0 || decoded.Timeout != time.Millisecond || decoded.MaxLifetime != 0 {
		t.Fatal("zero/sentinel/unit semantics changed", err)
	}
	for _, change := range []func(*Settings){
		func(value *Settings) { value.Name = "Invalid" },
		func(value *Settings) { value.Address = "invalid" },
		func(value *Settings) { value.Port = 0 },
		func(value *Settings) { value.MaxConnections = 33 },
		func(value *Settings) { value.MaxRows = 65537 },
		func(value *Settings) { value.MaxResultBytes = 1023 },
		func(value *Settings) { value.Timeout = -1 },
		func(value *Settings) { value.CloseTimeout = time.Nanosecond },
		func(value *Settings) { value.QueuedCalls = 65 },
		func(value *Settings) { value.RootCAPEM = "invalid" },
	} {
		candidate := original
		change(&candidate)
		if err := Validate(candidate); err == nil {
			t.Fatal("invalid settings accepted")
		}
	}
}

func TestRecommendedDefaultsAndBounds(t *testing.T) {
	defaults := Settings{Name: "settings", Address: "127.0.0.1", Port: 1, Database: "settings", User: "user", Password: "secret", Plaintext: true}
	explicit := defaults
	explicit.MaxConnections = 4
	explicit.Timeout = 10 * time.Second
	explicit.CloseTimeout = 5 * time.Second
	explicit.MaxRows = 1024
	explicit.MaxResultBytes = 4 << 20
	explicit.MaxMessageBytes = 1 << 20
	before, err := Recommend(defaults)
	if err != nil {
		t.Fatal(err)
	}
	after, err := Recommend(explicit)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("zero defaults diverged from explicit values", err)
	}
	maximum := explicit
	maximum.MaxConnections = 32
	maximum.QueuedCalls = 64
	maximum.MaxRows = 65536
	maximum.MaxResultBytes = 16 << 20
	maximum.MaxMessageBytes = 4 << 20
	for _, input := range []Settings{defaults, maximum} {
		policy, err := Recommend(input)
		if err != nil {
			t.Fatal(err)
		}
		limits := native.LimitsV1(options(input))
		if policy.Budget.WorkBytes != limits.Bytes/int64(limits.Active)+64<<10 || policy.Runtime.MaxWorkBytes != sourceWorkBytes+int64(limits.Active)*policy.Budget.WorkBytes {
			t.Fatal("root budget mapping changed")
		}
		if policy.Runtime.MaxQueuedBytes != int64(limits.Queued)*policy.Budget.WorkBytes || policy.Evidence.Capacity != 1+limits.Active*familyRecords+limits.Queued || policy.Evidence.MaxBytes != sourceEvidenceBytes+int64(policy.Evidence.Capacity-1)*policy.Budget.EvidenceBytes {
			t.Fatal("queue/evidence budget mapping changed")
		}
		runtime, err := adapters.New(context.Background(), policy.Runtime)
		if err != nil {
			t.Fatal("invalid recommended runtime", err)
		}
		if _, err := adapters.NewInbox[Result](policy.Evidence); err != nil {
			t.Fatal("invalid recommended evidence", err)
		}
		if err := runtime.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEffectiveSettingsProfile(t *testing.T) {
	defaults := peerSettings()
	defaults.MaxConnections = 0
	defaults.MaxIdleTime = 0
	defaults.MaxLifetime = 0
	defaults.QueuedCalls = 0
	defaults.Timeout = 0
	defaults.CloseTimeout = 0
	defaults.MaxRows = 0
	defaults.MaxResultBytes = 0
	defaults.MaxMessageBytes = 0
	owner, _, _ := testOwner(t, defaults, 0)
	profile, err := owner.Client().Profile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	observed := make(map[string]string, len(profile.Options))
	for _, option := range profile.Options {
		observed[option.Name] = option.Value
	}
	expected := map[string]string{
		"max-connections": "4", "max-idle-time-ns": "0", "max-lifetime-ns": "0",
		"queued-calls": "0", "timeout-ns": "10000000000", "close-timeout-ns": "5000000000",
		"max-rows": "1024", "max-result-bytes": "4194304", "plaintext": "true",
		"max-message-bytes": "1048576",
	}
	for name, want := range expected {
		if observed[name] != want {
			t.Errorf("effective default %s: got %q, want %q", name, observed[name], want)
		}
	}
	if profile.ServiceVersion.Kind != "" || profile.ServiceVersion.Value != "" || profile.ServiceMode.Kind != "declared" {
		t.Fatal("local profile claimed service readiness")
	}
	profile.Options[0].Value = "changed"
	again, err := owner.Client().Profile(context.Background())
	if err != nil || again.Options[0].Value == "changed" {
		t.Fatal("profile aliases native source settings", err)
	}
}

func TestRecommendedQueueEvidence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		settings := Settings{Name: "settings", Address: "127.0.0.1", Port: 1, Database: "settings", User: "user", Password: "secret", Plaintext: true}
		settings.MaxConnections = 1
		settings.QueuedCalls = 64
		policy, err := Recommend(settings)
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
		endpoint, err := adapters.Bind(runtime, adapters.Declaration[Result]{Evidence: inbox, Copy: func(value Result) Result { return value }})
		if err != nil {
			t.Fatal(err)
		}
		var sourceGuard, workGuard adapters.Guard
		hold := func(target *adapters.Guard) func(*adapters.Call[Result]) {
			return func(call *adapters.Call[Result]) {
				var err error
				*target, err = call.Hold()
				if err != nil {
					t.Fatal(err)
				}
				if err = call.Resolve(adapters.Outcome[Result]{}); err != nil {
					t.Fatal(err)
				}
			}
		}
		if _, err := endpoint.Run(context.Background(), request("open", "", sourceWorkBytes, sourceEvidenceBytes), hold(&sourceGuard)); err != nil {
			t.Fatal(err)
		}
		var workScope adapters.Scope
		if _, err := endpoint.Run(context.Background(), request("query", "", policy.Budget.WorkBytes, policy.Budget.EvidenceBytes), func(call *adapters.Call[Result]) {
			workScope = call.Scope()
			hold(&workGuard)(call)
		}); err != nil {
			t.Fatal(err)
		}
		children := make([]adapters.Guard, familyRecords-1)
		for index := range children {
			if _, err := endpoint.Child(context.Background(), workScope, request("query", "", 0, policy.Budget.EvidenceBytes), hold(&children[index])); err != nil {
				t.Fatal(err)
			}
		}
		admission, cancel := context.WithCancel(context.Background())
		var dispatched atomic.Int64
		results := make(chan error, settings.QueuedCalls)
		for range settings.QueuedCalls {
			go func() {
				_, err := endpoint.Run(admission, request("query", "", policy.Budget.WorkBytes, policy.Budget.EvidenceBytes), func(call *adapters.Call[Result]) { dispatched.Add(1); _ = call.Resolve(adapters.Outcome[Result]{}) })
				results <- err
			}()
		}
		synctest.Wait()
		status, err := runtime.Inspect()
		if err != nil || status.Queued != settings.QueuedCalls {
			t.Error("recommended evidence prevented declared queue capacity", status.Queued, err)
		}
		if evidence, _ := inbox.Inspect(); evidence.Outstanding != policy.Evidence.Capacity {
			t.Error("live family plus queued roots did not occupy the exact recommendation")
		}
		if _, err := endpoint.Child(context.Background(), workScope, request("query", "", 0, policy.Budget.EvidenceBytes), func(call *adapters.Call[Result]) {
			dispatched.Add(1)
			_ = call.Resolve(adapters.Outcome[Result]{})
		}); !errors.Is(err, adapters.ErrEvidence) {
			t.Error("full evidence admitted additional work", err)
		}
		cancel()
		synctest.Wait()
		for range settings.QueuedCalls {
			if err := <-results; !errors.Is(err, context.Canceled) {
				t.Error("queued caller was not cancellation-bounded", err)
			}
		}
		if dispatched.Load() != 0 {
			t.Error("queued cancellation dispatched work")
		}
		if err := sourceGuard.Release(); err != nil {
			t.Error(err)
		}
		if err := workGuard.Release(); err != nil {
			t.Error(err)
		}
		for _, child := range children {
			if err := child.Release(); err != nil {
				t.Error(err)
			}
		}
		if err := runtime.Close(context.Background()); err != nil {
			t.Error(err)
		}
		for range 1 + familyRecords {
			delivery, err := inbox.NextReleased(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err = delivery.Ack(); err != nil {
				t.Fatal(err)
			}
		}
		if status, _ := inbox.Inspect(); status.Outstanding != 0 {
			t.Fatal("rejected queue retained evidence")
		}
	})
}
