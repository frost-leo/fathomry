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

package doris

import (
	"context"
	"encoding/json"
	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/sqlengine/doris/v1"
	"reflect"
	"testing"
	"time"
)

func TestSettingsParityDefaultsAndBounds(t *testing.T) {
	base := Settings{Name: "test", SQLAddress: "127.0.0.1:1", HTTPOrigins: []string{"http://127.0.0.1:1"}, Database: "fixture", User: "user", Password: "synthetic", Plaintext: true}
	explicit := base
	explicit.Active = 4
	explicit.Timeout = 10 * time.Second
	explicit.MaxBatchBytes = 1 << 20
	explicit.MaxRows = 1024
	explicit.MaxResultBytes = 4 << 20
	explicit.MaxPacketBytes = 1 << 20
	explicit.MaxResponseBytes = 8 << 20
	explicit.MaxHTTPResponseBytes = 64 << 10
	explicit.CursorTimeout = time.Minute
	explicit.MaxPageRows = 128
	explicit.MaxPageBytes = 256 << 10
	explicit.MaxCursorRows = 65536
	before, err := Recommend(base)
	if err != nil {
		t.Fatal(err)
	}
	after, err := Recommend(explicit)
	if err != nil || before != after {
		t.Fatal("default drift", err)
	}
	maximum := explicit
	maximum.Active = 32
	maximum.Queued = 64
	maximum.MaxRows = 65536
	maximum.MaxPageRows = 65536
	maximum.MaxResultBytes = 16 << 20
	maximum.MaxPageBytes = 16 << 20
	maximum.MaxPacketBytes = 4 << 20
	maximum.MaxResponseBytes = 32 << 20
	maximum.MaxBatchBytes = 8 << 20
	maximum.MaxHTTPResponseBytes = 1 << 20
	maximum.CursorTimeout = time.Hour
	maximum.MaxCursorRows = 1 << 20
	for _, value := range []Settings{base, explicit, maximum, newSQLPeer(t, true).options()} {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1, Validate: func(_ context.Context, s Settings) error { return Validate(s) }},
			[]configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: raw}})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := prepared.ValueCopy()
		if err != nil || !reflect.DeepEqual(decoded, value) {
			t.Fatal("typed settings", err)
		}
		pub, internal := reflect.ValueOf(decoded), reflect.ValueOf(options(decoded))
		for index := range internal.NumField() {
			field := internal.Type().Field(index)
			if !field.IsExported() {
				continue
			}
			got := pub.FieldByName(field.Name)
			if !got.IsValid() || !reflect.DeepEqual(got.Interface(), internal.Field(index).Interface()) {
				t.Fatal("native option omitted", field.Name)
			}
		}
		for index := range pub.NumField() {
			field := pub.Type().Field(index)
			if field.Tag.Get("json") == "" || field.Tag.Get("json") != field.Tag.Get("mapstructure") || !internal.FieldByName(field.Name).IsValid() {
				t.Fatal("public option drift", field.Name)
			}
		}
		resolved, err := native.PrepareV1(options(decoded))
		if err != nil {
			t.Fatal(err)
		}
		policy, err := Recommend(decoded)
		if err != nil {
			t.Fatal(err)
		}
		budget := resolved.Reservation()
		if policy.Budget.WorkBytes < budget.WorkBytes+2*budget.EvidenceBytes || policy.Budget.EvidenceBytes < budget.EvidenceBytes {
			t.Fatal("undercharged resolution")
		}
		if policy.SourceWorkBytes != budget.SourceWorkBytes || policy.SourceEvidenceBytes != budget.SourceEvidenceBytes ||
			policy.Runtime.MaxWorkBytes != policy.SourceWorkBytes+int64(budget.Limits.Active)*policy.Budget.WorkBytes ||
			policy.Evidence.MaxBytes != policy.SourceEvidenceBytes+int64(policy.Evidence.Capacity-1)*policy.Budget.EvidenceBytes {
			t.Fatal("source costs differ from the resolved native reservation or are counted twice")
		}
		runtime, err := adapters.New(context.Background(), policy.Runtime)
		if err != nil {
			t.Fatal("invalid recommended policy", err)
		}
		if _, err := adapters.NewInbox[Result](policy.Evidence); err != nil {
			t.Fatal(err)
		}
		if err := runtime.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for _, raw := range []string{`{"unknown":1}`, `{"max_rows":1.5}`, `{"timeout_ns":"1s"}`, `{"plaintext":null}`, `{"active":1,"active":2}`} {
		if _, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1, Defaults: base},
			[]configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: []byte(raw)}}); err == nil {
			t.Fatal("malformed settings", raw)
		}
	}
	for _, mutate := range []func(*Settings){
		func(s *Settings) { s.SQLAddress = "localhost:1" }, func(s *Settings) { s.HTTPOrigins = []string{"http://host/path"} },
		func(s *Settings) { s.CursorTimeout = time.Hour + 1 }, func(s *Settings) { s.MaxCursorRows = 1<<20 + 1 },
		func(s *Settings) { s.MaxPageRows = 65537 }, func(s *Settings) { s.MaxPageBytes = 1023 },
		func(s *Settings) { s.MaxRows = -1 }, func(s *Settings) { s.Active = 33 }, func(s *Settings) { s.Queued = 65 },
		func(s *Settings) { s.RootCAPEM = "untrusted" }, func(s *Settings) { s.Plaintext = false },
	} {
		s := base
		mutate(&s)
		if err := Validate(s); err == nil {
			t.Fatal("invalid settings accepted")
		}
	}
}

func FuzzSettingsPolicy(f *testing.F) {
	for _, seed := range []string{"{}", `{"max_page_rows":1,"max_page_bytes":1024,"max_cursor_rows":10}`, `{"active":33}`, `{"cursor_timeout_ns":-1}`, `{"http_origins":[]}`, `{"max_response_bytes":1024}`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64<<10 {
			return
		}
		base := Settings{Name: "fuzz", SQLAddress: "127.0.0.1:1", Database: "fixture", User: "user", Plaintext: true}
		prepared, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1, Defaults: base},
			[]configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: raw}})
		if err != nil {
			return
		}
		value, err := prepared.ValueCopy()
		if err != nil {
			t.Fatal(err)
		}
		policy, err := Recommend(value)
		if (err == nil) != (Validate(value) == nil) {
			t.Fatal("validation drift")
		}
		if err != nil {
			return
		}
		runtime, err := adapters.New(context.Background(), policy.Runtime)
		if err != nil {
			t.Fatal(err)
		}
		defer runtime.Close(context.Background())
		if _, err := adapters.NewInbox[Result](policy.Evidence); err != nil {
			t.Fatal(err)
		}
	})
}
