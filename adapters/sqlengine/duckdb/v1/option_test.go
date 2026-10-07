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

package duckdb

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/sqlengine/duckdb/v2"
)

func TestCompleteSettingsMappingAndPurePreparation(t *testing.T) {
	selected := Settings{Name: "settings", Path: filepath.Join(t.TempDir(), "missing", "never-opened.duckdb"),
		Connections: 3, QueuedCalls: 2, Threads: 2, MemoryBytes: 64 << 20, Timeout: 17 * time.Millisecond,
		CleanupTimeout: 19 * time.Millisecond, MaxRows: 37, MaxBatchRows: 41, InputBytes: 4096, ResultBytes: 8192,
		ReaderChunkRows: 7, ReaderChunkBytes: 4096, ReaderTotalRows: 113, ReaderTotalBytes: 16384, ReaderLifetime: 23 * time.Second}
	raw, err := json.Marshal(selected)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1,
		Validate: func(_ context.Context, value Settings) error { return Validate(value) }},
		[]configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: raw}})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := prepared.ValueCopy()
	if err != nil || !reflect.DeepEqual(decoded, selected) {
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
			t.Fatal("native option omitted", field.Name)
		}
	}
	for index := 0; index < public.NumField(); index++ {
		field := public.Type().Field(index)
		if field.Tag.Get("json") == "" || field.Tag.Get("mapstructure") != field.Tag.Get("json") || !internal.FieldByName(field.Name).IsValid() {
			t.Fatal("public setting mapping missing", field.Name)
		}
	}
	policy, err := Recommend(decoded)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := native.PrepareV1(options(decoded))
	if err != nil || !reflect.DeepEqual(policy, policyFor(resolved)) {
		t.Fatal("recommendation drifted from authoritative preparation", err)
	}
	if _, err := os.Stat(filepath.Dir(selected.Path)); !os.IsNotExist(err) {
		t.Fatal("pure preparation touched native file parent", err)
	}
	for _, raw := range []string{`{"unknown":1}`, `{"connections":null}`, `{"max_rows":1.5}`, `{"timeout_ns":"1s"}`, `{"max_rows":1,"max_rows":2}`, `[]`, `null`, `{} {}`} {
		if _, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1, Defaults: selected}, []configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: []byte(raw)}}); err == nil {
			t.Fatal("malformed settings accepted")
		}
	}
	yaml, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1, Defaults: selected},
		[]configsource.Layer{{Kind: configsource.Base, Encoding: configsource.YAML, Content: []byte("queued_calls: 0\ntimeout_ns: 1000000\nreader_chunk_rows: 5\n")}})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err = yaml.ValueCopy()
	if err != nil || decoded.QueuedCalls != 0 || decoded.Timeout != time.Millisecond || decoded.ReaderChunkRows != 5 {
		t.Fatal("zero/units or YAML overlay mapping changed", err)
	}
	for _, change := range []func(*Settings){
		func(value *Settings) { value.Name = "Invalid" },
		func(value *Settings) { value.Path = "relative.duckdb" },
		func(value *Settings) { value.Path += "?threads=32" },
		func(value *Settings) { value.Connections = 9 },
		func(value *Settings) { value.QueuedCalls = 65 },
		func(value *Settings) { value.Threads = 33 },
		func(value *Settings) { value.MemoryBytes = (16 << 20) - 1 },
		func(value *Settings) { value.Timeout = time.Nanosecond },
		func(value *Settings) { value.CleanupTimeout = -1 },
		func(value *Settings) { value.MaxRows = 65537 },
		func(value *Settings) { value.MaxBatchRows = 65537 },
		func(value *Settings) { value.InputBytes = 1023 },
		func(value *Settings) { value.ResultBytes = 1023 },
		func(value *Settings) { value.ReaderChunkRows = -1 },
		func(value *Settings) { value.ReaderChunkBytes = -1 },
		func(value *Settings) { value.ReaderTotalRows = -1 },
		func(value *Settings) { value.ReaderTotalBytes = -1 },
		func(value *Settings) { value.ReaderLifetime = -1 },
	} {
		candidate := selected
		change(&candidate)
		if Validate(candidate) == nil {
			t.Fatal("invalid configuration accepted")
		}
		if _, err := Recommend(candidate); err == nil {
			t.Fatal("invalid configuration obtained a policy")
		}
	}
}

func TestProfileOwnsEffectiveSettings(t *testing.T) {
	owner, _, _ := testOwner(t, testSettings(), 0)
	profile, err := owner.Client().Profile(context.Background())
	if err != nil || profile.Native.Kind != "observed" || profile.Native.Value != "v1.5.5" || profile.ServiceMode.Value != "memory" {
		t.Fatal("profile lost actual linked core or mode", err)
	}
	actual := make(map[string]string)
	for _, option := range profile.Options {
		actual[option.Name] = option.Value
	}
	for name, want := range map[string]string{"connections": "2", "threads": "1", "memory-bytes": "67108864", "external-access": "disabled", "spill": "disabled", "extensions": "disabled"} {
		if actual[name] != want {
			t.Fatal("effective native option changed", name)
		}
	}
	profile.Options[0].Value = "changed"
	again, err := owner.Client().Profile(context.Background())
	if err != nil || again.Options[0].Value == "changed" {
		t.Fatal("profile retained caller-mutable storage", err)
	}
}

func TestConfigurationPreservesAbsentVersusExplicitZero(t *testing.T) {
	schema, err := Configuration(Settings{Name: "typed"})
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{`{}`, `{"queued_calls":0}`, `{"path":""}`} {
		prepared, err := configsource.Prepare(context.Background(), schema,
			[]configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: []byte(raw)}})
		if err != nil {
			t.Fatal("absent fields or supported explicit zero refused", err)
		}
		value, err := prepared.ValueCopy()
		if err != nil || value.Connections != 1 || value.MemoryBytes != 256<<20 || value.ReaderChunkRows != 256 || value.ReaderTotalRows != 1000000 || value.ReaderLifetime != 5*time.Minute {
			t.Fatal("resolved defaults were not authoritative", err)
		}
	}
	for _, field := range []string{"connections", "threads", "memory_bytes", "timeout_ns", "cleanup_timeout_ns", "max_rows", "max_batch_rows", "input_bytes", "result_bytes", "reader_chunk_rows", "reader_chunk_bytes", "reader_total_rows", "reader_total_bytes", "reader_lifetime_ns"} {
		raw := []byte(`{"` + field + `":0}`)
		if _, err := configsource.Prepare(context.Background(), schema,
			[]configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: raw}}); err == nil {
			t.Fatal("strict explicit zero was silently re-defaulted", field)
		}
	}
}

func FuzzSettingsPolicy(f *testing.F) {
	for _, seed := range []string{`{}`, `{"connections":8,"queued_calls":64}`, `{"reader_chunk_rows":257,"reader_total_rows":70000}`, `{"reader_total_bytes":9223372036854775807}`, `{"unknown":1}`, `{"memory_bytes":-1}`, `{"timeout_ns":null}`, `{"max_rows":1,"max_rows":2}`, `null`, `[]`, `{} {}`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64<<10 {
			return
		}
		defaults := Settings{Name: "fuzz"}
		prepared, err := configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1, Defaults: defaults},
			[]configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: raw}})
		if err != nil {
			return
		}
		actual, err := prepared.ValueCopy()
		if err != nil {
			t.Fatal(err)
		}
		expected := defaults
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&expected) != nil || decoder.Decode(new(any)) != io.EOF || !reflect.DeepEqual(actual, expected) {
			t.Fatal("strict typed settings disagreed with independent decoding")
		}
		policy, policyErr := Recommend(actual)
		if (Validate(actual) == nil) != (policyErr == nil) {
			t.Fatal("validation and recommendation disagree")
		}
		if policyErr != nil {
			return
		}
		runtime, err := adapters.New(context.Background(), policy.Runtime)
		if err != nil {
			t.Fatal("resolved settings produced invalid operation limits")
		}
		if _, err := adapters.NewInbox[Result](policy.Evidence); err != nil {
			t.Fatal("resolved settings produced invalid evidence limits")
		}
		if err := runtime.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}
