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

package redis

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	native "github.com/frost-leo/fathomry/internal/cache/redis/v9"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestAllOptionsMapAndResolve(t *testing.T) {
	value := testSettings("127.0.0.1:1")
	// Reflective parity is a coverage guard: new Internal fields must be accounted
	// for explicitly here, including their strict public loading names.
	public := reflect.ValueOf(&value).Elem()
	typ := public.Type()
	for index := 0; index < public.NumField(); index++ {
		field := public.Field(index)
		decl := typ.Field(index)
		if decl.Tag.Get("json") == "" || decl.Tag.Get("json") != decl.Tag.Get("mapstructure") {
			t.Fatal("missing loading contract", decl.Name)
		}
		switch field.Kind() {
		case reflect.String:
			field.SetString("synthetic-" + decl.Name)
		case reflect.Bool:
			field.SetBool(true)
		case reflect.Int, reflect.Int64:
			field.SetInt(int64(index + 1))
		case reflect.Uint32:
			field.SetUint(1)
		case reflect.Slice:
			field.Set(reflect.ValueOf([]string{"sentinel-" + decl.Name}))
		case reflect.Map:
			field.Set(reflect.ValueOf(map[string]string{"shard": "127.0.0.1:9"}))
		case reflect.Pointer:
			idle := time.Duration(0)
			field.Set(reflect.ValueOf(&idle))
		default:
			t.Fatal("uncovered option kind")
		}
	}
	mapped := reflect.ValueOf(options(value))
	nativeType := mapped.Type()
	count := 0
	for index := 0; index < mapped.NumField(); index++ {
		field := nativeType.Field(index)
		if !field.IsExported() {
			continue
		}
		count++
		if field.Name == "MaxIdleTime" {
			continue
		}
		original := public.FieldByName(field.Name)
		if !original.IsValid() || !reflect.DeepEqual(original.Interface(), mapped.Field(index).Interface()) {
			t.Fatal("option lost", field.Name)
		}
	}
	if count != public.NumField() {
		t.Fatal("unsupported/unaccounted option added")
	}
	zero := time.Duration(0)
	for _, idle := range []*time.Duration{nil, &zero} {
		input := testSettings("127.0.0.1:1")
		input.MaxIdleTime = idle
		prepared, err := Prepare(input)
		if err != nil {
			t.Fatal(err)
		}
		policy, err := prepared.Policy()
		if err != nil {
			t.Fatal(err)
		}
		meta := prepared.native.Metadata()
		if policy.SourceWorkBytes != meta.SourceBytes || policy.Budget.EvidenceBytes != meta.EvidenceBytes+64<<10 {
			t.Fatal("metadata duplicated or undercharged")
		}
		owner, deps := openTest(t, input)
		profile, err := owner.Client().Profile(testContext(t))
		if err != nil {
			t.Fatal(err)
		}
		expected := "300000000000"
		if idle != nil {
			expected = "0"
		}
		found := false
		for _, option := range profile.Options() {
			if option.Name == "max-idle-ns" {
				found = option.Value == expected
			}
		}
		if !found {
			t.Fatal("explicit zero lost")
		}
		if status, _ := deps.Runtime.Inspect(); status.WorkBytes != meta.SourceBytes {
			t.Fatal("idle source not charged")
		}
	}
	// Internal overlay metadata must match actual resolution, not defaults.
	prepared, err := native.PrepareV1(testNativeSettings(), nil, resource.Layer{Kind: resource.Local, Content: []byte("max_commands: 3\nmax_active: 2\n")})
	if err != nil || prepared.Metadata().MaxCommands != 3 || prepared.Metadata().Limits.Active != 2 {
		t.Fatal("resolved envelope mismatch", err)
	}
}
func testNativeSettings() native.OptionsV1 { return options(testSettings("127.0.0.1:1")) }

func TestStrictLoadingFrozenPreparationAndUnsupportedProfiles(t *testing.T) {
	base := testSettings("127.0.0.1:1")
	schema := configsource.Schema[Settings]{Version: 1, Defaults: base, Validate: func(_ context.Context, value Settings) error { return Validate(value) }}
	loaded, err := configsource.Prepare(context.Background(), schema, []configsource.Layer{
		{Kind: configsource.Base, Encoding: configsource.YAML, Content: []byte("max_active: 1\nmax_idle_time_ns: 100\n")},
		{Kind: configsource.Local, Encoding: configsource.JSON, Content: []byte("{\"max_active\":2,\"max_idle_time_ns\":0,\"queued_calls\":0}")},
	})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := loaded.ValueCopy()
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := Prepare(resolved)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.metadata.Limits.Active != 2 || resolved.MaxIdleTime == nil || *resolved.MaxIdleTime != 0 {
		t.Fatal("overlay precedence/zero lost")
	}
	resolved.Addrs[0] = "127.0.0.1:9"
	frozen, _ := loaded.ValueCopy()
	if frozen.Addrs[0] != base.Addrs[0] {
		t.Fatal("configuration alias")
	}
	for _, doc := range []string{"unknown: true", "version: 2", "max_active: null", "max_idle_time_ns: -1", "max_active: 1\nmax_active: 2"} {
		if _, err := configsource.Prepare(context.Background(), schema, []configsource.Layer{{Kind: configsource.Local, Encoding: configsource.YAML, Content: []byte(doc)}}); err == nil {
			t.Fatal("invalid load accepted", doc)
		}
	}
	for name, change := range map[string]func(*Settings){
		"universal": func(v *Settings) { v.Mode = "universal" },
		"ring-auto": func(v *Settings) {
			v.Mode = "ring"
			v.Addrs = nil
			v.Shards = map[string]string{"one": "127.0.0.1:1"}
			v.ExperimentalAutoPipeline = true
		},
		"csc-cluster":          func(v *Settings) { v.Mode = "cluster"; v.ExperimentalCache = true },
		"csc-db":               func(v *Settings) { v.DB = 1; v.ExperimentalCache = true },
		"csc-resp":             func(v *Settings) { v.Protocol = 2; v.ExperimentalCache = true },
		"maintenance-sentinel": func(v *Settings) { v.Mode = "sentinel"; v.MasterName = "owned"; v.MaintenanceMode = "auto" },
		"maintenance-resp2":    func(v *Settings) { v.Protocol = 2; v.MaintenanceMode = "enabled" },
		"plaintext-tls":        func(v *Settings) { v.RootCAPEM = "synthetic" },
		"acl-empty":            func(v *Settings) { v.Username = "named" },
		"protocol-grant":       func(v *Settings) { v.AdminCommands = []string{"AUTH"} },
	} {
		t.Run(name, func(t *testing.T) {
			value := base
			change(&value)
			if err := Validate(value); err == nil {
				t.Fatal("unsupported combination accepted")
			}
		})
	}
	password, err := NewPassword("secret")
	if err != nil {
		t.Fatal(err)
	}
	rotating := base
	rotating.ExperimentalCache = true
	if _, err := PrepareWithPassword(rotating, password); !errors.Is(err, ErrUnsupported) {
		t.Fatal("CSC rotation accepted", err)
	}
	if err := password.Replace(strings.Repeat("x", 4097)); err == nil {
		t.Fatal("credential bound bypassed")
	}
	if reflect.TypeFor[Settings]().Implements(reflect.TypeFor[json.Marshaler]()) {
		t.Fatal("settings acquired runtime serialization")
	}
}

func TestComposedHeterogeneousBudgets(t *testing.T) {
	first, _ := Prepare(testSettings("127.0.0.1:1"))
	larger := testSettings("127.0.0.1:1")
	larger.MaxReplyBytes = 1 << 20
	larger.ExperimentalCache = true
	second, err := Prepare(larger)
	if err != nil {
		t.Fatal(err)
	}
	one, _ := Compose(first)
	both, err := Compose(first, second)
	if err != nil {
		t.Fatal(err)
	}
	if both.SourceWorkBytes != first.metadata.SourceBytes+second.metadata.SourceBytes ||
		both.Runtime.MaxWorkBytes < both.SourceWorkBytes+int64(both.Runtime.MaxActive-2)*both.Budget.WorkBytes ||
		both.Budget.WorkBytes <= one.Budget.WorkBytes {
		t.Fatal("overlap/native residence undercharged")
	}
	if _, err := Compose(Prepared{}); !errors.Is(err, ErrInput) {
		t.Fatal("zero preparation accepted")
	}
}

func FuzzSettings(f *testing.F) {
	f.Add([]byte("{\"max_idle_time_ns\":0}"))
	f.Add([]byte("null"))
	f.Add([]byte("{\"max_commands\":99999999999999999}"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}
		_, _ = configsource.Prepare(context.Background(), configsource.Schema[Settings]{Version: 1, Defaults: testSettings("127.0.0.1:1"),
			Validate: func(_ context.Context, value Settings) error { return Validate(value) }}, []configsource.Layer{{Kind: configsource.Local, Encoding: configsource.JSON, Content: data}})
	})
}
