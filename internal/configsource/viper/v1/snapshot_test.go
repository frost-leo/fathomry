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

package viper

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
)

func TestSnapshotContracts(t *testing.T) {
	t.Run("empty_environment_and_native_tags", func(t *testing.T) {
		t.Setenv("FATHOMRY_SNAPSHOT_PORT", "")
		for _, allowEmpty := range []bool{false, true} {
			input := readerInput("yml", "port: 8100")
			input.Options.AutomaticEnv = true
			input.Options.EnvPrefix = "FATHOMRY_SNAPSHOT"
			input.Options.AllowEmptyEnv = allowEmpty
			document := loadOne(t, input)
			type Fields struct{ Port int }
			type config struct {
				Fields `mapstructure:",squash"`
			}
			value, err := Decode[config](context.Background(), document)
			if err != nil || allowEmpty && value.Port != 0 || !allowEmpty && value.Port != 8100 {
				t.Fatal("native empty-env or squash semantics changed", err)
			}
		}
	})
	t.Run("dynamic_map_keys_are_explicit", func(t *testing.T) {
		t.Setenv("FATHOMRY_SNAPSHOT_LABELS_REGION", "selected")
		input := readerInput("yaml", "{}")
		input.Options.AutomaticEnv = true
		input.Options.EnvPrefix = "FATHOMRY_SNAPSHOT"
		input.Options.EnvKeyReplacements = []Replacement{{Old: ".", New: "_"}}
		document := loadOne(t, input)
		keys, err := document.Keys()
		if err != nil || len(keys) != 0 {
			t.Fatal("automatic environment was enumerated")
		}
		type config struct{ Labels map[string]string }
		value, err := Decode[config](context.Background(), document, "labels.region")
		if err != nil || value.Labels["region"] != "selected" {
			t.Fatal("explicit environment-only map key lost", err)
		}
	})
	t.Run("refusal_and_cancellation", func(t *testing.T) {
		document := loadOne(t, readerInput("yaml", "port: not-a-number"))
		type config struct{ Port int }
		if _, err := Decode[config](context.Background(), document); !errors.Is(err, ErrDecode) {
			t.Fatal("native conversion failure not retained")
		}
		type recursive struct{ Next *recursive }
		if _, err := Decode[recursive](context.Background(), document); !errors.Is(err, ErrInput) {
			t.Fatal("recursive schema admitted")
		}
		if _, err := document.Capture(nil); !errors.Is(err, ErrInput) {
			t.Fatal("nil context admitted")
		}
		ctx, cancel := context.WithCancelCause(context.Background())
		cause := errors.New("capture cancellation")
		cancel(cause)
		if _, err := document.Capture(ctx); !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
			t.Fatal("capture cancellation causes lost")
		}
		if _, err := document.Capture(context.Background(), "list.-1"); !errors.Is(err, ErrInput) {
			t.Fatal("unsafe native query admitted")
		}
		for _, options := range []OptionsV1{
			{Encoding: "yaml", EnvPrefix: "bad=prefix"},
			{Encoding: "yaml", EnvKeyReplacements: []Replacement{{New: "_"}}},
			{Encoding: "yaml", EnvKeyReplacements: []Replacement{{Old: ".", New: strings.Repeat("x", MaxKeyBytes+1)}}},
		} {
			if _, err := Load(context.Background(), []LoadInput{{Options: options, Reader: strings.NewReader("{}")}}); !errors.Is(err, ErrInput) {
				t.Fatal("invalid environment options admitted")
			}
		}
	})
	t.Run("privacy", func(t *testing.T) {
		replacement := Replacement{Old: "replacement-canary", New: "replacement-canary"}
		conformance.Runtime(t, replacement, new(Replacement), "replacement-canary")
		conformance.Runtime(t, &replacement, new(Replacement), "replacement-canary")
		snapshot, err := loadOne(t, readerInput("toml", "secret='snapshot-canary'")).Capture(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		conformance.Runtime(t, snapshot, new(Snapshot), "snapshot-canary")
		conformance.Private(t, (*Snapshot)(nil), "snapshot-canary")
	})
}
func TestAutomaticEnvironment(t *testing.T) {
	t.Run("schema_complete_snapshot", func(t *testing.T) {
		t.Setenv("FATHOMRY_AUTO_SERVICE_PORT", "8100")
		input := readerInput("yaml", "{}")
		input.Options.AutomaticEnv = true
		input.Options.EnvPrefix = "FATHOMRY_AUTO"
		input.Options.EnvKeyReplacements = []Replacement{{Old: ".", New: "_"}}
		documents, err := Load(context.Background(), []LoadInput{input})
		if err != nil {
			t.Fatal(err)
		}
		input.Options.EnvKeyReplacements[0].New = "mutated"
		type config struct {
			Service struct {
				Port int `mapstructure:"port"`
			} `mapstructure:"service"`
		}
		decoded, err := Decode[config](context.Background(), documents[0])
		if err != nil || decoded.Service.Port != 8100 {
			t.Fatal("environment-only struct field missing", err)
		}
		snapshot, err := documents[0].Capture(context.Background(), "service.port")
		if err != nil {
			t.Fatal(err)
		}
		t.Setenv("FATHOMRY_AUTO_SERVICE_PORT", "8200")
		captured, _ := snapshot.ValueCopy("service.port")
		live, _ := documents[0].ValueCopy("service.port")
		if captured != "8100" || live != "8200" {
			t.Fatal("captured/live semantics mixed")
		}
		values, err := snapshot.ValuesCopy()
		if err != nil {
			t.Fatal(err)
		}
		values["service"].(map[string]any)["port"] = "mutated"
		if captured, _ = snapshot.ValueCopy("service.port"); captured != "8100" {
			t.Fatal("snapshot alias escaped")
		}
		if string(documents[0].RawCopy()) != "{}" {
			t.Fatal("environment snapshot replaced raw document")
		}
		conformance.Runtime(t, snapshot, new(Snapshot), "FATHOMRY_AUTO")
		t.Setenv("FATHOMRY_AUTO_SERVICE_PORT", strings.Repeat("x", MaxDocumentBytes+1))
		if _, err := documents[0].Capture(context.Background(), "service.port"); !errors.Is(err, ErrLimit) {
			t.Fatal("captured environment bound ignored")
		}
	})
}
