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

package trino_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	trino "github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestConfigurationIsStrictOfflineAndPreservesUnits(t *testing.T) {
	peer := newWirePeer(t, func(http.ResponseWriter, *http.Request, []byte) {
		t.Error("offline settings performed application I/O")
	})
	defaults := peer.settings()
	defaults.Version = 0
	defaults.Timeout = 0
	schema, err := trino.Configuration(defaults)
	if err != nil {
		t.Fatal(err)
	}
	load := func(raw string) (trino.Settings, error) {
		prepared, err := configsource.Prepare(context.Background(), schema, []configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: []byte(raw)}})
		if err != nil {
			return trino.Settings{}, err
		}
		return prepared.ValueCopy()
	}
	resolved, err := load(`{}`)
	if err != nil || resolved.Version != 1 || resolved.Timeout <= 0 || resolved.MaxActive != defaults.MaxActive {
		t.Fatal("absent fields did not inherit authoritative defaults", err)
	}
	for _, raw := range []string{`{"max_active":0}`, `{"max_page_bytes":0}`, `{"timeout_ns":0}`, `{"cleanup_timeout_ns":0}`, `{"max_read_rows":0}`, `{"max_read_pages":0}`, `{"max_read_wire_bytes":0}`, `{"read_timeout_ns":0}`, `{"unknown":1}`, `{"version":2}`, `{"plaintext":null}`, `{"timeout_ns":"1s"}`, `{"max_active":1,"max_active":2}`, `{"max_active":1.5}`} {
		if _, err := load(raw); err == nil {
			t.Fatal("strict configuration accepted invalid explicit override", raw)
		}
	}
	changed, err := load(`{"max_active":1,"timeout_ns":1000000,"read_timeout_ns":2000000,"writes":false,"maintenance":false}`)
	if err != nil || changed.MaxActive != 1 || changed.Timeout != time.Millisecond || changed.ReadTimeout != 2*time.Millisecond || changed.Writes || changed.Maintenance {
		t.Fatal("typed settings changed explicit units or false values", err)
	}
	if trino.Validate(defaults) != nil {
		t.Fatal("direct zero-as-default settings unexpectedly rejected")
	}
	implicit, err := trino.Recommend(defaults)
	if err != nil {
		t.Fatal(err)
	}
	explicit, err := trino.Recommend(resolved)
	if err != nil || !reflect.DeepEqual(implicit, explicit) {
		t.Fatal("strict resolved and direct default budgets disagree", err)
	}
	if peer.readiness.Load() != 0 || peer.posts.Load() != 0 {
		t.Fatal("offline Validate/Configuration/Recommend constructed a source")
	}
	shape := reflect.TypeFor[trino.Settings]()
	for index := 0; index < shape.NumField(); index++ {
		field := shape.Field(index)
		if !field.IsExported() || field.Anonymous || field.Tag.Get("json") == "" || field.Tag.Get("mapstructure") != field.Tag.Get("json") {
			t.Fatal("loadable settings contain an opaque or unmapped field", field.Name)
		}
	}
}

func TestSettingsIntentionalSerializationAndRedactedPresentation(t *testing.T) {
	value := trino.Settings{Name: "source_canary", Endpoint: "https://endpoint_canary.invalid", User: "user_canary", Password: "password_canary", RootCAPEM: "root_canary", Catalog: "catalog_canary", Schema: "schema_canary"}
	raw, err := json.Marshal(value)
	if err != nil || !bytes.Contains(raw, []byte("password_canary")) {
		t.Fatal("intentional configuration serialization lost explicit credentials")
	}
	var decoded trino.Settings
	if err := json.Unmarshal(raw, &decoded); err != nil || !reflect.DeepEqual(value, decoded) {
		t.Fatal("typed settings serialization changed values", err)
	}
	var buffer bytes.Buffer
	slog.New(slog.NewJSONHandler(&buffer, nil)).Info("settings", "value", value, "pointer", &value)
	for _, display := range []string{fmt.Sprintf("%v", value), fmt.Sprintf("%+v", value), fmt.Sprintf("%#v", value), fmt.Sprintf("%+v", &value), buffer.String()} {
		if strings.Contains(display, "canary") {
			t.Fatal("default settings presentation exposed source or credentials")
		}
	}
}

func FuzzPublicConfiguration(f *testing.F) {
	for _, seed := range []string{`{}`, `{"version":1,"max_active":1}`, `{"max_rows":1048576,"max_read_rows":16777216}`, `{"max_read_pages":65536,"max_read_wire_bytes":4294967296}`, `{"timeout_ns":0}`, `{"read_timeout_ns":-1}`, `{"max_active":1,"max_active":2}`, `{"plaintext":null}`, `{"unknown":1}`, `null`, `[]`, `{} {}`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64<<10 {
			return
		}
		schema, err := trino.Configuration(trino.Settings{Name: "fuzz", Endpoint: "http://127.0.0.1:1", User: "fixture", Plaintext: true})
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := configsource.Prepare(context.Background(), schema, []configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: raw}})
		if err != nil {
			return
		}
		value, err := prepared.ValueCopy()
		if err != nil {
			t.Fatal(err)
		}
		expected := schema.Defaults
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&expected) != nil || decoder.Decode(new(any)) != io.EOF || !reflect.DeepEqual(value, expected) {
			t.Fatal("strict configuration differs from independent typed decoding")
		}
		if err := trino.Validate(value); err != nil {
			t.Fatal("strict accepted settings fail direct validation")
		}
		policy, err := trino.Recommend(value)
		if err != nil || policy.Budget.WorkBytes < 1 || policy.Budget.EvidenceBytes < 1 {
			t.Fatal("accepted settings produced invalid public budgets", err)
		}
		runtime, err := adapters.New(context.Background(), policy.Runtime)
		if err != nil {
			t.Fatal("recommended runtime bounds invalid", err)
		}
		if err := runtime.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := adapters.NewInbox[trino.Result](policy.Evidence); err != nil {
			t.Fatal("recommended evidence bounds invalid", err)
		}
	})
}
