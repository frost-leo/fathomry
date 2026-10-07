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

package sqlengine_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/adapters/sqlengine/v1"
)

func TestContractTypesBelongToSQLengine(t *testing.T) {
	for _, value := range []any{sqlengine.Budget{}, sqlengine.Policy{}, sqlengine.Info{}, sqlengine.LayerInfo{},
		sqlengine.Attribution{}, sqlengine.Attempts{}, sqlengine.Profile{}, sqlengine.Fact{}, sqlengine.Option{}} {
		if reflect.TypeOf(value).PkgPath() != "github.com/frost-leo/fathomry/adapters/sqlengine/v1" {
			t.Fatal("SQL-engine contract is an alias of another public layer")
		}
	}
}

func TestMetadataSnapshots(t *testing.T) {
	original := sqlengine.Info{Scope: "scope-canary", Provider: "provider-canary", Name: "name-canary", FormatVersion: 1, Revision: "revision-canary",
		Provenance: []sqlengine.LayerInfo{{Kind: 1, Fields: []string{"password"}}, {Kind: 2, Fields: []string{}}}}
	profile := sqlengine.Profile{SDKMode: "mode-canary", ServiceVersion: sqlengine.Fact{Kind: "observed", Value: "version-canary"},
		Options: []sqlengine.Option{{Name: "option-canary", Value: "value-canary"}}}
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for range 50 {
				copy := original.Clone()
				copy.Provenance[0].Fields[0] = "changed"
				copy.Provenance = append(copy.Provenance, sqlengine.LayerInfo{})
				snapshot := profile.Clone()
				snapshot.Options[0].Value = "changed"
			}
		})
	}
	workers.Wait()
	if original.Provenance[0].Fields[0] != "password" || len(original.Provenance) != 2 || profile.Options[0].Value != "value-canary" {
		t.Fatal("metadata clones share mutable storage")
	}
	if (sqlengine.Info{}).Clone().Provenance != nil || (sqlengine.Profile{}).Clone().Options != nil ||
		original.Clone().Provenance[1].Fields == nil || (sqlengine.Profile{Options: []sqlengine.Option{}}).Clone().Options == nil {
		t.Fatal("absent and present empty metadata were conflated")
	}
}

func TestRuntimeMetadataPrivacy(t *testing.T) {
	values := []any{
		sqlengine.Info{Name: "payload-canary", Provenance: []sqlengine.LayerInfo{{Fields: []string{"field-canary"}}}},
		sqlengine.Attribution{ID: "id-canary"},
		sqlengine.Profile{Options: []sqlengine.Option{{Value: "option-canary"}}},
		sqlengine.Fact{Value: "fact-canary"},
	}
	for _, value := range append([]any(nil), values...) {
		pointer := reflect.New(reflect.TypeOf(value))
		pointer.Elem().Set(reflect.ValueOf(value))
		values = append(values, pointer.Interface())
	}
	for _, value := range values {
		if strings.Contains(fmt.Sprintf("%v %+v %#v", value, value, value), "canary") {
			t.Fatal("implicit metadata disclosure")
		}
		var output bytes.Buffer
		slog.New(slog.NewJSONHandler(&output, nil)).Info("metadata", "value", value)
		if strings.Contains(output.String(), "canary") || !strings.Contains(output.String(), "sqlengine[restricted]") {
			t.Fatal("structured metadata disclosure")
		}
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("runtime observation became durable data")
		}
	}
	for _, target := range []any{new(sqlengine.Info), new(sqlengine.Attribution), new(sqlengine.Profile), new(sqlengine.Fact)} {
		if err := json.Unmarshal([]byte(`{"Value":"payload-canary"}`), target); err == nil {
			t.Fatal("runtime reconstruction accepted")
		}
	}
}

func TestUnknownFactsAndPolicyAreNotExecutionEvidence(t *testing.T) {
	if value := (sqlengine.Attempts{}); value.Exact || value.Observed != 0 {
		t.Fatal("unknown attempts became exact")
	}
	if value := (sqlengine.Fact{}); value.Kind != "" || value.Value != "" {
		t.Fatal("unknown fact acquired a value")
	}
	if value := (sqlengine.Policy{}); value.SourceWorkBytes != 0 || value.SourceEvidenceBytes != 0 {
		t.Fatal("undeclared source charges were defaulted")
	}
	encoded, err := json.Marshal(sqlengine.Budget{WorkBytes: 17, EvidenceBytes: 29})
	if err != nil || string(encoded) != `{"work_bytes":17,"evidence_bytes":29}` {
		t.Fatal("declared budget units changed", err)
	}
}

func FuzzMetadataClone(f *testing.F) {
	f.Add("field", "option")
	f.Add("", "秘密")
	f.Fuzz(func(t *testing.T, field, value string) {
		if len(field) > 4096 || len(value) > 4096 {
			return
		}
		original := sqlengine.Info{Provenance: []sqlengine.LayerInfo{{Fields: []string{field}}}}
		cloned := original.Clone()
		cloned.Provenance[0].Fields[0] += "changed"
		profile := sqlengine.Profile{Options: []sqlengine.Option{{Name: field, Value: value}}}
		copy := profile.Clone()
		copy.Options[0].Value += "changed"
		if original.Provenance[0].Fields[0] != field || profile.Options[0].Value != value {
			t.Fatal("clone modified input")
		}
	})
}
