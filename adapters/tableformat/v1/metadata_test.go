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

package tableformat_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/adapters/tableformat/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestContractTypesBelongToTableformat(t *testing.T) {
	for _, value := range []any{tableformat.Budget{}, tableformat.Policy{}, tableformat.Info{}, tableformat.LayerInfo{},
		tableformat.Attribution{}, tableformat.Attempts{}, tableformat.Phases{}, tableformat.NotSubmitted} {
		if reflect.TypeOf(value).PkgPath() != "github.com/frost-leo/fathomry/adapters/tableformat/v1" {
			t.Fatal("table-format contract is an alias of another public layer")
		}
	}
}

func TestMetadataClonePreservesIndependentStorage(t *testing.T) {
	original := tableformat.Info{Scope: "scope-canary", Provider: "provider-canary", Name: "name-canary", FormatVersion: 1,
		Revision: "revision-canary", Provenance: []tableformat.LayerInfo{{Kind: 1, Fields: []string{"credentials"}}, {Kind: 2, Fields: []string{}}}}
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for range 50 {
				copy := original.Clone()
				copy.Provenance[0].Fields[0] = "changed"
				copy.Provenance[0].Kind = 3
				copy.Provenance = append(copy.Provenance, tableformat.LayerInfo{})
			}
		})
	}
	workers.Wait()
	if original.Provenance[0].Fields[0] != "credentials" || original.Provenance[0].Kind != 1 || len(original.Provenance) != 2 {
		t.Fatal("metadata clones alias the original")
	}
	if (tableformat.Info{}).Clone().Provenance != nil || original.Clone().Provenance[1].Fields == nil ||
		(tableformat.Info{Provenance: []tableformat.LayerInfo{}}).Clone().Provenance == nil {
		t.Fatal("absent and present-empty provenance were conflated")
	}
}

func TestRuntimeMetadataPrivacy(t *testing.T) {
	values := []any{
		tableformat.Info{Name: "metadata-canary", Provenance: []tableformat.LayerInfo{{Fields: []string{"field-canary"}}}},
		tableformat.Attribution{SourceID: "source-canary", UseID: 41, ID: "id-canary", Source: adapters.Source{Name: "name-canary", Generation: 7}},
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
		if strings.Contains(output.String(), "canary") || !strings.Contains(output.String(), "tableformat[restricted]") {
			t.Fatal("structured metadata disclosure")
		}
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("runtime observation became durable data")
		}
	}
	for _, target := range []any{new(tableformat.Info), new(tableformat.Attribution)} {
		if err := json.Unmarshal([]byte(`{"Name":"payload-canary"}`), target); err == nil {
			t.Fatal("runtime reconstruction accepted")
		}
	}
}

func TestIndependentEffectAndPhaseFacts(t *testing.T) {
	seen := make(map[tableformat.Effect]bool)
	for _, effect := range []tableformat.Effect{tableformat.NotSubmitted, tableformat.Unknown, tableformat.Acknowledged, tableformat.Rejected} {
		if seen[effect] {
			t.Fatal("effect distinctions collapsed")
		}
		seen[effect] = true
	}
	fileEffect, commitEffect := tableformat.Acknowledged, tableformat.Unknown
	phases := tableformat.Phases{Staged: true, Uploaded: true, Submitted: true, Released: true}
	if fileEffect == commitEffect || phases.Reloaded {
		t.Fatal("acknowledged upload or release implied acknowledged/reloaded commit")
	}
	copy := phases
	copy.Reloaded = true
	if phases.Reloaded {
		t.Fatal("phase values share mutable state")
	}
	if (tableformat.Phases{}) != (tableformat.Phases{Staged: false, Uploaded: false, Submitted: false, Reloaded: false, Released: false}) {
		t.Fatal("zero phase claimed execution")
	}
	if value := (tableformat.Attempts{}); value.Observed != 0 || value.Exact {
		t.Fatal("unknown attempts became exact")
	}
	if value := (tableformat.Attribution{}); value.SourceID != "" || value.UseID != 0 || value.Source.Generation != 0 {
		t.Fatal("uncaptured source acquired identity")
	}
}

func TestDeclaredBudgetUnitsAndSourceLifetimes(t *testing.T) {
	policy := tableformat.Policy{
		Budget:   tableformat.Budget{WorkBytes: 17, EvidenceBytes: 29, SourceWorkBytes: 31, SourceEvidenceBytes: 43},
		Runtime:  adapters.Options{MaxActive: 2, MaxWorkBytes: 48},
		Evidence: adapters.EvidenceOptions{Capacity: 2, MaxBytes: 72},
	}
	copy := policy
	copy.Budget.SourceWorkBytes++
	copy.Runtime.MaxWorkBytes++
	if policy.Budget.SourceWorkBytes != 31 || policy.Runtime.MaxWorkBytes != 48 || policy.Budget.SourceEvidenceBytes != 43 {
		t.Fatal("policy copy changed source or runtime declaration")
	}
	encoded, err := json.Marshal(policy.Budget)
	if err != nil || string(encoded) != `{"work_bytes":17,"evidence_bytes":29,"source_work_bytes":31,"source_evidence_bytes":43}` {
		t.Fatal("declared byte units changed", err)
	}
	if (tableformat.Policy{}).Budget != (tableformat.Budget{}) {
		t.Fatal("undeclared source or operation charge was defaulted")
	}
}

func FuzzMetadataClone(f *testing.F) {
	f.Add("field", uint8(1))
	f.Add("", uint8(0))
	f.Fuzz(func(t *testing.T, field string, kind uint8) {
		if len(field) > 4096 {
			return
		}
		original := tableformat.Info{Provenance: []tableformat.LayerInfo{{Kind: kind, Fields: []string{field}}}}
		copy := original.Clone()
		copy.Provenance[0].Fields[0] += "changed"
		copy.Provenance[0].Kind++
		if original.Provenance[0].Fields[0] != field || original.Provenance[0].Kind != kind {
			t.Fatal("metadata clone changed input")
		}
	})
}
