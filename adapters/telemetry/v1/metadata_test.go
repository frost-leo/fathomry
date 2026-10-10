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

package telemetry_test

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/adapters/telemetry/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestInfoCloneDetachesEveryProvenanceContainer(t *testing.T) {
	value := telemetry.Info{Scope: "scope", Provider: "provider", Name: "source", Revision: "revision", FormatVersion: 2,
		Provenance: []telemetry.LayerInfo{{Kind: 1, Fields: []string{"timeout", "queue"}}, {Kind: 2, Fields: []string{"resource"}}}}
	copy := value.Clone()
	if !reflect.DeepEqual(copy, value) {
		t.Fatal("clone changed frozen metadata")
	}
	copy.Provenance[0].Fields[0] = "changed-copy"
	copy.Provenance[1].Kind = 9
	value.Provenance[1].Fields[0] = "changed-source"
	if value.Provenance[0].Fields[0] != "timeout" || value.Provenance[1].Kind != 2 || copy.Provenance[1].Fields[0] != "resource" {
		t.Fatal("clone aliases outer or nested provenance storage")
	}
	if (telemetry.Info{}).Clone().Provenance != nil {
		t.Fatal("nil provenance changed to present empty provenance")
	}
	empty := telemetry.Info{Provenance: []telemetry.LayerInfo{}}
	if result := empty.Clone(); result.Provenance == nil || len(result.Provenance) != 0 {
		t.Fatal("present empty provenance changed to absent")
	}
	for _, fields := range [][]string{nil, {}} {
		original := telemetry.Info{Provenance: []telemetry.LayerInfo{{Kind: 3, Fields: fields}}}
		result := original.Clone()
		if len(result.Provenance) != 1 || result.Provenance[0].Kind != 3 || result.Provenance[0].Fields != nil {
			t.Fatal("empty layer fields did not retain their established nil normalization")
		}
		if !reflect.DeepEqual(original.Provenance[0].Fields, fields) {
			t.Fatal("clone mutated original field presence")
		}
	}
}

func TestSignalEffectAndAttributionRemainIndependentFacts(t *testing.T) {
	if telemetry.Logs != "logs" || telemetry.Traces != "traces" || telemetry.Metrics != "metrics" ||
		telemetry.NotAttempted != "" || telemetry.UnknownEffect != "unknown" || telemetry.Acknowledged != "acknowledged" || telemetry.PartialEffect != "partial" {
		t.Fatal("public signal/effect values changed")
	}
	seen := make(map[telemetry.Effect]bool)
	for _, effect := range []telemetry.Effect{telemetry.NotAttempted, telemetry.UnknownEffect, telemetry.Acknowledged, telemetry.PartialEffect} {
		if seen[effect] {
			t.Fatal("different effect observations collapsed")
		}
		seen[effect] = true
	}
	var calls atomic.Int32
	cause := &privateCause{calls: &calls}
	value := telemetry.SignalResult{Signal: telemetry.Traces, Accepted: 2, Submitted: 3, Acknowledged: 5, Rejected: 7, TransportCalls: 11, Sampled: true, Effect: telemetry.PartialEffect, Err: cause}
	if value.Err != cause || value.Accepted != 2 || value.Submitted != 3 || value.Acknowledged != 5 || value.Rejected != 7 || value.TransportCalls != 11 || !value.Sampled || value.Effect != telemetry.PartialEffect || calls.Load() != 0 {
		t.Fatal("signal result normalized unrelated counts or inspected the borrowed cause")
	}
	origin := telemetry.Attribution{Runtime: "runtime", Operation: "operation", ID: "id", Sequence: 13, Parent: 17, Depth: 2, Source: adapters.Source{Name: "source", Generation: 19}}
	copy := origin
	copy.Source.Generation = 23
	if origin.Source.Generation != 19 || origin.Sequence != 13 || origin.Parent != 17 || origin.Depth != 2 || (telemetry.Attribution{}).Source != (adapters.Source{}) {
		t.Fatal("attribution changed generation, lineage or zero-source semantics")
	}
	for _, value := range []any{telemetry.Signal(""), telemetry.Effect(""), telemetry.SignalResult{}, telemetry.Info{}, telemetry.LayerInfo{}, telemetry.Attribution{}, telemetry.Fact{}, telemetry.Option{}} {
		if reflect.TypeOf(value).PkgPath() != "github.com/frost-leo/fathomry/adapters/telemetry/v1" {
			t.Fatal("shared telemetry shape is an implementation alias")
		}
	}
}

type privateCause struct{ calls *atomic.Int32 }

func (value *privateCause) Error() string { value.calls.Add(1); panic("private error presentation") }
func (value *privateCause) Unwrap() error { value.calls.Add(1); panic("private error traversal") }
func (value *privateCause) Format(fmt.State, rune) {
	value.calls.Add(1)
	panic("private error formatting")
}
func (value *privateCause) LogValue() slog.Value { value.calls.Add(1); panic("private error logging") }

func TestRuntimeMetadataRedactsWithoutInvokingBorrowedErrors(t *testing.T) {
	const canary = "telemetry-private-canary"
	var calls atomic.Int32
	values := []any{
		telemetry.Info{Scope: canary, Provider: canary, Name: canary, Revision: canary, Provenance: []telemetry.LayerInfo{{Fields: []string{canary}}}},
		telemetry.Attribution{Runtime: canary, Operation: canary, ID: canary, Source: adapters.Source{Name: canary, Generation: 1}},
		telemetry.Fact{Kind: canary, Value: canary},
		telemetry.SignalResult{Signal: telemetry.Signal(canary), Effect: telemetry.Effect(canary), Err: &privateCause{calls: &calls}},
	}
	for _, value := range values {
		pointer := reflect.New(reflect.TypeOf(value))
		pointer.Elem().Set(reflect.ValueOf(value))
		for _, candidate := range []any{value, pointer.Interface()} {
			for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
				if got := fmt.Sprintf(format, candidate); got != "telemetry[restricted]" {
					t.Fatal("ordinary metadata formatting disclosed or reclassified a value", got)
				}
			}
			if got := slog.AnyValue(candidate).Resolve().String(); got != "telemetry[restricted]" {
				t.Fatal("metadata structured logging changed", got)
			}
			if data, err := json.Marshal(candidate); err == nil || len(data) != 0 || strings.Contains(err.Error(), canary) {
				t.Fatal("runtime metadata serialized or leaked during refusal")
			}
		}
		if err := json.Unmarshal([]byte(`{"Name":"changed","Value":"changed"}`), pointer.Interface()); err == nil || !reflect.DeepEqual(pointer.Elem().Interface(), value) {
			t.Fatal("metadata reconstruction was accepted or mutated its destination")
		}
		if err := json.Unmarshal([]byte(`null`), pointer.Interface()); err == nil || !reflect.DeepEqual(pointer.Elem().Interface(), value) {
			t.Fatal("null reconstructed runtime metadata")
		}
	}
	for _, value := range []any{(*telemetry.Info)(nil), (*telemetry.Attribution)(nil), (*telemetry.Fact)(nil), (*telemetry.SignalResult)(nil)} {
		if got := slog.AnyValue(value).Resolve().String(); got != "telemetry[restricted]" {
			t.Fatal("nil metadata logging is not safe", got)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("diagnostics invoked a borrowed native error")
	}
}
