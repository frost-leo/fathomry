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

package otel_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"testing"

	otel "github.com/frost-leo/fathomry/adapters/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/adapters/telemetry/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

var (
	_ *telemetry.Budget    = (*otel.Budget)(nil)
	_ *otel.Budget         = (*telemetry.Budget)(nil)
	_ *telemetry.Policy    = (*otel.Policy)(nil)
	_ *otel.Policy         = (*telemetry.Policy)(nil)
	_ *telemetry.Signal    = (*otel.Signal)(nil)
	_ *otel.Signal         = (*telemetry.Signal)(nil)
	_ *telemetry.Effect    = (*otel.Effect)(nil)
	_ *otel.Effect         = (*telemetry.Effect)(nil)
	_ *telemetry.LayerInfo = (*otel.LayerInfo)(nil)
	_ *otel.LayerInfo      = (*telemetry.LayerInfo)(nil)
	_ *telemetry.Option    = (*otel.Option)(nil)
	_ *otel.Option         = (*telemetry.Option)(nil)
)

func TestCategoryContractBudgetKeepsProviderAccounting(t *testing.T) {
	prepared, err := otel.Prepare(otel.Settings{Name: "category-contract", ServiceName: "fixture", LogsEndpoint: "http://127.0.0.1:1/logs"})
	if err != nil {
		t.Fatal(err)
	}
	metadata := prepared.Metadata()
	var nativeCosts otel.BudgetInfo = metadata
	var policy telemetry.Policy
	policy, err = prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	if policy.Budget.WorkBytes != nativeCosts.WorkBytes+2*nativeCosts.EvidenceBytes+64<<10 ||
		policy.Budget.EvidenceBytes != nativeCosts.EvidenceBytes+64<<10 ||
		policy.SourceWorkBytes != nativeCosts.SourceBytes+64<<10+4096 || policy.SourceEvidenceBytes != 64<<10 ||
		policy.Runtime.MaxActive != 1+nativeCosts.ActiveCalls || policy.Runtime.MaxQueued != nativeCosts.QueuedCalls ||
		policy.Runtime.MaxWorkBytes != policy.SourceWorkBytes+int64(nativeCosts.ActiveCalls)*policy.Budget.WorkBytes ||
		policy.Runtime.MaxQueuedBytes != int64(nativeCosts.QueuedCalls)*policy.Budget.WorkBytes ||
		policy.Evidence.Capacity != 1+nativeCosts.ActiveCalls+nativeCosts.QueuedCalls ||
		policy.Evidence.MaxBytes != policy.SourceEvidenceBytes+int64(policy.Evidence.Capacity-1)*policy.Budget.EvidenceBytes {
		t.Fatal("shared data aliases changed provider-owned accounting or added a hidden slot")
	}
	combined, err := otel.Compose(prepared, prepared)
	if err != nil || combined.Budget != policy.Budget || combined.SourceWorkBytes != 2*policy.SourceWorkBytes ||
		combined.SourceEvidenceBytes != 2*policy.SourceEvidenceBytes || combined.Runtime.MaxActive != 2*policy.Runtime.MaxActive ||
		combined.Runtime.MaxQueued != 2*policy.Runtime.MaxQueued || combined.Runtime.MaxWorkBytes != 2*policy.Runtime.MaxWorkBytes ||
		combined.Runtime.MaxQueuedBytes != 2*policy.Runtime.MaxQueuedBytes || combined.Evidence.Capacity != 2*policy.Evidence.Capacity ||
		combined.Evidence.MaxBytes != 2*policy.Evidence.MaxBytes {
		t.Fatal("provider Compose no longer accounts for both source lifetimes", err)
	}
	var originalName otel.Policy = policy
	if !reflect.DeepEqual(originalName, policy) || metadata != prepared.Metadata() {
		t.Fatal("shared policy changed native preparation or required a second interpretation")
	}
	encoded, err := json.Marshal(otel.Budget{WorkBytes: 17, EvidenceBytes: 23})
	if err != nil || string(encoded) != `{"work_bytes":17,"evidence_bytes":23}` {
		t.Fatal("existing plain Budget JSON changed", err)
	}
	if otel.Logs != telemetry.Logs || otel.Traces != telemetry.Traces || otel.Metrics != telemetry.Metrics ||
		otel.NotAttempted != telemetry.NotAttempted || otel.UnknownEffect != telemetry.UnknownEffect ||
		otel.Acknowledged != telemetry.Acknowledged || otel.PartialEffect != telemetry.PartialEffect {
		t.Fatal("provider signal/effect names no longer select the shared vocabulary")
	}
	if string(otel.Logs) != "logs" || string(otel.Traces) != "traces" || string(otel.Metrics) != "metrics" ||
		string(otel.NotAttempted) != "" || string(otel.UnknownEffect) != "unknown" ||
		string(otel.Acknowledged) != "acknowledged" || string(otel.PartialEffect) != "partial" {
		t.Fatal("existing signal/effect values changed")
	}
}

func TestCategoryContractCopiesAndProfileFactTypesRemainCompatible(t *testing.T) {
	original := otel.Info{Scope: "scope", Provider: otel.ProviderID, Name: "source", Revision: "revision", FormatVersion: 1,
		Provenance: []otel.LayerInfo{{Kind: 2, Fields: []string{"logs_endpoint"}}}}
	copy := original.Clone()
	copy.Provenance[0].Kind = 3
	copy.Provenance[0].Fields[0] = "changed"
	if original.Provenance[0].Kind != 2 || original.Provenance[0].Fields[0] != "logs_endpoint" {
		t.Fatal("provider Clone exposed shared provenance containers")
	}
	category := telemetry.Info(original).Clone()
	category.Provenance[0].Fields[0] = "category-changed"
	if original.Provenance[0].Fields[0] != "logs_endpoint" || otel.Info(telemetry.Info(original)).Name != original.Name {
		t.Fatal("shared Info conversion or copying lost the original source identity")
	}
	if (otel.Info{}).Clone().Provenance != nil {
		t.Fatal("nil provenance became present")
	}
	if (otel.Info{Provenance: []otel.LayerInfo{}}).Clone().Provenance == nil {
		t.Fatal("present empty provenance became absent")
	}
	if (otel.Info{Provenance: []otel.LayerInfo{{Fields: []string{}}}}).Clone().Provenance[0].Fields != nil {
		t.Fatal("existing empty Fields normalization changed")
	}
	fact := otel.Fact{Kind: "declared", Value: "native-mode"}
	profile := otel.Profile{ImplementationModule: "provider", SDKMode: "selected", ServiceMode: fact, ServiceVersion: fact, Protocol: fact, Native: fact,
		Options: []otel.Option{{Name: "signal", Value: "logs"}}}
	var extracted otel.Fact = profile.ServiceMode
	if extracted != fact || otel.Fact(telemetry.Fact(fact)) != fact {
		t.Fatal("Profile changed existing provider Fact field types or values")
	}
	cloned := profile.Clone()
	cloned.Options[0].Value = "changed"
	if profile.Options[0].Value != "logs" || (otel.Profile{}).Clone().Options != nil || (otel.Profile{Options: []otel.Option{}}).Clone().Options == nil {
		t.Fatal("provider Profile Clone changed option copying or nil/empty semantics")
	}
}

func TestCategoryContractObservationsKeepOriginalAttributionAndCause(t *testing.T) {
	attribution := otel.Attribution{Runtime: "runtime", Operation: "operation", ID: "correlation", Sequence: 17, Parent: 9, Depth: 2,
		Source: adapters.Source{Name: "source", Generation: 3}}
	sharedAttribution := telemetry.Attribution(attribution)
	if otel.Attribution(sharedAttribution) != attribution || sharedAttribution.Source != attribution.Source {
		t.Fatal("shared attribution conversion changed actual borrowed generation")
	}
	cause := errors.New("deliberate native cause")
	signal := otel.SignalResult{Signal: otel.Traces, Accepted: 7, Submitted: 6, Acknowledged: 4, Rejected: 2, TransportCalls: 1,
		Sampled: true, Effect: otel.PartialEffect, Err: cause}
	sharedSignal := telemetry.SignalResult(signal)
	if otel.SignalResult(sharedSignal) != signal || sharedSignal.Err != cause || sharedSignal.Signal != telemetry.Traces || sharedSignal.Effect != telemetry.PartialEffect {
		t.Fatal("shared observation changed native counts, effect or borrowed error identity")
	}
}

type categoryContractDiagnostic interface {
	fmt.Stringer
	fmt.GoStringer
	fmt.Formatter
	json.Marshaler
}

type categoryContractCause struct{ called *bool }

func (cause categoryContractCause) Error() string {
	*cause.called = true
	return "private-cause-canary"
}

func TestCategoryContractProviderDiagnosticsKeepOTelIdentity(t *testing.T) {
	called := false
	info := otel.Info{Name: "private-source-canary", Provenance: []otel.LayerInfo{{Fields: []string{"private-field-canary"}}}}
	attribution := otel.Attribution{ID: "private-id-canary"}
	signal := otel.SignalResult{Signal: otel.Logs, Err: categoryContractCause{called: &called}}
	fact := otel.Fact{Kind: "declared", Value: "private-fact-canary"}
	profile := otel.Profile{Native: fact, Options: []otel.Option{{Name: "option", Value: "private-option-canary"}}}
	for _, test := range []struct {
		name    string
		value   categoryContractDiagnostic
		pointer interface {
			json.Unmarshaler
			slog.LogValuer
		}
	}{
		{"info", info, &info},
		{"attribution", attribution, &attribution},
		{"signal", signal, &signal},
		{"fact", fact, &fact},
		{"profile", profile, &profile},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.value.String() != "otel[restricted]" || test.value.GoString() != "otel[restricted]" {
				t.Fatal("provider diagnostic inherited the shared category identity")
			}
			for _, format := range []string{"%s", "%v", "%+v", "%#v", "%q"} {
				if fmt.Sprintf(format, test.value) != "otel[restricted]" || fmt.Sprintf(format, test.pointer) != "otel[restricted]" {
					t.Fatal("ordinary provider formatting changed or exposed its fields")
				}
			}
			if test.pointer.LogValue().String() != "otel[restricted]" || slog.AnyValue(test.pointer).Resolve().String() != "otel[restricted]" {
				t.Fatal("provider slog identity changed")
			}
			if _, err := test.value.MarshalJSON(); !errors.Is(err, otel.ErrSerialization) {
				t.Fatal("direct marshal lost the OTel serialization facility", err)
			}
			if _, err := json.Marshal(test.value); !errors.Is(err, otel.ErrSerialization) {
				t.Fatal("JSON marshal lost the OTel serialization facility", err)
			}
			if err := test.pointer.UnmarshalJSON([]byte(`{"Name":"replacement"}`)); !errors.Is(err, otel.ErrSerialization) {
				t.Fatal("direct reconstruction lost the OTel serialization facility", err)
			}
			if err := json.Unmarshal([]byte(`{}`), test.pointer); !errors.Is(err, otel.ErrSerialization) {
				t.Fatal("JSON reconstruction lost the OTel serialization facility", err)
			}
			if called {
				t.Fatal("safe diagnostics invoked a borrowed native cause")
			}
		})
	}
	for _, value := range []slog.LogValuer{(*otel.Info)(nil), (*otel.Attribution)(nil), (*otel.SignalResult)(nil), (*otel.Fact)(nil), (*otel.Profile)(nil)} {
		if value.LogValue().String() != "otel[restricted]" || slog.AnyValue(value).Resolve().String() != "otel[restricted]" {
			t.Fatal("typed-nil provider slog behavior changed")
		}
	}
	if info.Name != "private-source-canary" || attribution.ID != "private-id-canary" || fact.Value != "private-fact-canary" || profile.Native != fact {
		t.Fatal("refused reconstruction mutated caller metadata")
	}
}
