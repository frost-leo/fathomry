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

package temporal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	orchestration "github.com/frost-leo/fathomry/adapters/orchestration/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	sdk "go.temporal.io/sdk/client"
)

func categoryDependencies(t *testing.T, policy orchestration.Policy) Dependencies {
	t.Helper()
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal("shared policy cannot construct the public runtime", err)
	}
	t.Cleanup(func() {
		wait, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := runtime.Close(wait); err != nil {
			t.Error("category runtime cleanup", err)
		}
	})
	operations, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	workers, err := adapters.NewInbox[WorkerResult](policy.Workers)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := adapters.NewInbox[TaskResult](policy.Tasks)
	if err != nil {
		t.Fatal(err)
	}
	return Dependencies{Runtime: runtime, Evidence: operations, Workers: workers, Tasks: tasks}
}

func categoryCloseOwner(t *testing.T, owner *Owner) {
	t.Helper()
	t.Cleanup(func() {
		wait, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := owner.Close(wait); err != nil {
			t.Error("category source cleanup", err)
		}
	})
}

func TestCategoryPolicyPreservesLegacyLiteralsAndSharedConsumption(t *testing.T) {
	legacy := Policy{
		Budget: Budget{WorkBytes: 4096, WorkerBytes: 8192, EvidenceBytes: 1024},
		Runtime: adapters.Options{MaxActive: 2, MaxQueued: 1, MaxWorkBytes: 16384,
			MaxQueuedBytes: 8192, MaxTasks: 3, MaxDepth: 2, MaxHolds: 1},
		Evidence:        adapters.EvidenceOptions{Capacity: 3, MaxBytes: 3072},
		Workers:         adapters.EvidenceOptions{Capacity: 2, MaxBytes: 2048},
		Tasks:           adapters.EvidenceOptions{Capacity: 4, MaxBytes: 4096},
		SourceWorkBytes: 2048, SourceEvidenceBytes: 1024,
		NativeWorkBytes: 1024, NativeWorkerBytes: 2048,
	}
	var shared orchestration.Policy = legacy
	var roundtrip Policy = shared
	var sharedBudget orchestration.Budget = legacy.Budget
	var legacyBudget Budget = sharedBudget
	if roundtrip != legacy || legacyBudget != legacy.Budget {
		t.Fatal("category aliases changed an existing exported policy field")
	}
	encoded, err := json.Marshal(legacyBudget)
	if err != nil || string(encoded) != `{"work_bytes":4096,"worker_bytes":8192,"evidence_bytes":1024}` {
		t.Fatal("existing Budget JSON field names changed", err)
	}
	encoded, err = json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	var decoded orchestration.Policy
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != shared {
		t.Fatal("shared policy lost its data-only round trip", err)
	}
	dependencies := categoryDependencies(t, shared)
	actual, err := dependencies.Runtime.Options()
	if err != nil || actual != shared.Runtime {
		t.Fatal("shared runtime declaration changed during consumption", err)
	}
	for _, entry := range []struct {
		want adapters.EvidenceOptions
		read func() (adapters.EvidenceOptions, error)
	}{
		{shared.Evidence, dependencies.Evidence.Options},
		{shared.Workers, dependencies.Workers.Options},
		{shared.Tasks, dependencies.Tasks.Options},
	} {
		actual, err := entry.read()
		if err != nil || actual != entry.want {
			t.Fatal("shared policy conflated independent result receivers", err)
		}
	}
}

func TestCategoryPolicyKeepsExactFrozenNativeLimits(t *testing.T) {
	for _, queued := range []int{0, 3} {
		for _, workerBytes := range []int64{0, 4 << 20} {
			t.Run(fmt.Sprintf("queued-%d-worker-%d", queued, workerBytes), func(t *testing.T) {
				settings := policySettings()
				settings.Endpoint, settings.Lazy = "127.0.0.1:1", true
				settings.QueuedCalls, settings.WorkerWorkBytes = queued, workerBytes
				prepared, err := Prepare(settings, NativeOptions{})
				if err != nil {
					t.Fatal(err)
				}
				var shared orchestration.Policy
				shared, err = prepared.Policy()
				if err != nil {
					t.Fatal(err)
				}
				limits := prepared.nativeLimits()
				operationBytes := int64(settings.MaxRequestBytes) + int64(settings.MaxResponseBytes) + (16 << 10)
				familyBytes := int64(settings.FamilyLimit) * operationBytes
				effectiveWorkerBytes := max(workerBytes, familyBytes)
				if shared.NativeWorkBytes != familyBytes || shared.NativeWorkerBytes != effectiveWorkerBytes ||
					limits.Active != settings.MaxActive || limits.Queued != queued ||
					limits.Bytes != int64(settings.MaxActive)*effectiveWorkerBytes || limits.QueuedBytes != int64(queued)*effectiveWorkerBytes ||
					limits.MaxLeases != settings.FamilyLimit {
					t.Fatal("native admission lost its exact provider reservation")
				}
				if shared.Runtime.MaxWorkBytes <= limits.Bytes || shared.Budget.WorkBytes <= shared.NativeWorkBytes {
					t.Fatal("public bridge custody was collapsed into the native envelope")
				}
				altered := shared
				altered.NativeWorkBytes, altered.NativeWorkerBytes = 1, 1
				altered.Budget.WorkBytes, altered.Runtime.MaxActive = 1, 1
				detached := prepared.Settings()
				detached.FamilyLimit, detached.WorkerWorkBytes = 1024, 1<<39
				again, err := prepared.Policy()
				if err != nil || again != shared || prepared.nativeLimits() != limits || altered == again {
					t.Fatal("caller-owned policy/settings data changed frozen source authority", err)
				}
				dependencies := categoryDependencies(t, shared)
				owner, err := prepared.Open(context.Background(), dependencies)
				if owner != nil {
					categoryCloseOwner(t, owner)
				}
				if err != nil || owner == nil {
					t.Fatal("shared policy could not open its lazily constructed native source", err)
				}
				report := owner.state.assembly.Snapshot()
				if len(report.Sources) != 1 || report.Sources[0].Limits != limits {
					t.Fatal("Open installed public bridge limits instead of exact native limits")
				}
			})
		}
	}
}

func TestCategoryPolicyKeepsQueuedCeilingAtPolicyBeforeNativeConstruction(t *testing.T) {
	probe := &preparationProbe{}
	settings := policySettings()
	settings.MaxActive, settings.QueuedCalls, settings.WorkerWorkBytes = 1, 1024, 1<<38
	prepared, err := Prepare(settings, NativeOptions{Plugins: []sdk.Plugin{probe}})
	if err != nil || probe.calls.Load() != 0 {
		t.Fatal("category migration moved public policy validation or invoked native construction", err)
	}
	if _, err := prepared.Policy(); !errors.Is(err, ErrLimit) {
		t.Fatal("unrepresentable queued-byte envelope escaped Policy", err)
	}
	if owner, err := prepared.Open(context.Background(), Dependencies{}); owner != nil || !errors.Is(err, ErrLimit) || probe.calls.Load() != 0 {
		t.Fatal("Open entered dependencies/native construction before Policy refusal", err)
	}
	if _, err := (Prepared{}).Policy(); !errors.Is(err, ErrState) {
		t.Fatal("zero preparation gained a category policy", err)
	}
}

func TestCategoryPolicySharedDerivationPreservesParentScopeAndEnvelope(t *testing.T) {
	parent := newCapabilityFixture(t, &testServer{}, NativeOptions{})
	probe := &preparationProbe{}
	parentLimits := parent.owner.state.prepared.nativeLimits()
	tooManyActive := int(parentLimits.Bytes/parent.owner.state.prepared.metadata.WorkBytes) + 1
	if _, err := PrepareFromExisting(Settings{Name: "category-over-parent", Namespace: "child", MaxActive: tooManyActive,
		FamilyLimit: 3, InnerEvidenceCapacity: 8}, NativeOptions{Plugins: []sdk.Plugin{probe}}, parent.owner.Client()); !errors.Is(err, ErrInput) || probe.calls.Load() != 0 {
		t.Fatal("shared preparation used a public bridge envelope instead of the parent's native bound", err)
	}
	prepared, err := PrepareFromExisting(Settings{Name: "category-child", Namespace: "child", MaxActive: 1,
		QueuedCalls: 1, FamilyLimit: 3, InnerEvidenceCapacity: 8}, NativeOptions{Plugins: []sdk.Plugin{probe}}, parent.owner.Client())
	if err != nil || probe.calls.Load() != 0 {
		t.Fatal("shared preparation changed native entry timing", err)
	}
	var shared orchestration.Policy
	shared, err = prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	if prepared.nativeLimits().Bytes >= parentLimits.Bytes {
		t.Fatal("test did not distinguish parent and derived native envelopes")
	}
	dependencies := categoryDependencies(t, shared)
	view, err := parent.owner.Client().WithID("category-parent-view")
	if err != nil {
		t.Fatal(err)
	}
	peer, err := parent.owner.Client().Borrow(parent.ctx)
	if err != nil {
		t.Fatal(err)
	}
	child, err := prepared.OpenFrom(parent.ctx, view, dependencies)
	if child != nil {
		categoryCloseOwner(t, child)
	}
	if err != nil || child == nil || probe.calls.Load() == 0 {
		t.Fatal("same-use facade no longer opens the exact shared preparation", err)
	}
	report := child.state.assembly.Snapshot()
	if len(report.Sources) != 1 || report.Sources[0].Limits != prepared.nativeLimits() {
		t.Fatal("derived source inherited its parent's capacity instead of its own envelope")
	}
	if err := child.Client().SignalWorkflow(parent.ctx, "workflow", "run", "signal", nil); err != nil {
		t.Fatal(err)
	}
	childFixture := testFixture{owner: child, dependencies: dependencies, ctx: parent.ctx}
	result := capabilityEvidence(t, childFixture, "workflow.signal")
	attribution := orchestration.Attribution(result.Source)
	if attribution != orchestration.Attribution(child.Client().Attribution()) || attribution.Namespace != "child" ||
		attribution.SourceID == "" || attribution.SourceID == parent.owner.Client().Attribution().SourceID || attribution.UseID == 0 {
		t.Fatal("category attribution lost the actual derived source/use")
	}
	if err := child.Close(parent.ctx); err != nil {
		t.Fatal(err)
	}
	configured := probe.calls.Load()
	if owner, err := prepared.OpenFrom(parent.ctx, peer, dependencies); owner != nil || !errors.Is(err, ErrAuthority) || probe.calls.Load() != configured {
		t.Fatal("separate retained use rebound prepared parent authority", err)
	}
	if err := view.Close(parent.ctx); err != nil {
		t.Fatal(err)
	}
	for _, open := range []func() (*Owner, error){
		func() (*Owner, error) { return prepared.Open(parent.ctx, dependencies) },
		func() (*Owner, error) { return prepared.OpenFrom(parent.ctx, view, dependencies) },
	} {
		if owner, err := open(); owner != nil || !errors.Is(err, ErrState) || probe.calls.Load() != configured {
			t.Fatal("closed same-use parent entered derived native construction", err)
		}
	}
	if err := peer.SignalWorkflow(parent.ctx, "workflow", "run", "signal", nil); err != nil {
		t.Fatal("category extraction revoked an independent parent-source use", err)
	}
	if result := capabilityEvidence(t, parent, "workflow.signal"); orchestration.Attribution(result.Source) != orchestration.Attribution(peer.Attribution()) {
		t.Fatal("surviving peer evidence lost exact use identity")
	}
}

func TestCategoryAttributionKeepsTemporalDiagnosticsAndTypedNilBehavior(t *testing.T) {
	canary := "category-attribution-private-canary"
	value := Attribution{SourceID: canary, UseID: 23, Name: canary, Namespace: canary, Generation: 7}
	shared := orchestration.Attribution(value)
	if Attribution(shared) != value || shared.SourceID != canary || shared.UseID != 23 || shared.Generation != 7 {
		t.Fatal("explicit category conversion lost attribution fields")
	}
	shared.SourceID = "detached-category-copy"
	if value.SourceID != canary || reflect.TypeOf(value).PkgPath() != "github.com/frost-leo/fathomry/adapters/orchestration/temporal/v1" {
		t.Fatal("provider attribution became an alias or shared mutable state")
	}
	if value.String() != "temporal[restricted]" || value.GoString() != "temporal[restricted]" || value.LogValue().String() != "temporal[restricted]" {
		t.Fatal("provider diagnostic vocabulary changed to category diagnostics")
	}
	for _, input := range []any{value, &value} {
		for _, format := range []string{"%s", "%v", "%+v", "%#v"} {
			if fmt.Sprintf(format, input) != "temporal[restricted]" {
				t.Fatal("provider attribution formatting changed or exposed identity")
			}
		}
		encoded, err := json.Marshal(input)
		if len(encoded) != 0 || !errors.Is(err, ErrSerialization) {
			t.Fatal("provider attribution serialization code changed", err)
		}
		marshalerError, wrapped := err.(*json.MarshalerError)
		if !wrapped {
			t.Fatal("JSON did not preserve the provider's marshaler refusal")
		}
		core, present := failure.Inspect(marshalerError.Err)
		if !present || core.Diagnostic().Definition.Code != ErrSerialization {
			t.Fatal("serialization lost the classified Temporal diagnostic")
		}
		var output bytes.Buffer
		slog.New(slog.NewJSONHandler(&output, nil)).Info("attribution", "source", input)
		if strings.Contains(output.String(), canary) || !strings.Contains(output.String(), `"source":"temporal[restricted]"`) {
			t.Fatal("provider attribution slog projection changed")
		}
	}
	original := value
	if err := json.Unmarshal([]byte(`{"SourceID":"replacement"}`), &value); !errors.Is(err, ErrSerialization) || value != original {
		t.Fatal("reconstruction changed provider code or attribution data", err)
	}
	var typedNil *Attribution
	encoded, err := json.Marshal(typedNil)
	if err != nil || string(encoded) != "null" || fmt.Sprintf("%v", typedNil) != "<nil>" {
		t.Fatal("standard typed-nil JSON/formatting behavior changed", err)
	}
	if err := json.Unmarshal([]byte(`{}`), &typedNil); !errors.Is(err, ErrSerialization) {
		t.Fatal("allocated typed-nil reconstruction bypassed provider refusal", err)
	}
}
