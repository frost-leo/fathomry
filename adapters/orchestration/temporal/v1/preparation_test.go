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
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
	sdk "go.temporal.io/sdk/client"
)

type preparationProbe struct {
	sdk.PluginBase
	calls atomic.Int32
}

func (*preparationProbe) Name() string { return "offline-preparation-probe" }
func (probe *preparationProbe) ConfigureClient(context.Context, sdk.PluginConfigureClientOptions) error {
	probe.calls.Add(1)
	return nil
}

func TestPublicPreparationResolvesDefaultsWithoutCallingRuntimeExtensions(t *testing.T) {
	probe := &preparationProbe{}
	var dials atomic.Int32
	settings := Settings{Name: "offline", Endpoint: "127.0.0.1:1", Namespace: "test", Plaintext: true}
	prepared, err := Prepare(settings, NativeOptions{Plugins: []sdk.Plugin{probe}, ContextDialer: func(context.Context, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("offline dial must not run")
	}})
	if err != nil {
		t.Fatal(err)
	}
	effective := prepared.Settings()
	if probe.calls.Load() != 0 || dials.Load() != 0 || effective.Version != 1 || effective.MaxUses != 64 || effective.InnerEvidenceCapacity != 64 || effective.FamilyLimit != 64 ||
		effective.MaxActive <= 0 || effective.QueuedCalls != 0 || effective.WorkerWorkBytes <= 0 || effective.AdmissionTimeout <= 0 {
		t.Fatal("offline preparation changed defaults or invoked borrowed code")
	}
	repeated, err := Prepare(effective, NativeOptions{})
	if err != nil {
		t.Fatal("effective settings cannot be reused", err)
	}
	if repeated.Settings().WorkerWorkBytes != effective.WorkerWorkBytes {
		t.Fatal("re-preparation recomputed a different declared envelope")
	}
	known := KnownRPCs()
	if len(known) != 135 {
		t.Fatal("selected RPC inventory changed without review")
	}
	first := known[0]
	known[0] = "caller-modified"
	if KnownRPCs()[0] != first {
		t.Fatal("RPC inventory exposed shared storage")
	}
}

func TestPublicPreparationRejectsMalformedCapacityBeforeOpening(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		alter    func(*Settings)
		expected error
	}{
		{"uses-negative", func(value *Settings) { value.MaxUses = -1 }, ErrInput},
		{"uses-too-large", func(value *Settings) { value.MaxUses = 1025 }, ErrInput},
		{"inner-negative", func(value *Settings) { value.InnerEvidenceCapacity = -1 }, ErrInput},
		{"inner-too-large", func(value *Settings) { value.InnerEvidenceCapacity = 65536 }, ErrInput},
		{"family-too-small", func(value *Settings) { value.FamilyLimit = 2 }, ErrInput},
		{"family-too-large", func(value *Settings) { value.FamilyLimit = 1025 }, ErrInput},
		{"worker-negative", func(value *Settings) { value.WorkerWorkBytes = -1 }, ErrInput},
		{"worker-too-large", func(value *Settings) { value.WorkerWorkBytes = 1<<40 + 1 }, ErrInput},
		{"worker-underdeclared", func(value *Settings) { value.WorkerWorkBytes = 1 }, ErrLimit},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			settings := policySettings()
			scenario.alter(&settings)
			if _, err := Prepare(settings, NativeOptions{}); !errors.Is(err, scenario.expected) {
				t.Fatal("malformed capacity did not fail at preparation", err)
			}
		})
	}
}

func TestPublicPreparationPinsSharedOriginAndRefusesClosedParentBeforeNativeEntry(t *testing.T) {
	probe := &preparationProbe{}
	parent := newCapabilityFixture(t, &testServer{}, NativeOptions{})
	derived, err := PrepareFromExisting(Settings{Name: "derived", Namespace: "child", MaxActive: 1, FamilyLimit: 3, InnerEvidenceCapacity: 8}, NativeOptions{Plugins: []sdk.Plugin{probe}}, parent.owner.Client())
	if err != nil {
		t.Fatal(err)
	}
	if derived.Settings().Namespace != "child" || derived.Settings().Endpoint == "" || !derived.Settings().Plaintext || probe.calls.Load() != 0 {
		t.Fatal("shared offline selection lost inherited transport or invoked plugin")
	}
	peer, err := parent.owner.Client().Borrow(parent.ctx)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := derived.Policy()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(parent.ctx, policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
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
	dependencies := Dependencies{Runtime: runtime, Evidence: operations, Workers: workers, Tasks: tasks}
	copied, err := parent.owner.Client().WithID("shared-parent-copy")
	if err != nil {
		t.Fatal(err)
	}
	child, err := derived.OpenFrom(parent.ctx, copied, dependencies)
	if child != nil {
		t.Cleanup(func() { _ = child.Close(context.Background()) })
	}
	if err != nil || child == nil || child.Client().Namespace() != "child" {
		t.Fatal("same-use facade copy changed the prepared shared origin", err)
	}
	if err := child.Close(parent.ctx); err != nil {
		t.Fatal(err)
	}
	configured := probe.calls.Load()
	if owner, err := derived.OpenFrom(parent.ctx, peer, dependencies); owner != nil || !errors.Is(err, ErrAuthority) {
		t.Fatal("prepared shared source rebound to a peer identity", err)
	}
	if err := parent.owner.Client().Close(parent.ctx); err != nil {
		t.Fatal(err)
	}
	if owner, err := derived.Open(parent.ctx, dependencies); owner != nil || !errors.Is(err, ErrState) || probe.calls.Load() != configured {
		t.Fatal("closed prepared origin entered native construction", err)
	}
	if err := peer.SignalWorkflow(parent.ctx, "peer", "run", "signal", nil); err != nil {
		t.Fatal("closing preparation origin revoked independent peer", err)
	}
	capabilityEvidence(t, parent, "workflow.signal")
}

func TestPublicPreparationValidateAndConvenienceOpenUseExplicitNativeDependencies(t *testing.T) {
	if err := Validate(Settings{}); !errors.Is(err, ErrInput) {
		t.Fatal("Validate accepted a missing endpoint/namespace", err)
	}
	settings := Settings{Name: "convenience", Endpoint: "127.0.0.1:1", Namespace: "test", Plaintext: true, Lazy: true,
		MaxActive: 1, MaxRequestBytes: 1024, MaxResponseBytes: 1024, FamilyLimit: 3, InnerEvidenceCapacity: 4}
	if err := Validate(settings); err != nil {
		t.Fatal(err)
	}
	fixture := newTestFixture(t, NativeOptions{}, nil)
	probe := &preparationProbe{}
	dependencies := fixture.dependencies
	dependencies.Native = NativeOptions{Plugins: []sdk.Plugin{probe}}
	owner, err := Open(fixture.ctx, settings, dependencies)
	if owner != nil {
		t.Cleanup(func() {
			if err := owner.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil || owner == nil || owner.Client().Namespace() != "test" || probe.calls.Load() != 1 {
		t.Fatal("convenience Open ignored explicit native dependencies", err)
	}
	if err := owner.Close(fixture.ctx); err != nil {
		t.Fatal(err)
	}
}
