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
	"reflect"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func policySettings() Settings {
	return Settings{Name: "policy", Endpoint: "127.0.0.1:7233", Namespace: "test", Plaintext: true,
		MaxActive: 2, QueuedCalls: 3, MaxRequestBytes: 1024, MaxResponseBytes: 2048, FamilyLimit: 4, MaxUses: 5, InnerEvidenceCapacity: 6}
}

func TestPublicPolicyReservesSourceFamiliesAndIndependentEvidence(t *testing.T) {
	settings := policySettings()
	prepared, err := Prepare(settings, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	recommended, err := Recommend(settings)
	if err != nil || !reflect.DeepEqual(policy, recommended) {
		t.Fatal("data-only recommendation differs from exact preparation", err)
	}
	if policy.Runtime.MaxActive != 1+settings.MaxActive || policy.Runtime.MaxQueued != settings.QueuedCalls ||
		policy.Runtime.MaxTasks != settings.FamilyLimit || policy.Runtime.MaxWorkBytes != policy.SourceWorkBytes+int64(settings.MaxActive)*max(policy.Budget.WorkBytes, policy.Budget.WorkerBytes) ||
		policy.Runtime.MaxQueuedBytes != int64(settings.QueuedCalls)*max(policy.Budget.WorkBytes, policy.Budget.WorkerBytes) ||
		policy.NativeWorkerBytes != policy.NativeWorkBytes || policy.Budget.WorkBytes <= policy.NativeWorkBytes ||
		policy.SourceEvidenceBytes != policy.Budget.EvidenceBytes {
		t.Fatal("public policy lost source/family/evidence reservation")
	}
	if policy.Evidence.Capacity != 1+settings.InnerEvidenceCapacity || policy.Workers.Capacity != settings.InnerEvidenceCapacity || policy.Tasks.Capacity != settings.InnerEvidenceCapacity {
		t.Fatal("operation, Worker and task evidence capacity conflated")
	}
	for _, limits := range []adapters.EvidenceOptions{policy.Evidence, policy.Workers, policy.Tasks} {
		if limits.MaxBytes != int64(limits.Capacity)*policy.Budget.EvidenceBytes {
			t.Fatal("evidence slots lack full declared bytes")
		}
		inbox, err := adapters.NewInbox[Result](limits)
		if err != nil || inbox == nil {
			t.Fatal("returned policy produced invalid receiver", err)
		}
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal("returned runtime policy is invalid", err)
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	settings.WorkerWorkBytes = policy.NativeWorkBytes * 2
	larger, err := Recommend(settings)
	if err != nil {
		t.Fatal(err)
	}
	if larger.NativeWorkBytes != policy.NativeWorkBytes || larger.NativeWorkerBytes != settings.WorkerWorkBytes ||
		larger.Budget.WorkerBytes <= policy.Budget.WorkerBytes || larger.Runtime.MaxWorkBytes <= policy.Runtime.MaxWorkBytes {
		t.Fatal("larger Worker pipeline failed to increase only its declared reservation")
	}
	settings.QueuedCalls = 0
	unqueued, err := Recommend(settings)
	if err != nil || unqueued.Runtime.MaxQueuedBytes != 0 {
		t.Fatal("zero queued calls acquired an implicit queue", err)
	}
}

func TestPublicPolicyRejectsUnrepresentableRuntimeDimensions(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		mutate func(*Settings)
	}{
		{"active", func(value *Settings) { value.MaxActive = 1024 }},
		{"active-bytes", func(value *Settings) { value.MaxActive = 2; value.WorkerWorkBytes = 1 << 39 }},
		{"queued-bytes", func(value *Settings) { value.MaxActive = 1; value.QueuedCalls = 1024; value.WorkerWorkBytes = 1 << 38 }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			settings := policySettings()
			scenario.mutate(&settings)
			prepared, err := Prepare(settings, NativeOptions{})
			if err != nil {
				t.Fatal("boundary should be representable before public composition", err)
			}
			policy, err := prepared.Policy()
			if !errors.Is(err, ErrLimit) {
				t.Fatalf("invalid runtime dimension escaped Policy: queued=%d bytes=%d error=%v", policy.Runtime.MaxQueued, policy.Runtime.MaxQueuedBytes, err)
			}
		})
	}
	if _, err := (Prepared{}).Policy(); !errors.Is(err, ErrState) {
		t.Fatal("zero preparation produced a policy", err)
	}
}
