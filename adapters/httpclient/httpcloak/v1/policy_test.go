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

package httpcloak_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	p "github.com/frost-leo/fathomry/adapters/httpclient/httpcloak/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestPublicPolicyRejectsInsufficientCallerMechanisms(t *testing.T) {
	prepared, err := p.Prepare(testSettings("policy", p.HTTP1), p.NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"active", "source-work", "evidence-count", "evidence-bytes"} {
		t.Run(field, func(t *testing.T) {
			limits, evidence := policy.Runtime, policy.Evidence
			switch field {
			case "active":
				limits.MaxActive--
			case "source-work":
				limits.MaxWorkBytes--
			case "evidence-count":
				evidence.Capacity--
			case "evidence-bytes":
				evidence.MaxBytes--
			}
			runtime, err := adapters.New(context.Background(), limits)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close(context.Background())
			inbox, err := adapters.NewInbox[p.Result](evidence)
			if err != nil {
				t.Fatal(err)
			}
			before, _ := runtime.Options()
			owner, err := prepared.Open(testContext(t), p.Dependencies{Runtime: runtime, Evidence: inbox})
			if owner != nil || !errors.Is(err, p.ErrLimit) {
				t.Fatal("insufficient policy acquired source", err)
			}
			after, _ := runtime.Options()
			state, _ := runtime.Inspect()
			records, _ := inbox.Inspect()
			if !reflect.DeepEqual(before, after) || state.Accepted != 0 || records.Outstanding != 0 {
				t.Fatal("policy refusal changed borrowed mechanisms")
			}
		})
	}
	second := testSettings("second", p.HTTP2)
	second.MaxResponseBytes = pointer(int64(16 << 20))
	larger, err := p.Prepare(second, p.NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	largePolicy, err := larger.Policy()
	if err != nil {
		t.Fatal(err)
	}
	combined, err := p.Compose(prepared, larger)
	if err != nil {
		t.Fatal(err)
	}
	if combined.SourceWorkBytes != policy.SourceWorkBytes+largePolicy.SourceWorkBytes ||
		combined.SourceEvidenceBytes != policy.SourceEvidenceBytes+largePolicy.SourceEvidenceBytes ||
		combined.Budget.WorkBytes != max(policy.Budget.WorkBytes, largePolicy.Budget.WorkBytes) ||
		combined.Runtime.MaxActive != policy.Runtime.MaxActive+largePolicy.Runtime.MaxActive {
		t.Fatal("overlapping generation envelope lost actual selection")
	}
}

func TestPublicRetainedExchangePolicyPreservesGenerationBudgets(t *testing.T) {
	for _, protocol := range []p.ProtocolMode{p.HTTP1, p.HTTP2, p.HTTP3} {
		t.Run(string(protocol), func(t *testing.T) {
			settings := testSettings("retained-public", protocol)
			settings.MaxActive, settings.MaxBindings, settings.MaxExchanges = pointer(1), pointer(1), pointer(1)
			first, err := p.Prepare(settings, p.NativeOptions{})
			if err != nil {
				t.Fatal(err)
			}
			one, err := first.Policy()
			if err != nil {
				t.Fatal(err)
			}
			settings.MaxExchanges = pointer(3)
			second, err := p.Prepare(settings, p.NativeOptions{})
			if err != nil {
				t.Fatal(err)
			}
			more, err := second.Policy()
			if err != nil {
				t.Fatal(err)
			}
			settings.MaxBindings = pointer(3)
			cached, err := p.Recommend(settings, p.NativeOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if more.Budget.WorkBytes-one.Budget.WorkBytes < cached.SourceWorkBytes-more.SourceWorkBytes || more.Budget.EvidenceBytes != one.Budget.EvidenceBytes || cached.Budget != more.Budget {
				t.Fatal("public policy lost retained exchange graphs or conflated source/root/evidence")
			}
			combined, err := p.Compose(first, second)
			if err != nil || combined.SourceWorkBytes != one.SourceWorkBytes+more.SourceWorkBytes || combined.Budget.WorkBytes != more.Budget.WorkBytes {
				t.Fatal("overlapping generations lost the larger retained root envelope", err)
			}
			dependencies := mechanisms(t, one)
			before, _ := dependencies.Runtime.Options()
			owner, err := second.Open(testContext(t), dependencies)
			if owner != nil || !errors.Is(err, p.ErrLimit) {
				t.Fatal("old root budget silently accepted a larger exchange generation", err)
			}
			after, _ := dependencies.Runtime.Options()
			state, _ := dependencies.Runtime.Inspect()
			records, _ := dependencies.Evidence.Inspect()
			if !reflect.DeepEqual(before, after) || state.Accepted != 0 || records.Outstanding != 0 {
				t.Fatal("retained-budget refusal expanded or admitted caller-owned mechanisms")
			}
		})
	}
}

func TestPublicEvidenceSaturationDoesNotBlockSourceCleanup(t *testing.T) {
	var effects atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { effects.Add(1); _, _ = io.WriteString(w, "ok") }))
	defer peer.Close()
	value := testSettings("saturated", p.HTTP1)
	value.MaxActive = pointer(1)
	owner, dependencies := opened(t, value, p.NativeOptions{})
	receipt, err := owner.Client().Do(testContext(t), testContext(t), input(t, context.Background(), peer.URL))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(testContext(t))
	if err != nil || snapshot.Err() != nil {
		t.Fatal(err, snapshot.Err())
	}
	before, _ := dependencies.Runtime.Inspect()
	blocked, err := owner.Client().Do(testContext(t), testContext(t), input(t, context.Background(), peer.URL))
	if blocked != nil || !errors.Is(err, adapters.ErrEvidence) {
		t.Fatal("full evidence ledger admitted another operation", err)
	}
	after, _ := dependencies.Runtime.Inspect()
	if before.Accepted != after.Accepted || effects.Load() != 1 {
		t.Fatal("evidence saturation dispatched HTTP")
	}
	if err := owner.Close(testContext(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("cleanup needed new evidence capacity", err)
	}
	drain(t, dependencies.Evidence)
	state, _ := dependencies.Runtime.Inspect()
	if state.Active != 0 || state.WorkBytes != 0 {
		t.Fatal("source or operation reservation leaked")
	}
}

func TestPublicSettingsBoundsBeforeSerialization(t *testing.T) {
	for _, modify := range []func(*p.Settings){
		func(value *p.Settings) { value.Name = strings.Repeat("n", 65) },
		func(value *p.Settings) { value.PresetName = strings.Repeat("p", 257) },
		func(value *p.Settings) { value.PresetJSON = strings.Repeat(" ", (1<<20)+1) },
		func(value *p.Settings) { value.ProxyURL = strings.Repeat("p", 8193) },
		func(value *p.Settings) { value.ResolverAddress = strings.Repeat("r", 65) },
		func(value *p.Settings) { value.ResolverNetwork = pointer(strings.Repeat("r", 4)) },
	} {
		value := testSettings("bounded", p.HTTP1)
		modify(&value)
		if _, err := p.Prepare(value, p.NativeOptions{}); err == nil {
			t.Fatal("oversized Settings reached native preparation")
		}
		if err := p.Validate(value); err == nil {
			t.Fatal("oversized Settings validated")
		}
	}
	value := testSettings("network", p.HTTP1)
	value.ResolverNetwork = pointer("")
	if _, err := p.Prepare(value, p.NativeOptions{}); err == nil {
		t.Fatal("explicit empty resolver network silently defaulted")
	}
	value.ResolverNetwork = nil
	if _, err := p.Prepare(value, p.NativeOptions{}); err != nil {
		t.Fatal("omitted resolver network lost native default", err)
	}
}
