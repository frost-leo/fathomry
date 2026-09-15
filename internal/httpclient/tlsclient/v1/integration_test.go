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

package tlsclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestProviderConcurrentNativeIntegration(t *testing.T) {
	providerProtocols(t, func(t *testing.T, mode ProtocolMode) {
		var mu sync.Mutex
		seen := make(map[string]string)
		endpoint, options := providerPeer(t, mode, func(w http.ResponseWriter, r *http.Request) {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				return
			}
			id := r.Header.Get("X-Call")
			mu.Lock()
			seen[id] = string(data)
			mu.Unlock()
			w.Header().Set("X-Call", id)
			_, _ = w.Write(data)
		})
		options.MaxActive, options.QueuedCalls = 4, 16
		fixture := bindProvider(t, options, 16)
		observer, err := invocation.NewObserver(1)
		if err != nil {
			t.Fatal(err)
		}
		client, err := Bind(fixture.assembly, fixture.selected, fixture.inbox, observer)
		if err != nil {
			t.Fatal(err)
		}
		access, err := resource.AccessFor(fixture.assembly, fixture.selected)
		if err != nil {
			t.Fatal(err)
		}
		ctx := testContext(t)
		var group sync.WaitGroup
		for index := range 16 {
			id := fmt.Sprintf("native-%02d", index)
			group.Go(func() {
				request := providerRequest(t, "POST", endpoint, strings.NewReader("payload-"+id))
				request.Header.Set("X-Call", id)
				receipt, err := client.Do(ctx, ctx, providerID(id), request)
				if err != nil || receipt == nil {
					t.Error("native concurrent operation failed", err)
					return
				}
				result, err := receipt.WaitReleased(ctx)
				if err != nil || result.Err() != nil || string(result.Outcome.Value.DataCopy()) != "payload-"+id {
					t.Error("direct receipt differs", err)
				}
			})
		}
		group.Wait()
		expected := make([]conformance.Expected[Result], 0, 16)
		for index := range 16 {
			id := fmt.Sprintf("native-%02d", index)
			expected = append(expected, conformance.Expected[Result]{
				Context: fault.Context{Provider: ProviderID, Source: options.Name, Scope: "local", Operation: "request", Correlation: providerID(id)},
				Source:  access.Info(), Limits: access.Limits(), Shape: invocation.Finite, Present: true, Final: true, Released: true,
				Attempts: invocation.Attempts{Observed: 1},
				Value: func(t testing.TB, result Result) {
					mu.Lock()
					observed, ok := seen[id]
					mu.Unlock()
					if !ok || observed != "payload-"+id || !result.Complete() || string(result.DataCopy()) != observed || result.Metadata().HeadersCopy().Get("X-Call") != id {
						t.Error("peer/independent receipt isolation differs")
					}
				},
			})
		}
		conformance.Receive(t, ctx, fixture.inbox, expected)
		status := fixture.assembly.Snapshot().Sources[0]
		conformance.Accounting(t, status.Usage, status.Limits, fixture.inbox.Usage(), 16, 16*client.EvidenceBytes())
		if status.Usage.Active != 0 || status.Usage.Queued != 0 || fixture.inbox.Usage().Outstanding != 0 {
			t.Fatal("usage did not return to zero")
		}
		build, err := Build()
		if err != nil {
			t.Fatal(err)
		}
		report, err := compatibility.Assess(build, access, client.Profile(), []compatibility.Requirement{{Guarantee: "native-http", Layers: []compatibility.Layer{compatibility.SDK, compatibility.Capability}}}, nil)
		if err != nil || report.Require(compatibility.Policy{}) == nil {
			t.Fatal("unknown compatibility certified", err)
		}
		conformance.Facade(t, client, "Format", "LogValue", "MarshalJSON", "UnmarshalJSON", "Open", "Do", "EvidenceBytes", "Profile")
	})
}
func TestProviderPreflightRacingInputDoesNotAcquireReader(t *testing.T) {
	options := providerOptions()
	options.Mode = HTTP3Racing
	fixture := bindProvider(t, options, 1)
	input := &countedInput{}
	receipt, err := fixture.client.Do(context.Background(), testContext(t), providerID("unsupported"), providerRequest(t, "POST", "https://unused.invalid", input))
	if receipt != nil || !errors.Is(err, ErrUnsupported) || input.reads.Load() != 0 || input.closes.Load() != 0 {
		t.Fatal("non-replayable racing input acquired", err)
	}
}
