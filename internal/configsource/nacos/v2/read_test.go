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

package nacos

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	response "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_response"
)

func TestRawReadsRemainIndependentAndFailClosed(t *testing.T) {
	fixture := newFixture(t, false)
	client := openClient(t, fixture.options())
	first, err := client.Read(context.Background(), KeyV1{DataID: "settings.yaml"})
	if err != nil || first.MD5() == "" || first.LastModifiedMillis() != 1 {
		t.Fatal("native read evidence missing", err)
	}
	before := string(first.RawCopy())
	copy := first.RawCopy()
	copy[0] = '!'
	fixture.mu.Lock()
	fixture.values[key{"DEFAULT_GROUP", "settings.yaml"}] = "host: updated"
	fixture.mu.Unlock()
	second, err := client.Read(context.Background(), KeyV1{DataID: "settings.yaml"})
	if err != nil || string(second.RawCopy()) == before || string(first.RawCopy()) != before {
		t.Fatal("raw result aliases or reuses earlier content", err)
	}
	conformance.Runtime(t, first, new(Document), "18446744073709551615")
	fixture.mu.Lock()
	delete(fixture.values, key{"DEFAULT_GROUP", "settings.yaml"})
	fixture.mu.Unlock()
	if value, err := client.Read(context.Background(), KeyV1{DataID: "settings.yaml"}); value != nil || !errors.Is(err, ErrMissing) {
		t.Fatal("missing configuration became stale success", err)
	}
	fixture.mu.Lock()
	fixture.values[key{"DEFAULT_GROUP", "settings.yaml"}] = ""
	fixture.mu.Unlock()
	if value, err := client.Read(context.Background(), KeyV1{DataID: "settings.yaml"}); value != nil || !errors.Is(err, ErrEmpty) {
		t.Fatal("empty configuration conflated with missing", err)
	}
}
func TestMultipleRequiredKeysAndClients(t *testing.T) {
	fixture := newFixture(t, false)
	fixture.mu.Lock()
	fixture.values[key{"custom", "extra.json"}] = `{"Case":null}`
	fixture.mu.Unlock()
	input := fixture.options()
	input.Keys = append(input.Keys, KeyV1{Group: "custom", DataID: "extra.json"})
	first, second := openClient(t, input), openClient(t, input)
	values, err := first.ReadAll(context.Background())
	if err != nil || len(values) != 2 || values[1].Key().Group != "custom" {
		t.Fatal("batch order or group identity lost", err)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := second.ReadAll(context.Background()); err != nil {
		t.Fatal("closing independent client crossed ownership", err)
	}
	fixture.mu.Lock()
	delete(fixture.values, key{"custom", "extra.json"})
	fixture.mu.Unlock()
	if values, err := second.ReadAll(context.Background()); values != nil || !errors.Is(err, ErrMissing) {
		t.Fatal("required failure exposed successful prefix", err)
	}
}
func TestParallelReadsAndResponseLimits(t *testing.T) {
	fixture := newFixture(t, false)
	input := fixture.options()
	input.QueuedRequests = 16
	client := openClient(t, input)
	var workers sync.WaitGroup
	for range 12 {
		workers.Go(func() {
			document, err := client.Read(context.Background(), KeyV1{DataID: "settings.yaml"})
			if err != nil || document == nil {
				t.Error("parallel read failed", err)
			}
		})
	}
	workers.Wait()
	for _, content := range []string{strings.Repeat("x", MaxDocumentBytes+1), strings.Repeat(" ", 1)} {
		fixture.mu.Lock()
		fixture.values[key{"DEFAULT_GROUP", "settings.yaml"}] = content
		fixture.mu.Unlock()
		document, err := client.Read(context.Background(), KeyV1{DataID: "settings.yaml"})
		if document != nil || err == nil {
			t.Fatal("unusable required content accepted")
		}
	}
	fixture.mu.Lock()
	fixture.override = encoded(&response.ConfigQueryResponse{Response: &response.Response{ResultCode: 500, ErrorCode: 403, Message: "response-private-canary"}})
	fixture.mu.Unlock()
	if _, err := client.Read(context.Background(), KeyV1{DataID: "settings.yaml"}); !errors.Is(err, ErrDenied) {
		t.Fatal("denial identity lost", err)
	} else {
		conformance.Private(t, err, "response-private-canary")
	}
	info := client.Info()
	mutated := client.Info()
	mutated.Configuration.Provenance[0].Fields[0] = "changed"
	if reflect.DeepEqual(info, mutated) || !reflect.DeepEqual(info, client.Info()) {
		t.Fatal("Info provenance aliases caller")
	}
}

func TestWireMessageLimitAndAuthorizedFailover(t *testing.T) {
	failed := newFixture(t, false)
	working := newFixture(t, false)
	input := failed.options()
	input.Servers = append(input.Servers, working.options().Servers[0])
	failed.server.Stop()
	client := openClient(t, input)
	if value, err := client.Read(context.Background(), KeyV1{DataID: "settings.yaml"}); err != nil || value == nil || working.queries.Load() != 1 {
		t.Fatal("configured endpoint failover failed", err)
	}
	working.mu.Lock()
	working.override = encoded(&response.ConfigQueryResponse{Response: &response.Response{ResultCode: 200, Success: true}, Content: strings.Repeat("x", MaxWireBytes)})
	working.mu.Unlock()
	if value, err := client.Read(context.Background(), KeyV1{DataID: "settings.yaml"}); value != nil || !errors.Is(err, ErrLimit) {
		t.Fatal("oversize gRPC message did not preserve its capacity failure", err)
	}
}
