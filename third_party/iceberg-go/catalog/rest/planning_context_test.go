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

package rest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/apache/iceberg-go/table"
)

func TestScanPlanningContextAdmissionAndIdentity(t *testing.T) {
	type marker struct{}
	for _, refuse := range []bool{false, true} {
		t.Run(map[bool]string{false: "admitted", true: "refused"}[refuse], func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/v1/config" {
					_, _ = w.Write([]byte(`{"endpoints":["POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan"]}`))
					return
				}
				requests.Add(1)
				if r.URL.Path != "/v1/namespaces/ns/tables/data/plan" {
					t.Error("hook mutated the request identifier")
				}
				_, _ = w.Write([]byte(`{"status":"completed","plan-id":"owned","file-scan-tasks":[]}`))
			}))
			defer server.Close()
			refusal := errors.New("source admission refused")
			calls := 0
			cat, err := NewCatalog(t.Context(), "planning", server.URL, WithScanPlanningContext(func(ctx context.Context, id table.Identifier) (context.Context, error) {
				calls++
				if ctx.Value(marker{}) != "caller" || !slices.Equal(id, []string{"ns", "data"}) {
					t.Error("caller context or identifier lost")
				}
				id[1] = "changed"
				if refuse {
					return nil, refusal
				}
				return context.WithValue(ctx, marker{}, "admitted"), nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			defer cat.Close()
			if calls != 0 {
				t.Fatal("planning hook ran during unrelated catalog acquisition")
			}
			id := table.Identifier{"ns", "data"}
			_, err = cat.PlanTableScan(context.WithValue(t.Context(), marker{}, "caller"), id, PlanTableScanRequest{})
			if refuse {
				if !errors.Is(err, refusal) || requests.Load() != 0 {
					t.Fatalf("refusal lost or occurred after I/O: %v", err)
				}
			} else if err != nil || requests.Load() != 1 {
				t.Fatalf("admitted planning did not execute: %v", err)
			}
			if calls != 1 || id[1] != "data" {
				t.Fatal("planning setup repeated or caller identifier aliased")
			}
		})
	}
}
