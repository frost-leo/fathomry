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

package trino

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

type fixture struct {
	client   *Client
	assembly *resource.Assembly
	selected resource.Selection[Source]
	inbox    *invocation.Inbox[Result]
	options  OptionsV1
}

func deadline(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func correlation(name string) fault.Correlation { return fault.Correlation{Call: name} }
func reply(t testing.TB, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Errorf("fixture response failed")
	}
}
func column(name, kind string, args ...any) map[string]any {
	if args == nil {
		args = []any{}
	}
	return map[string]any{"name": name, "type": kind, "typeSignature": map[string]any{"rawType": kind, "arguments": args}}
}
func long(value int64) map[string]any { return map[string]any{"kind": "LONG", "value": value} }
func typed(kind string) map[string]any {
	return map[string]any{"kind": "TYPE", "value": map[string]any{"rawType": kind, "arguments": []any{}}}
}
func peer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, OptionsV1) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error("fixture request read failed")
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			if string(body) == "SELECT version()" {
				reply(t, w, map[string]any{"id": "readiness", "columns": []any{column("version", "varchar", long(2147483647))}, "data": [][]string{{"483"}}})
				return
			}
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	return server, OptionsV1{Name: "trino", Endpoint: server.URL, User: "gh41_test", Plaintext: true,
		Catalog: "test_catalog", Schema: "gh41_run", Writes: true, Timeout: 2 * time.Second, CleanupTimeout: time.Second}
}
func bindFixture(t *testing.T, o OptionsV1, capacity int) fixture {
	t.Helper()
	selected, err := Select(o)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(o))
	assembly, err := resource.Assemble(deadline(t), deadline(t), "gh41", selected)
	if assembly != nil {
		t.Cleanup(func() {
			if err := assembly.Close(deadline(t)); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](capacity, int64(capacity)*defaults(o).evidenceBytes())
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for inbox.Usage().Outstanding > 0 {
			delivery, err := inbox.Next(deadline(t))
			if err != nil {
				t.Error(err)
				break
			}
			if err := delivery.Release(); err != nil {
				t.Error(err)
				break
			}
		}
	})
	return fixture{client: client, assembly: assembly, selected: selected, inbox: inbox, options: o}
}
func settle(t testing.TB, receipt *invocation.Receipt[Result], err error) invocation.Result[Result] {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	if receipt == nil {
		t.Fatal("missing receipt")
	}
	result, ok := receipt.Result()
	if !ok || !result.Final || !result.Released {
		t.Fatal("synchronous result remains locally owned")
	}
	return result
}
func success(t testing.TB, result invocation.Result[Result]) Result {
	t.Helper()
	if result.Err() != nil {
		t.Fatal(result.Err())
	}
	if !result.Outcome.Value.Complete() || !result.Outcome.Value.Succeeded() || !result.Outcome.Value.Terminal() {
		t.Fatal("missing terminal completeness evidence")
	}
	return result.Outcome.Value
}
func TestReadinessRequiresCoordinatorAndTerminalData(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reply(t, w, map[string]any{"id": "ping"})
	}))
	defer server.Close()
	o := OptionsV1{Name: "trino", Endpoint: server.URL, User: "test", Plaintext: true}
	selected, err := Select(o)
	if err != nil {
		t.Fatal(err)
	}
	assembly, err := resource.Assemble(deadline(t), deadline(t), "gh41", resource.WithLimits(selected, LimitsV1(o)))
	if err == nil || assembly == nil {
		t.Fatal("missing readiness failure/owner")
	}
	if closeErr := assembly.Close(deadline(t)); closeErr != nil {
		t.Fatal(closeErr)
	}
	for _, source := range assembly.Snapshot().Sources {
		if !source.Quiescent || !source.Released {
			t.Fatal("failed readiness retained local resources")
		}
	}
}
