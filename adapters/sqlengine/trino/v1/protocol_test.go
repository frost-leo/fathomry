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

package trino_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	trino "github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1"
)

func namedArgument(name, raw string, arguments ...any) map[string]any {
	if arguments == nil {
		arguments = []any{}
	}
	return map[string]any{"kind": "NAMED_TYPE", "value": map[string]any{"fieldName": map[string]any{"name": name}, "typeSignature": map[string]any{"rawType": raw, "arguments": arguments}}}
}

func TestPublicContradictoryAndCanonicalMetadata(t *testing.T) {
	for _, test := range []struct {
		name   string
		column any
		valid  bool
	}{
		{"decimal-precision", wireColumn("value", "decimal(38,9)", "decimal", longArgument(5), longArgument(2)), false},
		{"decimal-scale", wireColumn("value", "decimal(38,9)", "decimal", longArgument(38), longArgument(2)), false},
		{"timestamp-precision", wireColumn("value", "timestamp(3)", "timestamp", longArgument(9)), false},
		{"timezone", wireColumn("value", "timestamp(3) with time zone", "timestamp", longArgument(3)), false},
		{"nested-array", wireColumn("value", "array(bigint)", "array", typeArgument("varchar", longArgument(10))), false},
		{"map-member", wireColumn("value", "map(varchar(10),bigint)", "map", typeArgument("varchar", longArgument(10)), typeArgument("boolean")), false},
		{"row-field-type", wireColumn("value", "row(x bigint)", "row", namedArgument("x", "varchar", longArgument(10))), false},
		{"row-field-name", wireColumn("value", `row("X" bigint)`, "row", namedArgument("x", "bigint")), false},
		{"decimal-whitespace", wireColumn("value", " DECIMAL ( 38 , 9 ) ", "decimal", longArgument(38), longArgument(9)), true},
		{"integer-alias", wireColumn("value", "int", "integer"), true},
		{"double-alias", wireColumn("value", "double precision", "double"), true},
		{"decimal-default", wireColumn("value", "decimal", "decimal", longArgument(38), longArgument(0)), true},
		{"timestamp-default", wireColumn("value", "timestamp", "timestamp", longArgument(3)), true},
		{"varchar-unbounded", wireColumn("value", "varchar", "varchar", longArgument(2147483647)), true},
		{"quoted-row", wireColumn("value", `row("A""B" bigint, unquoted varchar)`, "row", namedArgument("A\"B", "bigint"), namedArgument("unquoted", "varchar", longArgument(2147483647))), true},
		{"nested-map", wireColumn("value", "array(map(varchar(10),bigint))", "array", typeArgument("map", typeArgument("varchar", longArgument(10)), typeArgument("bigint"))), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			peer := newWirePeer(t, func(writer http.ResponseWriter, _ *http.Request, _ []byte) {
				writePage(t, writer, map[string]any{"id": "metadata", "columns": []any{test.column}, "data": [][]any{{nil}}})
			})
			owner, inbox, _ := openPublic(t, peer.settings(), 0)
			value, err := owner.Client().Query(boundedContext(t), boundedContext(t), trino.Statement{SQL: "SELECT exact_type"})
			if test.valid {
				if err != nil || !value.Complete() || string(value.DataCopy()) != "[[null]]" || value.ColumnsCopy()[0].Type != test.column.(map[string]any)["type"] {
					t.Fatal("legitimate semantic metadata rejected or normalized destructively", err)
				}
			} else if err == nil || value.Complete() || !errors.Is(err, trino.ErrProtocol) {
				t.Fatal("contradictory metadata certified valid output", err)
			}
			acknowledge(t, inbox)
		})
	}
}

func TestPublicProtocolBoundsAndRouting(t *testing.T) {
	for _, mode := range []string{"page", "rows", "result", "pages", "wire", "changed-columns", "foreign", "repeated", "redirect", "spooling", "session", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			var peer *wirePeer
			peer = newWirePeer(t, func(writer http.ResponseWriter, request *http.Request, _ []byte) {
				if request.Method == http.MethodDelete {
					writer.WriteHeader(http.StatusNoContent)
					return
				}
				page := map[string]any{"id": "bounds"}
				switch mode {
				case "page":
					writer.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(writer, strings.Repeat(" ", 513))
					return
				case "rows", "result":
					page["columns"] = []any{wireColumn("value", "varchar(1000)", "varchar", longArgument(1000))}
					page["data"] = [][]string{{strings.Repeat("x", 100)}, {strings.Repeat("x", 100)}}
				case "pages", "wire":
					page["nextUri"] = peer.next("bounds", int(peer.gets.Load())+1)
					page["stats"] = map[string]any{"state": strings.Repeat("x", 100)}
				case "changed-columns":
					name := "first"
					if request.Method == http.MethodPost {
						page["nextUri"] = peer.next("bounds", 1)
					} else {
						name = "changed"
					}
					page["columns"] = []any{wireColumn(name, "bigint", "bigint")}
					page["data"] = [][]int{{1}}
				case "foreign":
					page["nextUri"] = "http://127.0.0.1:1/v1/statement/executing/bounds/slug/1"
				case "repeated":
					page["nextUri"] = peer.next("bounds", 1)
				case "redirect":
					writer.Header().Set("Location", "http://127.0.0.1:1/")
					writer.WriteHeader(http.StatusTemporaryRedirect)
					return
				case "spooling":
					page["data"] = map[string]any{"encoding": "json", "segments": []any{}}
				case "session":
					writer.Header().Set("X-Trino-Set-Session", "private_canary=value")
				case "malformed":
					writer.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(writer, `{"id":"bounds","data":[`)
					return
				}
				writePage(t, writer, page)
			})
			options := peer.settings()
			switch mode {
			case "page":
				options.MaxPageBytes = 512
			case "rows":
				options.MaxRows = 1
			case "result":
				options.MaxResultBytes = 300
			case "pages":
				options.MaxPages = 2
			case "wire":
				options.MaxPageBytes, options.MaxWireBytes = 512, 512
			}
			owner, inbox, _ := openPublic(t, options, 0)
			value, err := owner.Client().Query(boundedContext(t), boundedContext(t), trino.Statement{SQL: "SELECT data"})
			if err == nil || value.Complete() || value.Submissions() != 1 || peer.posts.Load() != 1 {
				t.Fatal("rejected protocol path succeeded or replayed")
			}
			if mode == "foreign" || mode == "redirect" {
				if peer.gets.Load() != 0 {
					t.Fatal("foreign continuation or redirect dispatched")
				}
			}
			if mode == "pages" && (value.Pages() != 2 || peer.gets.Load() != 1 || peer.deletes.Load() != 1) {
				t.Fatal("page limit or required cleanup not enforced")
			}
			if mode == "changed-columns" && (value.Rows() != 1 || string(value.DataCopy()) != "[[1]]") {
				t.Fatal("later bad metadata erased a valid initial prefix")
			}
			acknowledge(t, inbox)
		})
	}
}

func TestPublicMutationFaultEvidence(t *testing.T) {
	for _, mode := range []string{"lost-post", "http-unavailable", "late-error", "terminal-zero", "cancel-acknowledged", "cancel-failed"} {
		t.Run(mode, func(t *testing.T) {
			started := make(chan struct{})
			var peer *wirePeer
			peer = newWirePeer(t, func(writer http.ResponseWriter, request *http.Request, _ []byte) {
				if request.Method == http.MethodDelete {
					if mode == "cancel-failed" {
						writer.WriteHeader(http.StatusServiceUnavailable)
					} else {
						writer.WriteHeader(http.StatusNoContent)
					}
					return
				}
				if mode == "lost-post" {
					connection, _, err := writer.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = connection.Close()
					return
				}
				if mode == "http-unavailable" {
					writer.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				if request.Method == http.MethodPost {
					writePage(t, writer, map[string]any{"id": "effect", "nextUri": peer.next("effect", 1), "updateCount": 19})
					return
				}
				if strings.HasPrefix(mode, "cancel-") {
					close(started)
					<-request.Context().Done()
					return
				}
				page := map[string]any{"id": "effect", "updateCount": 0}
				if mode == "late-error" {
					page["error"] = map[string]any{"errorCode": 123, "errorName": "TEST_ERROR", "errorType": "EXTERNAL", "message": "native_private_canary"}
				}
				writePage(t, writer, page)
			})
			owner, inbox, _ := openPublic(t, peer.settings(), 0)
			ctx, cancel := context.WithCancel(boundedContext(t))
			defer cancel()
			if strings.HasPrefix(mode, "cancel-") {
				go func() {
					select {
					case <-started:
						cancel()
					case <-ctx.Done():
					}
				}()
			}
			value, err := owner.Client().Execute(ctx, boundedContext(t), trino.Statement{SQL: "UPDATE fixture SET x=1"})
			if mode == "terminal-zero" {
				count, known := value.UpdateCount()
				if err != nil || !value.Complete() || value.Effect() != trino.Acknowledged || !known || count != 0 {
					t.Fatal("terminal acknowledgement or aggregate zero lost", err)
				}
			} else {
				if err == nil || value.Complete() || value.Effect() != trino.Unknown {
					t.Fatal("uncertain mutation was promoted to success/absence")
				}
				if _, known := value.UpdateCount(); known {
					t.Fatal("intermediate count survived failed terminal evidence")
				}
				if strings.Contains(fmt.Sprintf("%+v", err), "canary") {
					t.Fatal("safe mutation failure exposed server message")
				}
			}
			if peer.posts.Load() != 1 || value.Submissions() != 1 {
				t.Fatal("uncertain mutation was replayed")
			}
			snapshot := acknowledge(t, inbox)
			if strings.HasPrefix(mode, "cancel-") {
				if !errors.Is(err, context.Canceled) || !value.CancellationAttempted() || value.CancellationAcknowledged() != (mode == "cancel-acknowledged") || peer.deletes.Load() != 1 {
					t.Fatal("work cancellation/DELETE evidence conflated", err)
				}
				if (snapshot.Cleanup() != nil) != (mode == "cancel-failed") {
					t.Fatal("cleanup failure lost independent phase custody")
				}
			}
		})
	}
}
