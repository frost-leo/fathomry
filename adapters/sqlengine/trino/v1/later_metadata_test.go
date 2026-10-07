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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	trino "github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestRepeatedMetadataValidatedBeforePublicCapture(t *testing.T) {
	decimal := `{"rawType":"decimal","arguments":[{"kind":"LONG","value":5},{"kind":"LONG","value":2}]}`
	for _, route := range []string{"query", "execute", "insert", "read"} {
		for _, check := range []struct {
			name, text, first, later string
			valid                    bool
		}{
			{"fractional-long", "decimal(5,2)", decimal, `{"rawType":"decimal","arguments":[{"kind":"LONG","value":5.0},{"kind":"LONG","value":2}]}`, false},
			{"exponential-long", "decimal(5,2)", decimal, `{"rawType":"decimal","arguments":[{"kind":"LONG","value":5e0},{"kind":"LONG","value":2}]}`, false},
			{"nested-long", "array(decimal(5,2))", `{"rawType":"array","arguments":[{"kind":"TYPE","value":` + decimal + `}]}`, `{"rawType":"array","arguments":[{"kind":"TYPE","value":{"rawType":"decimal","arguments":[{"kind":"LONG","value":5.0},{"kind":"LONG","value":2}]}}]}`, false},
			{"nested-type", "map(varchar(8),bigint)", `{"rawType":"map","arguments":[{"kind":"TYPE","value":{"rawType":"varchar","arguments":[{"kind":"LONG","value":8}]}},{"kind":"TYPE","value":{"rawType":"bigint","arguments":[]}}]}`, `{"rawType":"map","arguments":[{"kind":"TYPE","value":{"rawType":"varchar","arguments":[{"kind":"LONG","value":8}]}},{"kind":"TYPE","value":{"rawType":"integer","arguments":[]}}]}`, false},
			{"reordered", " DECIMAL ( 5 , 2 ) ", decimal, ` { "arguments" : [ { "value" : 5, "kind" : "LONG" }, { "value" : 2, "kind" : "LONG" } ], "rawType" : "decimal" } `, true},
			{"integer-alias", " Int ", `{"rawType":"integer","arguments":[]}`, `{"arguments":[],"rawType":"integer"}`, true},
			{"temporal-precision", "timestamp(12) with time zone", `{"rawType":"timestamp with time zone","arguments":[{"kind":"LONG","value":12}]}`, `{"arguments":[{"value":12,"kind":"LONG"}],"rawType":"timestamp with time zone"}`, true},
			{"unbounded-varchar", "varchar", `{"rawType":"varchar","arguments":[{"kind":"LONG","value":2147483647}]}`, `{"arguments":[{"value":2147483647,"kind":"LONG"}],"rawType":"varchar"}`, true},
			{"negative-zero", "decimal(5,0)", `{"rawType":"decimal","arguments":[{"kind":"LONG","value":5},{"kind":"LONG","value":0}]}`, `{"rawType":"decimal","arguments":[{"kind":"LONG","value":5},{"kind":"LONG","value":-0}]}`, true},
		} {
			t.Run(route+"/"+check.name, func(t *testing.T) {
				var peer *wirePeer
				peer = newWirePeer(t, func(writer http.ResponseWriter, request *http.Request, _ []byte) {
					if request.Method == http.MethodDelete {
						writer.WriteHeader(http.StatusNoContent)
						return
					}
					page := map[string]any{"id": "repeated_metadata", "data": [][]any{{nil}}}
					signature := check.later
					if request.Method == http.MethodPost {
						signature = check.first
						page["nextUri"] = peer.next("repeated_metadata", 1)
					}
					page["columns"] = []any{map[string]any{"name": "value", "type": check.text, "typeSignature": json.RawMessage(signature)}}
					writePage(t, writer, page)
				})
				owner, inbox, _ := openPublic(t, peer.settings(), 0)
				var root adapters.Snapshot[trino.Result]
				found := false
				receive := func() {
					snapshot := acknowledge(t, inbox)
					if snapshot.Info().Parent == 0 && snapshot.Info().Operation == "database.trino."+route {
						root, found = snapshot, true
					}
				}
				var value trino.Result
				var err error
				switch route {
				case "query":
					value, err = owner.Client().Query(boundedContext(t), boundedContext(t), trino.Statement{SQL: "SELECT value"})
				case "execute":
					value, err = owner.Client().Execute(boundedContext(t), boundedContext(t), trino.Statement{SQL: "UPDATE fixture SET value=1"})
				case "insert":
					value, err = owner.Client().Insert(boundedContext(t), boundedContext(t), trino.BatchInsert{Table: "fixture", Columns: []string{"value"}, Rows: [][]any{{int64(1)}}})
				case "read":
					reader, setupErr := owner.Client().Read(boundedContext(t), boundedContext(t), trino.Statement{SQL: "SELECT value"})
					if setupErr != nil || reader == nil {
						t.Fatal("reader setup failed", setupErr)
					}
					first, firstErr := reader.Next(boundedContext(t))
					if firstErr != nil || first.Complete() || string(first.DataCopy()) != "[[null]]" || first.ReadProgress().Offset != 0 {
						t.Fatal("validated initial page changed", firstErr)
					}
					receive()
					second, nextErr := reader.Next(boundedContext(t))
					if check.valid {
						if nextErr != nil || second.Complete() || string(second.DataCopy()) != "[[null]]" || second.ReadProgress().Offset != 1 {
							t.Fatal("legitimate repeated metadata changed", nextErr)
						}
						receive()
						if _, endErr := reader.Next(boundedContext(t)); !errors.Is(endErr, io.EOF) {
							t.Fatal("complete reader lost EOF", endErr)
						}
					} else if !errors.Is(nextErr, trino.ErrProtocol) || second.ReadProgress().Sequence != 0 {
						t.Error("invalid later page escaped before type validation", nextErr)
					}
					value, err = reader.Result(boundedContext(t))
				}
				if check.valid {
					if err != nil || !value.Complete() {
						t.Fatal("valid repeated metadata refused", err)
					}
				} else if !errors.Is(err, trino.ErrProtocol) || value.Complete() {
					t.Error("invalid later metadata lost protocol refusal", err)
				}
				wantRows := 1
				if check.valid {
					wantRows = 2
				}
				if route == "execute" || route == "insert" {
					wantRows = 0
					if value.Effect() != trino.Acknowledged {
						t.Error("local metadata failure erased observed terminal mutation acknowledgement")
					}
				} else if value.Effect() != trino.ReadOnly {
					t.Error("selected read-family evidence changed")
				}
				if value.Rows() != wantRows || peer.posts.Load() != 1 || value.Submissions() != 1 || !value.Terminal() || !value.Succeeded() {
					t.Error("repeated metadata replayed work, captured invalid rows or changed terminal facts")
				}
				columns := value.ColumnsCopy()
				if len(columns) != 1 || columns[0].Type != check.text || string(columns[0].Signature) != check.first {
					t.Error("later metadata overwrote exact initial metadata")
				}
				for status, _ := inbox.Inspect(); status.Outstanding > 1; status, _ = inbox.Inspect() {
					receive()
				}
				evidence, present := root.ValueCopy()
				if !found || !present || evidence.Rows() != value.Rows() || evidence.Complete() != value.Complete() || evidence.Effect() != value.Effect() ||
					!check.valid && !errors.Is(root.Primary(), trino.ErrProtocol) {
					t.Error("terminal independent evidence diverged from returned facts")
				}
			})
		}
	}
}
