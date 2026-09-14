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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func TestPageRowByteAndShapeLimits(t *testing.T) {
	for _, mode := range []string{"page", "rows", "result", "pages", "wire", "shape"} {
		t.Run(mode, func(t *testing.T) {
			var base string
			var gets, deletes atomic.Int64
			_, o := peer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodDelete {
					deletes.Add(1)
					w.WriteHeader(204)
					return
				}
				if mode == "page" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, strings.Repeat(" ", 513))
					return
				}
				if mode == "shape" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, "{\"id\":\"limit\",\"data\":"+strings.Repeat("[", 34)+"0"+strings.Repeat("]", 34)+"}")
					return
				}
				if mode == "pages" || mode == "wire" {
					n := gets.Add(1)
					reply(t, w, map[string]any{"id": "limit", "nextUri": base + "/v1/statement/executing/limit/slug/" + strconv.FormatInt(n, 10),
						"stats": map[string]any{"state": strings.Repeat("x", 100)}})
					return
				}
				reply(t, w, map[string]any{"id": "limit", "columns": []any{column("x", "varchar", long(1000))},
					"data": [][]string{{strings.Repeat("x", 100)}, {strings.Repeat("x", 100)}}})
			})
			base = o.Endpoint
			switch mode {
			case "page":
				o.MaxPageBytes = 512
			case "rows":
				o.MaxRows = 1
			case "result":
				o.MaxResultBytes = 300
			case "pages":
				o.MaxPages = 2
			case "wire":
				o.MaxPageBytes = 512
				o.MaxWireBytes = 512
			}
			f := bindFixture(t, o, 1)
			receipt, err := f.client.Query(deadline(t), deadline(t), correlation("limit"), Statement{SQL: "SELECT * FROM data"})
			result := settle(t, receipt, err)
			if !errors.Is(result.Err(), ErrLimit) || result.Outcome.Value.Complete() {
				t.Fatal("limit did not reject")
			}
			if mode == "pages" && (gets.Load() != 2 || deletes.Load() != 1) {
				t.Fatal("page bound or cleanup retry changed")
			}
			if mode == "rows" && !result.Outcome.Value.Terminal() {
				t.Fatal("local row failure erased observed terminal reply")
			}
		})
	}
}
func TestNestedTypesShapeAndStableColumns(t *testing.T) {
	s := defaults(OptionsV1{Endpoint: "http://localhost", Plaintext: true})
	keyType := map[string]any{"kind": "TYPE", "value": map[string]any{"rawType": "varchar", "arguments": []any{long(100)}}}
	sig := map[string]any{"rawType": "row", "arguments": []any{
		map[string]any{"kind": "NAMED_TYPE", "value": map[string]any{"fieldName": map[string]any{"name": "x"},
			"typeSignature": map[string]any{"rawType": "bigint", "arguments": []any{}}}},
		map[string]any{"kind": "NAMED_TYPE", "value": map[string]any{"fieldName": map[string]any{"name": "y"},
			"typeSignature": map[string]any{"rawType": "map", "arguments": []any{keyType, typed("bigint")}}}},
	}}
	columns := []any{map[string]any{"name": "nested", "type": "row(x bigint,y map(varchar,bigint))", "typeSignature": sig}}
	for _, good := range []bool{true, false} {
		t.Run(fmt.Sprint(good), func(t *testing.T) {
			e := newExchange(s, true, true, nil)
			row := []any{int64(7), map[string]any{"key": json.Number("9223372036854775807")}}
			if !good {
				row = []any{int64(7)}
			}
			body, _ := json.Marshal(map[string]any{"id": "nested", "columns": columns, "data": [][]any{{row}}})
			err := e.page(body)
			if good && err != nil || !good && !errors.Is(err, ErrProtocol) {
				t.Fatal("nested shape validation changed")
			}
		})
	}
	e := newExchange(s, true, true, nil)
	first, _ := json.Marshal(map[string]any{"id": "stable", "nextUri": s.Endpoint + "/v1/statement/executing/stable/slug/1", "columns": []any{column("x", "bigint")}})
	if err := e.page(first); err != nil {
		t.Fatal(err)
	}
	last, _ := json.Marshal(map[string]any{"id": "stable", "columns": []any{column("y", "bigint")}})
	if err := e.page(last); !errors.Is(err, ErrProtocol) {
		t.Fatal("changed schema accepted")
	}
}
func FuzzDirectPage(f *testing.F) {
	for _, seed := range []string{"{\"id\":\"q\"}", "{\"id\":\"q\",\"data\":[[]]}", "{\"id\":\"q\",\"error\":{}}", "{\"id\":\"q\",\"data\":{\"encoding\":\"json\",\"segments\":[]}}"} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 4096 {
			t.Skip()
		}
		s := defaults(OptionsV1{Endpoint: "http://localhost", Plaintext: true, MaxPageBytes: 4096, MaxResultBytes: 4096})
		e := newExchange(s, true, true, nil)
		_ = e.page(body)
		if len(e.data.json) > s.MaxResultBytes || e.data.rows > s.MaxRows {
			t.Fatal("unbounded result")
		}
	})
}
