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
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

func TestNativeBoundaryCommentGatesRejectBeforeSubmission(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		query bool
	}{
		{"nested-comment-delete", "/* /* */ DELETE FROM data -- */ SELECT 1", true},
		{"cr-explain-analyze", "EXPLAIN -- comment\rANALYZE DELETE FROM data", true},
		{"cr-maintenance", "ALTER TABLE data -- comment\rEXECUTE remove_orphan_files", false},
		{"nested-comment-session", "WITH /* /* */ SESSION retry_policy='TASK' -- */\nSELECT 1", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var posts atomic.Int64
			_, options := peer(t, func(w http.ResponseWriter, request *http.Request) {
				if request.Method == http.MethodDelete {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				body, _ := io.ReadAll(request.Body)
				if string(body) != test.sql {
					t.Error("native driver changed the reproduced statement")
				}
				posts.Add(1)
				reply(t, w, map[string]any{"id": "comment_gate", "updateCount": 1})
			})
			options.Writes = !test.query
			options.Maintenance = false
			fixture := bindFixture(t, options, 1)
			operation := fixture.client.Execute
			if test.query {
				operation = fixture.client.Query
			}
			receipt, err := operation(deadline(t), deadline(t), correlation(test.name), Statement{SQL: test.sql})
			result := settle(t, receipt, err)
			value := result.Outcome.Value
			if result.Err() == nil || posts.Load() != 0 || value.Effect() != NotSubmitted {
				t.Fatalf("excluded SQL entered native HTTP: posts=%d error=%t effect=%d complete=%t", posts.Load(), result.Err() != nil, value.Effect(), value.Complete())
			}
		})
	}
}

func TestNativeBoundaryOrdinaryCommentPositiveControls(t *testing.T) {
	settings := defaults(OptionsV1{})
	for _, sql := range []string{
		"SELECT 1 /* ordinary */",
		"-- comment\rSELECT 1",
		"SELECT '-- not a comment'",
		"SELECT 1 -- tail\r\n",
	} {
		read, err := prepare(Statement{SQL: sql}, settings, true)
		if err != nil || !read {
			t.Errorf("ordinary read rejected: sql=%q error=%t read=%t", sql, err != nil, read)
		}
	}
}

func TestNativeBoundaryMalformedCellsReject(t *testing.T) {
	tests := []struct {
		name   string
		column string
		value  string
	}{
		{"decimal-overflow", `{"name":"x","type":"decimal(2,1)","typeSignature":{"rawType":"decimal","arguments":[{"kind":"LONG","value":2},{"kind":"LONG","value":1}]}}`, `"12345.6"`},
		{"decimal-scale", `{"name":"x","type":"decimal(2,1)","typeSignature":{"rawType":"decimal","arguments":[{"kind":"LONG","value":2},{"kind":"LONG","value":1}]}}`, `"1.23"`},
		{"map-key", `{"name":"x","type":"map(bigint,bigint)","typeSignature":{"rawType":"map","arguments":[{"kind":"TYPE","value":{"rawType":"bigint","arguments":[]}},{"kind":"TYPE","value":{"rawType":"bigint","arguments":[]}}]}}`, `{"not-an-integer":1}`},
		{"real-overflow", `{"name":"x","type":"real","typeSignature":{"rawType":"real","arguments":[]}}`, `1e100`},
		{"column-type-mismatch", `{"name":"x","type":"bigint","typeSignature":{"rawType":"boolean","arguments":[]}}`, `true`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, options := peer(t, func(w http.ResponseWriter, request *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"id":"bad_cell","columns":[`+test.column+`],"data":[[`+test.value+`]]}`)
			})
			fixture := bindFixture(t, options, 1)
			receipt, err := fixture.client.Query(deadline(t), deadline(t), correlation(test.name), Statement{SQL: "SELECT * FROM data"})
			result := settle(t, receipt, err)
			if !errors.Is(result.Err(), ErrProtocol) || result.Outcome.Value.Complete() {
				t.Fatalf("malformed typed cell accepted: error=%t complete=%t ", result.Err() != nil, result.Outcome.Value.Complete())
			}
		})
	}
}

func TestNativeBoundaryCaseAliasCannotEraseServerError(t *testing.T) {
	_, options := peer(t, func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"case_alias","error":{"errorCode":123,"errorName":"SYNTHETIC_ERROR","errorType":"EXTERNAL","message":"synthetic failed write"},"ERROR":null}`)
	})
	fixture := bindFixture(t, options, 1)
	receipt, err := fixture.client.Execute(deadline(t), deadline(t), correlation("case-alias"), Statement{SQL: "INSERT INTO data VALUES (1)"})
	result := settle(t, receipt, err)
	value := result.Outcome.Value
	if result.Err() == nil || value.Succeeded() || value.Complete() || value.Effect() == Acknowledged {
		t.Fatalf("canonical protocol error was erased: error=%t success=%t complete=%t effect=%d", result.Err() != nil, value.Succeeded(), value.Complete(), value.Effect())
	}
}

func TestNativeBoundaryCaseSensitiveMapAndNumericPositiveControls(t *testing.T) {
	_, options := peer(t, func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"valid_cells","columns":[{"name":"map","type":"map(varchar,bigint)","typeSignature":{"rawType":"map","arguments":[{"kind":"TYPE","value":{"rawType":"varchar","arguments":[{"kind":"LONG","value":8}]}},{"kind":"TYPE","value":{"rawType":"bigint","arguments":[]}}]}},{"name":"decimal","type":"decimal(2,1)","typeSignature":{"rawType":"decimal","arguments":[{"kind":"LONG","value":2},{"kind":"LONG","value":1}]}},{"name":"real","type":"real","typeSignature":{"rawType":"real","arguments":[]}}],"data":[[{"A":1,"a":2},"-9.9",3.4028235e38],[{},"0.0","Infinity"]]}`)
	})
	f := bindFixture(t, options, 1)
	receipt, err := f.client.Query(deadline(t), deadline(t), correlation("valid-cells"), Statement{SQL: "SELECT * FROM data"})
	value := success(t, settle(t, receipt, err))
	if value.Rows() != 2 || !strings.Contains(string(value.DataCopy()), `{"A":1,"a":2}`) {
		t.Fatal("case-sensitive map keys or valid numeric limits changed")
	}
}
