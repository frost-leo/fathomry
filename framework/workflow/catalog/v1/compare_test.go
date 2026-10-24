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

package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"log/slog"
	"strings"
	"testing"
)

func TestComparisonPreparationAndBounds(t *testing.T) {
	if _, err := prepareComparison(nil); !errors.Is(err, ErrHandle) {
		t.Fatal("unprepared comparison admitted")
	}
	input := registrationInput(t)
	input.Category = &Category{Code: "initial", Name: &Text{English: "Initial"}}
	input.Tags = map[string]string{"team": "old"}
	prepared, err := Prepare(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := prepareComparison(prepared)
	if err != nil {
		t.Fatal(err)
	}
	var decoded comparisonInput
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Documents) != len(prepared.state.contents) || len(decoded.Resources) != len(prepared.state.resources) {
		t.Fatal("selected shape changed")
	}
	if bytes.Contains(raw, []byte("workflow.category")) || bytes.Contains(raw, []byte("team")) {
		t.Fatal("initialization metadata entered compare anchor")
	}
	if MaxComparisonRows != 1+declaration.MaxWorkflows+declaration.MaxModes+MaxResources {
		t.Fatal("row profile drift")
	}
	for _, tx := range []*publicationTx{{pg: new(postgres.Transaction), target: "\"catalog\""}, {target: "`catalog`"}} {
		query, args := tx.comparisonSQL(prepared.state, raw)
		if len(query) > 64<<10 || len(args) != 3 || !strings.Contains(query, "UNION ALL") || !strings.HasPrefix(query, "WITH request AS") {
			t.Fatal("comparison became unbounded SQL")
		}
	}
	result := Comparison{Namespace: "private-namespace-canary", Problems: []ComparisonProblem{}}
	for index := range MaxComparisonProblems + 1 {
		result.addComparisonProblem(ComparisonProblem{Kind: "content_missing", Path: fmt.Sprintf("/contents/%d", index)})
	}
	if !result.Truncated || len(result.Problems) != MaxComparisonProblems {
		t.Fatal("diagnostic ceiling bypassed")
	}
	var log bytes.Buffer
	slog.New(slog.NewJSONHandler(&log, nil)).Info("result", "comparison", result)
	if strings.Contains(log.String(), "private-") || strings.Contains(fmt.Sprintf("%+v", result), "private-") {
		t.Fatal("formatted comparison leaked identities")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store, err := PostgreSQL(new(postgres.Client), "catalog", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := store.Compare(ctx, prepared); !errors.Is(err, context.Canceled) || result.Complete || !store.Released() {
		t.Fatal("canceled comparison entered work", err)
	}
}

func FuzzComparisonAnchorShape(f *testing.F) {
	rows := [][][]byte{}
	for _, item := range []struct{ kind, index string }{{"definition", "-1"}, {"document", "0"}, {"document", "1"}, {"resource", "0"}, {"resource", "1"}} {
		row := make([][]byte, 15)
		row[0], row[1], row[2], row[11], row[12] = []byte(item.kind), []byte(item.index), []byte("1"), []byte("1"), []byte("1")
		rows = append(rows, row)
	}
	seed, _ := json.Marshal(rows)
	f.Add(seed)
	f.Add([]byte("[]"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64<<10 {
			t.Skip()
		}
		var rows [][][]byte
		if json.Unmarshal(raw, &rows) != nil {
			return
		}
		input := &registration{contents: make([]declaration.Content, 2), resources: make([]publicationResource, 2)}
		anchor, err := (&publicationTx{}).comparisonRows(rows, input)
		if err == nil {
			if len(anchor.documents) != 2 || len(anchor.resources) != 2 {
				t.Fatal("accepted shape lost selection")
			}
			for _, row := range append(anchor.documents, anchor.resources...) {
				if len(row) != 15 {
					t.Fatal("accepted incomplete anchor")
				}
			}
		}
	})
}
