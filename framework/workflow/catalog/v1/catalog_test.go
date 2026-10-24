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
	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestCatalogConstructionAndOwnership(t *testing.T) {
	for _, options := range []Options{{Timeout: -1}, {Timeout: time.Nanosecond}, {Timeout: time.Minute + 1}, {CleanupTimeout: time.Nanosecond}} {
		if value, err := PostgreSQL(new(postgres.Client), "catalog", options); value != nil || !errors.Is(err, ErrInput) {
			t.Fatal("invalid options accepted")
		}
	}
	if _, err := PostgreSQL(nil, "catalog", Options{}); !errors.Is(err, ErrInput) {
		t.Fatal("nil client accepted")
	}
	if _, err := MySQL(nil, "catalog", Options{}); !errors.Is(err, ErrInput) {
		t.Fatal("nil client accepted")
	}
	for _, dialect := range []string{"postgresql", "mysql"} {
		var value *Catalog
		var err error
		if dialect == "postgresql" {
			value, err = PostgreSQL(new(postgres.Client), "private-target-canary", Options{})
		} else {
			value, err = MySQL(new(mysql.Client), "private-target-canary", Options{})
		}
		if err != nil || !value.Released() {
			t.Fatal(err)
		}
		if _, err := value.LastPublication(); !errors.Is(err, ErrState) {
			t.Fatal("unobserved success fabricated")
		}
		prepared, err := Prepare(context.Background(), registrationInput(t))
		if err != nil {
			t.Fatal(err)
		}
		cause := errors.New("private-cancel-canary")
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)
		report, err := value.Register(ctx, prepared)
		if report.Effect != NotStarted || !report.CleanupComplete || !errors.Is(err, cause) {
			t.Fatal("pre-admission cancellation changed ownership", err)
		}
		copy := *value
		value.state.mu.Lock()
		if copy.Released() {
			t.Fatal("overlap claimed release")
		}
		if _, err := copy.Register(context.Background(), prepared); !errors.Is(err, ErrState) {
			t.Fatal("overlap admitted")
		}
		if err := copy.Close(context.Background()); !errors.Is(err, ErrState) {
			t.Fatal("overlapping close waited")
		}
		value.state.mu.Unlock()
		if _, err := value.Register(context.Background(), prepared); !errors.Is(err, ErrState) {
			t.Fatal("copied close did not seal owner")
		}
		if err := value.Close(context.Background()); err != nil || !value.Released() {
			t.Fatal(err)
		}
		if _, err := json.Marshal(value); !errors.Is(err, ErrSerialization) {
			t.Fatal("catalog runtime serialized")
		}
		if err := json.Unmarshal([]byte("{}"), value); !errors.Is(err, ErrSerialization) {
			t.Fatal("catalog runtime deserialized")
		}
		for _, item := range []any{*value, value, Publication{Namespace: "private-namespace-canary"}} {
			var output bytes.Buffer
			slog.New(slog.NewJSONHandler(&output, nil)).Info("catalog", "value", item)
			if strings.Contains(output.String(), "private-") || strings.Contains(fmt.Sprintf("%+v %#v", item, item), "private-") {
				t.Fatal("private identity leaked")
			}
		}
	}
}
func TestPublicationSeparatesEffectAcknowledgementAndCleanup(t *testing.T) {
	changes := publicationChanges{definitionID: 9223372036854775807, definitionCreated: true, versions: 1}
	for _, test := range []struct {
		ack                string
		complete, released bool
		effect             Effect
		identified         bool
	}{
		{"", true, false, Unknown, false},
		{"finalization-unknown", true, true, Unknown, false},
		{"rollback-acknowledged", false, true, RolledBack, false},
		{"commit-rolled-back", true, true, RolledBack, false},
		{"commit-acknowledged", false, true, Unknown, false},
		{"commit-acknowledged", true, false, Applied, true},
		{"commit-acknowledged", true, true, Applied, true},
	} {
		final := publicationFinal{acknowledgement: test.ack, released: test.released, cleanup: errors.New("cleanup failure")}
		result := publicationResult(Publication{Namespace: "example", Code: "health"}, final, true, test.complete, changes)
		if result.Effect != test.effect || result.CleanupComplete != test.released || !result.CleanupFailed {
			t.Fatal("evidence axes collapsed", result.Effect)
		}
		if test.identified {
			if result.DefinitionID != "9223372036854775807" || !result.Changes.DefinitionCreated || result.Changes.Versions != 1 {
				t.Fatal("known commit lost")
			}
		} else if result.DefinitionID != "" || result.Changes != (Changes{}) {
			t.Fatal("tentative rows became durable facts")
		}
		raw, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		var roundtrip Publication
		if err := json.Unmarshal(raw, &roundtrip); err != nil || roundtrip != result {
			t.Fatal("detached result changed", err)
		}
	}
	noOp := publicationResult(Publication{}, publicationFinal{acknowledgement: "commit-acknowledged", released: true}, true, true, publicationChanges{definitionID: 1})
	if noOp.Effect != Unchanged || noOp.Changes != (Changes{}) {
		t.Fatal("no-op counts changed")
	}
	state := &catalogState{accepted: true, started: true, complete: true, changes: changes, last: noPublication()}
	state.observe(publicationFinal{acknowledgement: "commit-acknowledged", released: true, cleanup: errors.New("late-cleanup-canary")})
	value := &Catalog{state: state}
	for range 2 {
		if err := value.Close(context.Background()); !errors.Is(err, ErrCleanup) {
			t.Fatal("cleanup failure erased", err)
		}
	}
	report, err := value.LastPublication()
	if err != nil || report.Effect != Applied || !report.CleanupFailed {
		t.Fatal("cleanup erased commit", err)
	}
	report.DefinitionID = "changed"
	repeated, _ := value.LastPublication()
	if repeated.DefinitionID == report.DefinitionID {
		t.Fatal("report aliases mutable state")
	}
}
