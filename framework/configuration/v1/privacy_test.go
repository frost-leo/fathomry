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

package configuration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
)

type sensitiveProject struct {
	Secret string            `json:"secret"`
	Values map[string]string `json:"values"`
}
type panicCause struct{}

func (*panicCause) Error() string { panic("cause-must-not-be-presented") }
func assertPrivate(t testing.TB, value any) {
	t.Helper()
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%d"} {
		output := fmt.Sprintf(format, value)
		if strings.Contains(output, "private-canary") || strings.Contains(output, "PANIC") {
			t.Errorf("unsafe formatting: %q", output)
		}
	}
	for _, jsonOutput := range []bool{false, true} {
		var buffer bytes.Buffer
		var handler slog.Handler = slog.NewTextHandler(&buffer, nil)
		if jsonOutput {
			handler = slog.NewJSONHandler(&buffer, nil)
		}
		slog.New(handler).Info("test", "runtime", value)
		output := buffer.String()
		if strings.Contains(output, "private-canary") || strings.Contains(output, "panicked") {
			t.Errorf("unsafe logging: %s", output)
		}
	}
}
func TestRuntimePrivacyAndNilAccess(t *testing.T) {
	schema := Schema[sensitiveProject]{FormatVersion: 1, Defaults: DefaultSettings(sensitiveProject{Secret: "private-canary", Values: map[string]string{"value": "private-canary"}})}
	input := Plan{Variables: []Variable{{Name: "private-canary", Field: "/project/secret"}}}
	snapshot, err := Load(context.Background(), schema, Plan{})
	if err != nil {
		t.Fatal(err)
	}
	view, err := snapshot.Presentation()
	if err != nil {
		t.Fatal(err)
	}
	state := State[sensitiveProject]{Snapshot: snapshot, Status: Ready}
	for _, value := range []any{schema, &schema, input, &input, snapshot, &snapshot, view, &view, state, &state, input.Variables[0], Input{}, LayerDocument{}, Cursor{}} {
		assertPrivate(t, value)
		if raw, err := json.Marshal(value); err == nil {
			t.Errorf("runtime serialized: %T %s", value, raw)
		}
	}
	for _, target := range []any{new(Schema[sensitiveProject]), new(Plan), new(Snapshot[sensitiveProject]), new(Presentation), new(State[sensitiveProject]), new(Cursor), new(Input), new(Variable), new(LayerDocument)} {
		if err := json.Unmarshal([]byte("{}"), target); err == nil {
			t.Errorf("runtime reconstructed: %T", target)
		}
	}
	for _, value := range []any{(*Schema[sensitiveProject])(nil), (*Plan)(nil), (*Snapshot[sensitiveProject])(nil), (*Presentation)(nil), (*State[sensitiveProject])(nil), (*Live[sensitiveProject])(nil), (*Cursor)(nil), (*Input)(nil), (*Variable)(nil), (*LayerDocument)(nil)} {
		assertPrivate(t, value)
		raw, err := json.Marshal(value)
		if err != nil || string(raw) != "null" {
			t.Fatal("Go nil-pointer JSON semantics changed")
		}
	}
	var live *Live[sensitiveProject]
	if _, err := live.Current(); !errors.Is(err, ErrValue) {
		t.Fatal("nil Current", err)
	}
	if _, err := live.Next(context.Background(), Cursor{}); !errors.Is(err, ErrValue) {
		t.Fatal("nil Next", err)
	}
	if err := live.Close(context.Background()); !errors.Is(err, ErrValue) {
		t.Fatal("nil Close", err)
	}
	cause := &panicCause{}
	schema.Validate = func(Settings[sensitiveProject]) error { return cause }
	_, err = Load(context.Background(), schema, Plan{})
	if !errors.Is(err, cause) {
		t.Fatal("validator cause removed")
	}
	if _, found := failure.Inspect(err); !found {
		t.Fatal("missing direct occurrence")
	}
	assertPrivate(t, err)
	data, err := json.Marshal(schema.Defaults)
	if err != nil || !bytes.Contains(data, []byte("private-canary")) {
		t.Fatal("plain Settings accidentally guarded")
	}
}
