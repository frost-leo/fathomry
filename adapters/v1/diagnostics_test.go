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

package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

type hostileFact struct{ Secret string }

func (hostileFact) String() string               { panic("private-format-canary") }
func (hostileFact) GoString() string             { panic("private-format-canary") }
func (hostileFact) Format(fmt.State, rune)       { panic("private-format-canary") }
func (hostileFact) Error() string                { panic("private-format-canary") }
func (hostileFact) LogValue() slog.Value         { panic("private-format-canary") }
func (hostileFact) MarshalJSON() ([]byte, error) { panic("private-format-canary") }

func TestDiagnostics(t *testing.T) {
	owner := testRuntime(t, Options{})
	inbox, _ := NewInbox[hostileFact](EvidenceOptions{})
	observer, _ := NewObserver(4)
	declaration := Declaration[hostileFact]{Copy: func(value hostileFact) hostileFact { return value }, Evidence: inbox, Observer: observer}
	endpoint, err := Bind(owner, declaration)
	if err != nil {
		t.Fatal(err)
	}
	var producer *Call[hostileFact]
	var guard Guard
	input := request("private.safe")
	input.ID = "private-correlation-canary"
	cause := hostileFact{Secret: "private-cause-canary"}
	outcome := Outcome[hostileFact]{Value: hostileFact{Secret: "private-payload-canary"}, Present: true, Primary: cause, Cleanup: cause}
	receipt, err := endpoint.Run(context.Background(), input, func(call *Call[hostileFact]) {
		producer = call
		guard, _ = call.Hold()
		_ = call.Resolve(outcome)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Release()
	snapshot, _ := receipt.Snapshot()
	scope, info := producer.Scope(), snapshot.Info()
	delivery, _ := inbox.Next(context.Background())
	handles := []any{owner, *owner, endpoint, &endpoint, producer, *producer, scope, &scope, guard, &guard,
		receipt, *receipt, snapshot, &snapshot, inbox, *inbox, delivery, &delivery, declaration, &declaration,
		outcome, &outcome, observer, *observer, info, &info, input, &input}
	for _, handle := range handles {
		t.Run(fmt.Sprintf("%T", handle), func(t *testing.T) {
			var output bytes.Buffer
			for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
				fmt.Fprintf(&output, format, handle)
			}
			output.WriteString(handle.(fmt.Stringer).String())
			output.WriteString(handle.(fmt.GoStringer).GoString())
			slog.New(slog.NewJSONHandler(&output, nil)).Error("diagnostic", "handle", handle)
			if strings.Contains(output.String(), "canary") || strings.Contains(output.String(), "PANIC") {
				t.Fatal("implicit diagnostics reached sensitive/native data")
			}
			if _, err := json.Marshal(handle); !errors.Is(err, ErrSerialization) {
				t.Fatal("runtime value serialized")
			}
		})
	}
	for _, handle := range []json.Unmarshaler{owner, &endpoint, producer, &scope, &guard, receipt, &snapshot, inbox, &delivery, &declaration, &outcome, observer, &info, &input} {
		if err := json.Unmarshal([]byte("{}"), handle); !errors.Is(err, ErrSerialization) {
			t.Fatal("runtime authority reconstructed")
		}
	}
	for _, handle := range []slog.LogValuer{(*Runtime)(nil), (*Endpoint[int])(nil), (*Call[int])(nil), (*Scope)(nil), (*Guard)(nil),
		(*Receipt[int])(nil), (*Snapshot[int])(nil), (*Inbox[int])(nil), (*Delivery[int])(nil), (*Declaration[int])(nil),
		(*Outcome[int])(nil), (*Observer)(nil), (*Info)(nil), (*Request)(nil)} {
		if strings.Contains(handle.LogValue().Resolve().String(), "PANIC") {
			t.Fatal("nil diagnostic panicked")
		}
	}
	reported := snapshot.Err()
	var retained hostileFact
	if !errors.Is(reported, ErrOperation) || !errors.As(reported, &retained) || retained.Secret != cause.Secret {
		t.Fatal("original native evidence lost")
	}
	var output bytes.Buffer
	fmt.Fprintf(&output, "%+v", reported)
	slog.New(slog.NewJSONHandler(&output, nil)).Error("operation", "error", reported)
	if strings.Contains(output.String(), "canary") || strings.Contains(output.String(), "PANIC") {
		t.Fatal("safe failure formatted native evidence")
	}
	_ = guard.Release()
	if delivery.Ack() != nil {
		t.Fatal("serialization refusal mutated custody")
	}
}
