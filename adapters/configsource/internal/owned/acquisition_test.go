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

package owned

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/failure/v1"
)

type refusingCause struct{}

func (*refusingCause) Error() string { panic("foreign cause must not be formatted") }
func TestAcquisitionOccurrenceOwnsFactsAndGuards(t *testing.T) {
	cause := &refusingCause{}
	info := source.AcquisitionInfo{Source: "one", Document: "slot", Phase: source.CapturePhase}
	err := Acquisition("example.source.read", info, cause)
	typed, ok := err.(source.AcquisitionFailure)
	if !ok {
		t.Fatal("extension missing")
	}
	core, ok := failure.Inspect(err)
	if !ok || typed.Failure() != core || !errors.Is(err, cause) {
		t.Fatal("occurrence/cause association lost")
	}
	info.Source = "mutated"
	first, valid := typed.Acquisition()
	if !valid || first.Source != "one" {
		t.Fatal("facts alias")
	}
	first.Document = "mutated"
	second, _ := typed.Acquisition()
	if second.Document != "slot" {
		t.Fatal("projection alias")
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%q", "%d"} {
		_ = fmt.Sprintf(format, err)
	}
	if _, err := json.Marshal(typed); !errors.Is(err, failure.ErrSerialization) {
		t.Fatal("typed error serialized")
	}
	if err := json.Unmarshal([]byte("{}"), new(acquisitionFailure)); !errors.Is(err, failure.ErrSerialization) {
		t.Fatal("typed error reconstructed")
	}
	var empty *acquisitionFailure
	if _, ok := empty.Acquisition(); ok {
		t.Fatal("nil facts valid")
	}
	if _, ok := failure.Inspect(empty); ok {
		t.Fatal("nil occurrence valid")
	}
	_ = empty.Error()
	_ = fmt.Sprint(empty)
	_ = slog.AnyValue(empty).Resolve()
	for _, bad := range []source.AcquisitionInfo{{Source: "bad name", Phase: source.CapturePhase}, {Source: "one", Document: "/private", Phase: source.CapturePhase}, {Source: "one", Phase: "native-secret"}} {
		if err := Acquisition("example.source.read", bad); !errors.Is(err, source.ErrValue) {
			t.Fatal("unbounded/invalid facts accepted")
		}
	}
}
