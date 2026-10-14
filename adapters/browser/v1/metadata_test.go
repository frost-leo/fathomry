/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package browser_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestObservationKeepsEffectAndCleanupIndependent(t *testing.T) {
	values := []browser.Observation{
		{},
		{Returned: true, Effect: browser.Unknown},
		{Effect: browser.Acknowledged, ContextDetached: true},
		{ContextDisposed: true, StreamClosed: true, EOF: false},
		{Transferred: true, ProcessExited: false},
	}
	if values[0].Effect != browser.NotSubmitted {
		t.Fatal("zero effect changed")
	}
	for _, value := range values {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var copy browser.Observation
		if err := json.Unmarshal(data, &copy); err != nil || copy != value {
			t.Fatal("observation axes were conflated", err)
		}
	}
}
func TestAttributionPreservesCapturedGenerationAndSafeDiagnostics(t *testing.T) {
	const canary = "private-browser-origin"
	origin := adapters.Source{Name: canary, Generation: 3}
	value := browser.Attribution{Runtime: canary, Sequence: 7, Parent: 2, Source: origin}
	origin.Generation = 4
	if value.Source.Generation != 3 {
		t.Fatal("captured attribution followed later mutation")
	}
	for _, verb := range []string{"%v", "%+v", "%#v"} {
		if strings.Contains(fmt.Sprintf(verb, value), canary) {
			t.Fatal("ordinary attribution disclosure")
		}
	}
	var output bytes.Buffer
	slog.New(slog.NewJSONHandler(&output, nil)).Info("observation", "origin", value)
	if strings.Contains(output.String(), canary) {
		t.Fatal("slog disclosed attribution")
	}
}
