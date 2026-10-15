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

package notification

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestMetadataCloningAndObservations(t *testing.T) {
	value := Info{Name: "private-canary", Provenance: []LayerInfo{{Kind: 1, Fields: []string{"field"}}}}
	cloned := value.Clone()
	cloned.Provenance[0].Fields[0] = "changed"
	if value.Provenance[0].Fields[0] != "field" {
		t.Fatal("provenance alias")
	}
	if fmt.Sprintf("%+v", value) != "notification[restricted]" {
		t.Fatal("diagnostic leak")
	}
	if _, err := json.Marshal(value); err == nil {
		t.Fatal("runtime metadata serialized")
	}
	seen := map[Observation]bool{}
	for _, observation := range []Observation{RelayAccepted, PlatformAcknowledged, CallbackAuthenticated, AcknowledgementWritten, LocalProcessSucceeded, RecipientReadObserved} {
		if observation == "" || seen[observation] {
			t.Fatal("distinct observations collapsed")
		}
		seen[observation] = true
	}
}
