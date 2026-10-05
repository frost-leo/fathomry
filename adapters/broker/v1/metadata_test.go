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

package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func TestRuntimeAttributionIsNotAnImplicitWireFormat(t *testing.T) {
	for _, value := range []any{Info{Name: "secret-canary"}, Attribution{ID: "secret-canary"}, (*Info)(nil), (*Attribution)(nil)} {
		if strings.Contains(fmt.Sprintf("%+v %#v", value, value), "secret-canary") {
			t.Fatal("runtime metadata formatted")
		}
		var output bytes.Buffer
		slog.New(slog.NewJSONHandler(&output, nil)).Info("metadata", "value", value)
		if strings.Contains(output.String(), "secret-canary") {
			t.Fatal("runtime metadata implicitly logged")
		}
	}
	if _, err := json.Marshal(Info{Name: "secret-canary"}); err == nil {
		t.Fatal("runtime metadata serialized")
	}
}
