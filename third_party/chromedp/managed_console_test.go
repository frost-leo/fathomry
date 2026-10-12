/*
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

package chromedp

import (
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"testing"
)

func TestManagedConsoleRejectsMalformedNativePayload(t *testing.T) {
	for _, raw := range []string{
		`{"method":"Runtime.exceptionThrown","params":{}}`,
		`{"method":"Log.entryAdded","params":{}}`,
		`{"method":"Runtime.consoleAPICalled","params":{"args":[null]}}`,
		`{"method":"Runtime.consoleAPICalled","params":{"stackTrace":{"callFrames":[null]}}}`,
		`{"method":"Runtime.consoleAPICalled","params":{"args":[{"type":"object","preview":{"properties":[null]}}]}}`,
		`{"method":"Runtime.consoleAPICalled","params":{"args":[{"type":"object","preview":{"entries":[null]}}]}}`,
		`{"method":"Unrecognized.event","params":{}}`,
	} {
		t.Run(raw, func(t *testing.T) {
			defer func() {
				if recover() != nil {
					t.Error("malformed console payload panicked")
				}
			}()
			if _, err := decodeConsole(jsonv2.Value(raw)); err == nil {
				t.Error("malformed native event accepted")
			}
		})
	}
}
