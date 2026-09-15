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

package nethttp

import (
	"context"
	"net/http"
	"net/url"
	"testing"
)

func FuzzRequestValidation(f *testing.F) {
	f.Add("GET", "https://example.invalid/path", "X-Input", "value")
	f.Add("POST", "http://127.0.0.1/", "Invalid\nName", "bad")
	f.Fuzz(func(t *testing.T, method, endpoint, name, value string) {
		if len(method)+len(endpoint)+len(name)+len(value) > 65536 {
			return
		}
		address, err := url.Parse(endpoint)
		if err != nil {
			return
		}
		request := &http.Request{Method: method, URL: address, Header: http.Header{name: {value}}}
		limits := defaults(OptionsV1{Name: "fuzz"})
		_ = validateRequest(context.Background(), request, limits)
		if address != nil {
			copy := copyRequest(request, context.Background())
			copy.Header.Set("X-Independent", "changed")
			if request.Header.Get("X-Independent") == "changed" && name != "X-Independent" {
				t.Fatal("request copy aliases caller headers")
			}
		}
	})
}
