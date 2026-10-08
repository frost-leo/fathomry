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

package surf

import (
	"context"
	"io"
	"strings"
	"testing"
	"unicode/utf8"

	http "github.com/enetx/http"
)

func FuzzMultipartAdmission(f *testing.F) {
	f.Add("field", "file.txt", "application/octet-stream", "body", uint8(0), false)
	f.Add("bad\r\nname", "file.txt", "", "", uint8(129), true)
	f.Fuzz(func(t *testing.T, name, filename, contentType, body string, count uint8, retries bool) {
		if len(name)+len(filename)+len(contentType)+len(body) > 8192 {
			return
		}
		called := false
		input := &Multipart{Fields: []Field{{Name: name, Value: body}}}
		for range int(count) {
			input.Parts = append(input.Parts, Part{Name: name, FileName: filename, ContentType: contentType,
				Open: func(context.Context) (io.ReadCloser, error) {
					called = true
					return io.NopCloser(strings.NewReader(body)), nil
				}})
		}
		value := settings{MaxHeaderBytes: 4096, MaxRequestBytes: 4096}
		if retries {
			value.NativeRetries = 1
		}
		copied, replayable, err := copyMultipart(input, &http.Request{}, value)
		if called {
			t.Fatal("offline admission invoked input factory")
		}
		if int(count)+1 > MaxMultipartParts && err == nil {
			t.Fatal("oversized part count admitted")
		}
		if err == nil {
			if name == "" || !utf8.ValidString(name) || !fieldValue(name) || !replayable || len(body) > 4096 {
				t.Fatal("invalid field contract admitted")
			}
			input.Fields[0].Name = "mutated"
			if copied.Fields[0].Name != name {
				t.Fatal("field slice aliases caller")
			}
			if count != 0 {
				input.Parts[0].Name = "mutated"
				if copied.Parts[0].Name != name {
					t.Fatal("part slice aliases caller")
				}
			}
		}
	})
}
