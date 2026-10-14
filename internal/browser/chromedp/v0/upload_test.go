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
	"context"
	"errors"
	"strings"
	"testing"
)

func TestUploadHostPathsStayLiteralCopiedAndBounded(t *testing.T) {
	type path string
	input := []path{`C:\\browser-host\\fixture.txt`, "/browser-host/path with spaces"}
	output, err := uploadPaths(input, 8, 512)
	if err != nil || len(output) != 2 || output[0] != string(input[0]) || output[1] != string(input[1]) {
		t.Fatal("browser-host paths were rewritten as Go-local paths", err)
	}
	output[0] = "/changed"
	if input[0] == "/changed" {
		t.Fatal("path containers remained aliased")
	}
	output, err = uploadPaths([]string{}, 0, 32)
	if err != nil || output == nil || len(output) != 0 {
		t.Fatal("explicit empty selection lost its native array meaning", err)
	}
	for _, paths := range [][]string{nil, {""}, {"bad\x00path"}} {
		if _, err := uploadPaths(paths, 0, 1024); !errors.Is(err, ErrInput) {
			t.Fatal("malformed host paths were accepted", err)
		}
	}
	if _, err := uploadPaths([]string{strings.Repeat("x", 129)}, 0, 128); !errors.Is(err, ErrLimit) {
		t.Fatal("file path bytes exceeded the admitted bound", err)
	}
	session := unitSession(t)
	session.closed = true
	if _, err := RunAction(context.Background(), session, SetUploadFiles("#file", []string{"/browser-host/file"})); !errors.Is(err, ErrState) {
		t.Fatal("closed page retained file-input authority", err)
	}
}
