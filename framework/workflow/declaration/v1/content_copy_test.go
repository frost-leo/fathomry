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

package declaration

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
)

func TestContentCopyExactIdentity(t *testing.T) {
	raw, err := os.ReadFile("testdata/health.bundle.json")
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := Parse(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	contents, _ := bundle.ContentsCopy()
	for _, expected := range contents {
		actual, found, err := bundle.ContentCopy(expected.Entry.Identity)
		if err != nil || !found || actual.Entry != expected.Entry || !bytes.Equal(actual.Bytes, expected.Bytes) {
			t.Fatal("wrong exact content", err)
		}
		actual.Bytes[0] = '!'
		again, _, _ := bundle.ContentCopy(expected.Entry.Identity)
		if !bytes.Equal(again.Bytes, expected.Bytes) {
			t.Fatal("aliased exact content")
		}
	}
	if _, found, err := bundle.ContentCopy(Identity{}); err != nil || found {
		t.Fatal("missing identity invented")
	}
	var empty *Bundle
	if _, _, err := empty.ContentCopy(Identity{}); !errors.Is(err, ErrHandle) {
		t.Fatal(err)
	}
}
