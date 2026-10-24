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

package pgx

import (
	"errors"
	"testing"

	"github.com/frost-leo/fathomry/internal/resource"
)

func TestConfiguredArgumentAdmission(t *testing.T) {
	document := make([]byte, 4<<20)
	if !errors.Is(validStatement("SELECT $1::bytea", []any{document}), ErrLimit) {
		t.Fatal("default argument admission changed")
	}
	if err := validStatementLimit("SELECT $1::bytea", []any{document}, 16<<20); err != nil {
		t.Fatal("explicit document budget rejected", err)
	}
	for _, size := range []int{495, 496} {
		err := validStatementLimit("SELECT $1,$2", []any{make([]byte, size), int64(1)}, 1024)
		if size == 495 && err != nil || size == 496 && !errors.Is(err, ErrLimit) {
			t.Fatal("encoded aggregate boundary changed", size, err)
		}
	}
	if err := validStatementLimit("SELECT $1", []any{new(int)}, 16<<20); !errors.Is(err, ErrUnsupported) {
		t.Fatal("larger budget admitted an unsupported argument")
	}
}

func TestArgumentBudgetSettings(t *testing.T) {
	base := testOptions()
	if got := defaults(base); got.MaxArgumentBytes != MaxArgumentBytes || got.reservation() != 50561024 {
		t.Fatal("default argument or work envelope changed")
	}
	for _, size := range []int{-1, 1023, (16 << 20) + 1} {
		candidate := base
		candidate.MaxArgumentBytes = size
		if _, err := Select(candidate); !errors.Is(err, ErrInput) {
			t.Fatal("invalid argument budget admitted", size, err)
		}
	}
	for _, size := range []int{1024, 16 << 20} {
		candidate := base
		candidate.MaxArgumentBytes = size
		candidate.MaxMessageBytes = 16 << 20
		if _, err := Select(candidate); err != nil {
			t.Fatal("supported opt-in envelope rejected", err)
		}
	}
	if _, err := Select(base, resource.Layer{Kind: resource.Local, Content: []byte("max_argument_bytes: 0")}); err == nil {
		t.Fatal("explicit zero layer bypassed validation")
	}
	larger := base
	larger.MaxArgumentBytes = 16 << 20
	if defaults(larger).reservation()-defaults(base).reservation() != (16<<20)-MaxArgumentBytes ||
		EvidenceBytesV1(larger) != EvidenceBytesV1(base) {
		t.Fatal("argument work is omitted or incorrectly retained as evidence")
	}
	config, err := nativeConfig(defaults(larger))
	if err != nil || config.MaxProtocolMessageBodyLen != 1<<20 {
		t.Fatal("argument opt-in silently changed independent read limits", err)
	}
}

func FuzzConfiguredArgumentAdmission(f *testing.F) {
	f.Add([]byte("data"), uint16(1024))
	f.Fuzz(func(t *testing.T, data []byte, budget uint16) {
		if len(data) > 64<<10 {
			t.Skip()
		}
		limit := 1024 + int(budget)
		err := validStatementLimit("SELECT $1", []any{data}, limit)
		if (err == nil) != (2*len(data)+2 <= limit) {
			t.Fatal("bytea encoding reservation mismatch")
		}
	})
}
