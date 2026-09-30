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

package i18n

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/text/language"
)

func oracle(locale string, number uint64) Form {
	switch locale {
	case "en":
		if number == 1 {
			return One
		}
	case "ru":
		if number%10 == 1 && number%100 != 11 {
			return One
		}
		if number%10 >= 2 && number%10 <= 4 && !(number%100 >= 12 && number%100 <= 14) {
			return Few
		}
		return Many
	case "ar":
		switch number {
		case 0:
			return Zero
		case 1:
			return One
		case 2:
			return Two
		}
		if number%100 >= 3 && number%100 <= 10 {
			return Few
		}
		if number%100 >= 11 && number%100 <= 99 {
			return Many
		}
	}
	return Other
}
func TestIntegerRendering(t *testing.T) {
	catalog := mustCatalog(t, fixtureComponent())
	numbers := []uint64{0, 1, 2, 3, 10, 11, 21, 22, 99, 100, 101, 800, 1000000, 1000001, 10000000, 10000001, 1<<53 + 1, 1<<63 + 1, math.MaxUint64}
	for _, locale := range []string{"en", "zh-CN", "ru", "ar"} {
		selected, err := catalog.Resolve("example.source.items", locale)
		if err != nil {
			t.Fatal(err)
		}
		for _, number := range numbers {
			rendered, err := selected.Render([]Argument{{Name: "label", Value: "public"}}, &number)
			if err != nil || rendered.Category != oracle(locale, number) || !strings.Contains(rendered.Text, strconv.FormatUint(number, 10)) || rendered.Locale != locale {
				t.Fatalf("integer rendering changed %s/%d: %v", locale, number, err)
			}
		}
	}
	if strconv.IntSize == 64 {
		comparisons := 0
		for _, base := range language.Supported.BaseLanguages() {
			tag, err := language.BCP47.Parse(base.String())
			if err != nil {
				continue
			}
			for _, number := range []uint64{800, 9999999, 10000000, 10000001, 1000000001, 1<<53 + 1, math.MaxInt64} {
				want := cardinalRules.MatchPlural(tag, int(number), 0, 0, 0, 0)
				representative := number
				if number >= 10000000 {
					representative = 10000000 + number%10000000
				}
				if got := cardinalRules.MatchPlural(tag, int(representative), 0, 0, 0, 0); got != want {
					t.Fatal("representative changed native category")
				}
				comparisons++
			}
		}
		if comparisons < 700 {
			t.Fatal("vacuous native comparison")
		}
		t.Logf("native wide-integer comparisons=%d", comparisons)
	}
}

type hostileScalar struct{}

func (hostileScalar) Format(fmt.State, rune) { panic("must not format") }
func (hostileScalar) String() string         { panic("must not format") }
func TestRenderBoundaries(t *testing.T) {
	catalog := mustCatalog(t, fixtureComponent())
	selected, _ := catalog.Resolve("example.source.details", "en")
	for _, input := range []any{hostileScalar{}, 1, float64(1), nil, []byte("private")} {
		if _, err := selected.Render([]Argument{{Name: "label", Value: input}}, nil); !errors.Is(err, ErrArguments) {
			t.Fatal("unsafe scalar accepted")
		}
	}
	if _, err := selected.Render(nil, nil); !errors.Is(err, ErrArguments) {
		t.Fatal("missing argument accepted")
	}
	if _, err := selected.Render([]Argument{{Name: "label", Value: strings.Repeat("x", MaxStringBytes+1)}}, nil); !errors.Is(err, ErrLimit) {
		t.Fatal("string bound ignored")
	}
	count := uint64(1)
	if _, err := selected.Render([]Argument{{Name: "label", Value: "public"}}, &count); !errors.Is(err, ErrArguments) {
		t.Fatal("unexpected count accepted")
	}
	program, err := compile(strings.Repeat("{label}", 64), []Parameter{{Name: "label", Kind: "string"}}, false)
	if err != nil {
		t.Fatal(err)
	}
	selection := Selection{item: &entry{definition: Definition{Arguments: []Parameter{{Name: "label", Kind: "string"}}}, programs: map[Form][]segment{Other: program}}}
	if output, err := selection.Render([]Argument{{Name: "label", Value: strings.Repeat("x", 4096)}}, nil); !errors.Is(err, ErrLimit) || output.Text != "" {
		t.Fatal("oversize expansion returned output")
	}
	if _, err := (Selection{}).Render(nil, nil); !errors.Is(err, ErrCatalog) {
		t.Fatal("zero selection rendered")
	}
}
func FuzzCardinal(f *testing.F) {
	for _, number := range []uint64{0, 1, 2, 11, 10000000, 1<<53 + 1, math.MaxUint64} {
		f.Add(number)
	}
	f.Fuzz(func(t *testing.T, number uint64) {
		for _, locale := range []string{"en", "zh-CN", "ru", "ar"} {
			if got := cardinalForm(language.MustParse(locale), number); got != oracle(locale, number) {
				t.Fatalf("wrong category %s/%d", locale, number)
			}
		}
	})
}
