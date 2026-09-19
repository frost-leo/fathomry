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

package cli

import (
	"math/rand"
	"testing"

	"github.com/frost-leo/fathomry/i18n"
)

func TestLocaleGrammarMatchesCatalog(t *testing.T) {
	catalog, err := i18n.Prepare(Resources())
	if err != nil {
		t.Fatal(err)
	}
	check := func(locale string) {
		t.Helper()
		_, err := catalog.Render(locale, "fathomry.cli.usage", nil)
		if validLocale(locale) != (err == nil) {
			t.Errorf("locale %q: CLI valid=%v catalog error=%v", locale, validLocale(locale), err)
		}
	}
	for _, locale := range []string{
		"en", "zh", "zh-CN", "zh-Hans-CN", "fr-FR", "ja-JP", "und", "de-1901",
		"en-u-ca-gregory", "en_US", "en--US", "a", "en-123", "en-GB-oed",
	} {
		check(locale)
	}
	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
	random := rand.New(rand.NewSource(77))
	for range 2000 {
		length := 1 + random.Intn(20)
		value := make([]byte, length)
		for index := range value {
			value[index] = letters[random.Intn(len(letters))]
		}
		check(string(value))
	}
}
