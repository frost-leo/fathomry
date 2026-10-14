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

package docs_test

import (
	"strings"
	"testing"
)

const fathomryNotice = `fathomry
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
along with this program. If not, see <http://www.gnu.org/licenses/>.`

func starredNotice() string {
	lines := strings.Split(fathomryNotice, "\n")
	for i, line := range lines {
		lines[i] = " *"
		if line != "" {
			lines[i] += " " + line
		}
	}
	return strings.Join(lines, "\n")
}

// maskLicenseNotice exempts only the exact required legal text, not its file.
func maskLicenseNotice(text string) string {
	for _, notice := range []string{fathomryNotice, starredNotice()} {
		blank := strings.Map(func(r rune) rune {
			if r == '\n' {
				return r
			}
			return ' '
		}, notice)
		text = strings.ReplaceAll(text, notice, blank)
	}
	return text
}

func TestLicenseNoticeExactExemption(t *testing.T) {
	for _, notice := range []string{fathomryNotice, starredNotice()} {
		for _, wrap := range [][2]string{{"<!--\n", "\n-->"}, {"/*\n", "\n*/"}, {"\n", "\n"}} {
			input := wrap[0] + notice + wrap[1]
			masked := maskLicenseNotice(input)
			if len(masked) != len(input) || strings.Count(masked, "\n") != strings.Count(input, "\n") {
				t.Fatal("license exemption changed diagnostic positions")
			}
			for name, rule := range proseRules {
				if rule.MatchString(masked) {
					t.Fatal("exact legal text still triggered prose rule", name)
				}
			}
		}
	}
}

func TestLicenseNoticeExemptionRejectsPartialOrChangedText(t *testing.T) {
	for _, input := range []string{
		strings.Replace(fathomryNotice, "2026", "2025", 1),
		strings.TrimPrefix(fathomryNotice, "fathomry\n"),
		strings.Replace(fathomryNotice, "Free Software Foundation", "another owner", 1),
		strings.Replace(fathomryNotice, "ANY WARRANTY", "A WARRANTY", 1),
		"You should have received a copy of the GNU General Public License",
		"but WITHOUT ANY WARRANTY; without even the implied warranty of",
	} {
		if maskLicenseNotice(input) != input {
			t.Fatal("inexact text received a legal-text exemption")
		}
		if !proseRules["modal"].MatchString(input) && !proseRules["semicolon"].MatchString(input) {
			t.Fatal("rejecting fixture did not contain a prose violation")
		}
	}
	outside := "\nThis should still fail; this may also fail.\n"
	masked := maskLicenseNotice(fathomryNotice + outside)
	if !strings.HasSuffix(masked, outside) || len(proseRules["modal"].FindAllString(masked, -1)) != 2 || len(proseRules["semicolon"].FindAllString(masked, -1)) != 1 {
		t.Fatal("license exemption hid adjacent prose violations")
	}
}
