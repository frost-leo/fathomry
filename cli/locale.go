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

import "strings"

type languageValue struct{ words *text }

func (value *languageValue) String() string { return value.words.language }
func (value *languageValue) Type() string   { return "string" }
func (value *languageValue) Set(input string) error {
	switch {
	case strings.EqualFold(input, "en"):
		value.words.language = "en"
	case strings.EqualFold(input, "zh-CN"):
		value.words.language = "zh-CN"
	default:
		return errLanguage
	}
	return nil
}
