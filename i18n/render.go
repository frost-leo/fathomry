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
	"strconv"
	"strings"
	"unicode/utf8"

	native "github.com/nicksnyder/go-i18n/v2/i18n"
	"golang.org/x/text/language"
)

// Number is an exact, nonnegative decimal for cardinal quantities. Render
// validates 1-9 integer digits and optionally 1-6 fractional digits, without
// signs, exponent, leading integer zeros or whitespace. Visible fractional zeros
// matter: 1 and 1.0 can select different forms. The zero value is invalid.
// The same spelling drives plural selection and interpolation; no rounding,
// grouping, currency or business-value conversion is performed.
type Number string

// Arguments accepts only exact string, int64, bool and Number values, as declared
// by the source. Other defined types, floats, nil, objects and callbacks are rejected
// without invoking their methods. Every declared parameter is required; extras
// are rejected even when a selected translation omits a parameter.
// The map is borrowed during Render only; callers must not mutate it concurrently.
type Arguments map[string]any

// Fallback explains why English replaced a requested translation. Related-locale
// matching alone is not degradation; compare RequestedLocale and MatchedLocale.
type Fallback string

const (
	NoFallback         Fallback = ""
	UnsupportedLocale  Fallback = "unsupported_locale"
	MissingTranslation Fallback = "missing_translation"
	StaleTranslation   Fallback = "stale_translation"
	TranslationFailure Fallback = "translation_failure"
)

// Result contains plain text and the actual resource language. Snapshot is the
// resource snapshot ID, not a build/workflow version. Successful English fallback
// returns nil error and a nonzero Fallback. Errors return an entirely zero Result,
// never partial text; presenters may retain their original outcome and show an ID.
type Result struct {
	Text            string
	RequestedLocale string
	MatchedLocale   string
	ResourceLocale  string
	Fallback        Fallback
	Snapshot        string
}

// Render uses one explicit language-script-region tag; empty selects English.
// Malformed or unsupported tag features fail. Valid but unmatched/low-confidence
// locales fall back to English. Canonical exact matches win, then x/text matches
// with at least High confidence; ties use en first and other locales sorted.
// A missing/stale/broken translation falls directly to English, not another
// preference or regional catalog. Output is not channel-escaped.
func (catalog *Catalog) Render(locale, id string, arguments Arguments) (Result, error) {
	if catalog == nil || catalog.matcher == nil {
		return Result{}, problem(InvalidCatalog)
	}
	if !validID(id) {
		return Result{}, problem(MessageNotFound)
	}
	if locale == "" {
		locale = "en"
	}
	tag, err := parseLocale(locale)
	if err != nil {
		return Result{}, err
	}
	source := catalog.sources[id]
	if source == nil {
		return Result{}, problem(MessageNotFound)
	}
	data, err := prepareArguments(source.Parameters, arguments)
	if err != nil {
		return Result{}, err
	}
	requested := tag.String()
	matched := requested
	reason := NoFallback
	if catalog.localizers[matched] == nil {
		_, index, confidence := catalog.matcher.Match(tag)
		matched = catalog.locales[index]
		if confidence < language.High {
			matched = "en"
			reason = UnsupportedLocale
		}
	}
	actual := matched
	if !catalog.entries[matched][id] {
		actual = "en"
		reason = MissingTranslation
		if catalog.stale[matched][id] {
			reason = StaleTranslation
		}
	}
	config := &native.LocalizeConfig{
		MessageID: id, TemplateData: data, TemplateParser: catalog.parser,
	}
	if source.Count != "" {
		config.PluralCount = data[source.Count]
	}
	text, err := catalog.localizers[actual].Localize(config)
	if errors.Is(err, LimitExceeded) {
		return Result{}, problem(LimitExceeded)
	}
	if err != nil && actual != "en" {
		actual, reason = "en", TranslationFailure
		text, err = catalog.localizers[actual].Localize(config)
	}
	if err != nil {
		if errors.Is(err, LimitExceeded) {
			return Result{}, problem(LimitExceeded)
		}
		return Result{}, problem(RenderFailed)
	}
	return Result{text, requested, matched, actual, reason, catalog.snapshot.ID}, nil
}

func prepareArguments(parameters map[string]parameter, arguments Arguments) (map[string]string, error) {
	if len(arguments) != len(parameters) {
		return nil, problem(InvalidArguments)
	}
	result := make(map[string]string, len(parameters))
	for name, parameter := range parameters {
		value, exists := arguments[name]
		if !exists {
			return nil, problem(InvalidArguments)
		}
		var text string
		switch parameter.Type {
		case "string":
			typed, ok := value.(string)
			if ok && len(typed) > MaxArgumentBytes {
				return nil, problem(LimitExceeded)
			}
			if !ok || !utf8.ValidString(typed) {
				return nil, problem(InvalidArguments)
			}
			text = typed
		case "integer":
			typed, ok := value.(int64)
			if !ok {
				return nil, problem(InvalidArguments)
			}
			text = strconv.FormatInt(typed, 10)
		case "boolean":
			typed, ok := value.(bool)
			if !ok {
				return nil, problem(InvalidArguments)
			}
			text = strconv.FormatBool(typed)
		case "number":
			typed, ok := value.(Number)
			if !ok || !validNumber(string(typed)) {
				return nil, problem(InvalidArguments)
			}
			text = string(typed)
		default:
			return nil, problem(InvalidArguments)
		}
		if len(text) > MaxArgumentBytes {
			return nil, problem(LimitExceeded)
		}
		result[name] = text
	}
	return result, nil
}

func validNumber(value string) bool {
	if len(value) == 0 || len(value) > 16 {
		return false
	}
	integer, fraction, decimal := strings.Cut(value, ".")
	if len(integer) == 0 || len(integer) > 9 || len(integer) > 1 && integer[0] == '0' ||
		decimal && (len(fraction) == 0 || len(fraction) > 6) {
		return false
	}
	for _, digits := range []string{integer, fraction} {
		for _, digit := range digits {
			if digit < '0' || digit > '9' {
				return false
			}
		}
	}
	return true
}
