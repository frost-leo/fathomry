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
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/frost-leo/fathomry/failure/v1"
	"golang.org/x/text/language"
)

type owner struct{ module, component string }
type entry struct {
	definition Definition
	tag        language.Tag
	programs   map[Form][]segment
	owner      *componentState
}
type componentState struct {
	info    ComponentInfo
	tags    []language.Tag
	matcher language.Matcher
}
type catalogState struct {
	entries    map[string]map[string]*entry
	ordered    []*entry
	components []*componentState
	failures   *failure.Catalog
	bindings   map[failure.Code]Binding
}

// Catalog holds immutable validated resources. Copies share state; do not
// overwrite shared handles. It has no process-global registration or retained FS.
type Catalog struct{ state *catalogState }

func (catalog *Catalog) valid() bool { return catalog != nil && catalog.state != nil }

func describe(item *entry) Definition {
	result := item.definition
	result.Arguments = slices.Clone(result.Arguments)
	result.Forms = slices.Clone(result.Forms)
	return result
}

// Inspect returns all resources ordered by ID, baseline first then locale.
func (catalog *Catalog) Inspect() ([]Definition, error) {
	if !catalog.valid() {
		return nil, reject(ErrCatalog)
	}
	result := make([]Definition, 0, len(catalog.state.ordered))
	for _, item := range catalog.state.ordered {
		result = append(result, describe(item))
	}
	return result, nil
}

// LookupResult separates unknown ID from missing exact translation.
type LookupResult struct {
	MessageExists, TranslationExists bool
	Definition                       Definition
}

// Lookup does exact canonical-locale lookup, without fallback or settings.
func (catalog *Catalog) Lookup(id, locale string) (LookupResult, error) {
	if !catalog.valid() {
		return LookupResult{}, reject(ErrCatalog)
	}
	if len(id) > MaxKeyBytes {
		return LookupResult{}, reject(ErrLimit)
	}
	tag, err := parseLocale(locale, false)
	if err != nil {
		return LookupResult{}, err
	}
	translations, exists := catalog.state.entries[id]
	result := LookupResult{MessageExists: exists}
	if item := translations[tag.String()]; item != nil {
		result.TranslationExists = true
		result.Definition = describe(item)
	}
	return result, nil
}

// Components reverses registration into detached owner/message/locale/code lists.
func (catalog *Catalog) Components() ([]ComponentInfo, error) {
	if !catalog.valid() {
		return nil, reject(ErrCatalog)
	}
	result := make([]ComponentInfo, 0, len(catalog.state.components))
	for _, component := range catalog.state.components {
		info := component.info
		info.Locales = slices.Clone(info.Locales)
		info.Messages = slices.Clone(info.Messages)
		info.Codes = slices.Clone(info.Codes)
		result = append(result, info)
	}
	return result, nil
}

// Coverage lists exact missing resources for each owner in an explicit locale.
func (catalog *Catalog) Coverage(locale string) ([]Coverage, error) {
	if !catalog.valid() {
		return nil, reject(ErrCatalog)
	}
	tag, err := parseLocale(locale, true)
	if err != nil {
		return nil, err
	}
	result := make([]Coverage, 0, len(catalog.state.components))
	for _, component := range catalog.state.components {
		item := Coverage{Module: component.info.Module, Component: component.info.Name, Locale: tag.String()}
		for _, id := range component.info.Messages {
			if catalog.state.entries[id][tag.String()] == nil {
				item.Missing = append(item.Missing, id)
			}
		}
		result = append(result, item)
	}
	return result, nil
}

// Fallback describes whole-message baseline selection, not plural-form fallback.
type Fallback string

const (
	NoFallback          Fallback = ""
	UnsupportedLocale   Fallback = "unsupported-locale"
	MissingTranslation  Fallback = "missing-translation"
	PresentationFailure Fallback = "presentation-failure"
)

// SelectionInfo records the actual resource language separately from the request.
type SelectionInfo struct {
	Requested, Canonical, Matched, Locale string
	Fallback                              Fallback
}

// Selection freezes one resource choice; it never retains render arguments.
type Selection struct {
	item *entry
	info SelectionInfo
}

func (selection Selection) Info() (SelectionInfo, error) {
	if selection.item == nil {
		return SelectionInfo{}, reject(ErrCatalog)
	}
	return selection.info, nil
}

// Resolve matches only this component's locales, then falls back to its explicit
// base resource. Matching never turns another owner's language into support.
func (catalog *Catalog) Resolve(id, locale string) (Selection, error) {
	if !catalog.valid() {
		return Selection{}, reject(ErrCatalog)
	}
	if len(id) > MaxKeyBytes {
		return Selection{}, reject(ErrLimit)
	}
	tag, err := parseLocale(locale, false)
	if err != nil {
		return Selection{}, err
	}
	translations := catalog.state.entries[id]
	if len(translations) == 0 {
		return Selection{}, reject(ErrMessage)
	}
	var component *componentState
	for _, item := range translations {
		component = item.owner
		break
	}
	matched, index, confidence := component.matcher.Match(tag)
	candidate := component.tags[index]
	info := SelectionInfo{Requested: strings.Clone(locale), Canonical: tag.String(), Matched: matched.String()}
	base, _, _ := tag.Raw()
	candidateBase, _, _ := candidate.Raw()
	script, _ := tag.Script()
	candidateScript, _ := candidate.Script()
	var selected *entry
	if confidence < language.High || base != candidateBase || script != candidateScript {
		info.Fallback = UnsupportedLocale
	} else {
		selected = translations[candidate.String()]
		if selected == nil {
			info.Fallback = MissingTranslation
		}
	}
	if selected == nil {
		selected = translations[component.info.BaseLocale]
	}
	info.Locale = selected.definition.Locale
	return Selection{item: selected, info: info}, nil
}

func parseLocale(input string, resource bool) (language.Tag, error) {
	if len(input) == 0 || len(input) > 128 {
		return language.Tag{}, reject(ErrLocale)
	}
	for _, char := range input {
		if !(char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '-') {
			return language.Tag{}, reject(ErrLocale)
		}
	}
	tag, err := language.BCP47.Parse(input)
	if err != nil {
		return language.Tag{}, reject(ErrLocale, err)
	}
	base, _, _ := tag.Raw()
	if base.String() == "und" {
		return language.Tag{}, reject(ErrLocale)
	}
	for _, extension := range tag.Extensions() {
		if resource || !strings.HasPrefix(extension.String(), "u-") {
			return language.Tag{}, reject(ErrLocale)
		}
	}
	return tag, nil
}

// Explanation combines stable machine metadata with a static localized meaning.
type Explanation struct {
	Definition failure.Definition
	Message    Rendered
	Selection  SelectionInfo
}

// Explain uses neither application settings nor runtime error projectors. A
// well-formed unknown code returns found=false; bad locales/codes return errors.
func (catalog *Catalog) Explain(code failure.Code, locale string) (Explanation, bool, error) {
	if !catalog.valid() {
		return Explanation{}, false, reject(ErrCatalog)
	}
	if _, err := parseLocale(locale, false); err != nil {
		return Explanation{}, false, err
	}
	definition, found, err := catalog.state.failures.Lookup(code)
	if err != nil || !found {
		return Explanation{}, found, err
	}
	selection, err := catalog.Resolve(string(definition.Identifier), locale)
	if err != nil {
		return Explanation{}, false, err
	}
	rendered, err := selection.Render(nil, nil)
	if err != nil {
		return Explanation{}, false, err
	}
	return Explanation{Definition: definition, Message: rendered, Selection: selection.info}, true, nil
}

func (Catalog) Format(state fmt.State, _ rune)   { _, _ = state.Write([]byte("i18n.Catalog")) }
func (Catalog) LogValue() slog.Value             { return slog.StringValue("i18n.Catalog") }
func (Catalog) MarshalJSON() ([]byte, error)     { return nil, reject(ErrSerialization) }
func (*Catalog) UnmarshalJSON([]byte) error      { return reject(ErrSerialization) }
func (Selection) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("i18n.Selection")) }
func (Selection) LogValue() slog.Value           { return slog.StringValue("i18n.Selection") }
func (Selection) MarshalJSON() ([]byte, error)   { return nil, reject(ErrSerialization) }
func (*Selection) UnmarshalJSON([]byte) error    { return reject(ErrSerialization) }
