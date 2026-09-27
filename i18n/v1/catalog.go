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
	"slices"
	"strings"

	"github.com/frost-leo/fathomry/failure/v1"
	"golang.org/x/text/language"
)

// Profile revisions identify the accepted resource grammar and qualified engine.
// Engine upgrades require requalification, not just a module-version edit.
const (
	Schema       = "fathomry.i18n-resource/v1"
	Profile      = "scalar-cardinal/v1"
	Engine       = "golang.org/x/text v0.41.0"
	LanguageData = "CLDR 32"
	PluralData   = "CLDR 32"
)

// Fixed inclusive limits bound additional work per catalog/call, not process RSS.
const (
	MaxSources         = 32
	MaxLocales         = 32
	MaxDocumentBytes   = 65536
	MaxInputBytes      = 524288
	MaxDepth           = 8
	MaxNodes           = 32768
	MaxTokenBytes      = 8192
	MaxKeyBytes        = 256
	MaxMessages        = 512
	MaxEntries         = 1024
	MaxArguments       = 16
	MaxForms           = 6
	MaxPatternBytes    = 8192
	MaxReferences      = 64
	MaxSegments        = 129
	MaxTotalSegments   = 16384
	MaxStringBytes     = 4096
	MaxArgumentBytes   = 16384
	MaxOutputBytes     = 65536
	MaxInspectionBytes = 4194304
)

// Failure conditions identify rejection without retaining source or argument text.
// Errors returned by this package are directly inspectable failure/v1 occurrences.
const (
	ErrCatalog   failure.Condition = "fathomry.i18n.invalid_catalog"
	ErrResource  failure.Condition = "fathomry.i18n.invalid_resource"
	ErrLimit     failure.Condition = "fathomry.i18n.limit"
	ErrLocale    failure.Condition = "fathomry.i18n.invalid_locale"
	ErrMessage   failure.Condition = "fathomry.i18n.missing_message"
	ErrArguments failure.Condition = "fathomry.i18n.invalid_arguments"
)

func reject(condition failure.Condition) error {
	occurrence, err := failure.New(condition)
	if err != nil {
		panic(err)
	}
	return occurrence
}

// Source is caller-loaded JSON with a unique logical Name, not a path or proof of
// publisher identity. Data is borrowed during Prepare only. Use keyed literals.
type Source struct {
	Name string
	Data []byte
}

// Parameter describes an ordinary named argument inherited from English.
type Parameter struct {
	Name    string
	Kind    string
	Meaning string
}

// Form names the supported integer cardinal categories.
type Form string

const (
	Zero  Form = "zero"
	One   Form = "one"
	Two   Form = "two"
	Few   Form = "few"
	Many  Form = "many"
	Other Form = "other"
)

// Variant describes one authored whole-message pattern, not a reachable-rule claim.
type Variant struct {
	Category Form
	Pattern  string
}

// Definition is an owned inspection snapshot. Arguments and Forms are sorted by
// exact name/category bytes. Digests identify supplied content, not authenticity,
// semantic compatibility or historical retention. Use keyed literals.
type Definition struct {
	Schema, Profile, Engine, LanguageData, PluralData     string
	Owner, ID, Locale, Contract, Context                  string
	SourceDigest, SourceName, DocumentDigest, EntryDigest string
	Cardinal                                              bool
	Arguments                                             []Parameter
	Forms                                                 []Variant
}

type entry struct {
	definition Definition
	tag        language.Tag
	programs   map[Form][]segment
}

// Catalog owns immutable validated resources. Copies share immutable storage;
// never overwrite a shared handle. Nil and zero catalogs are invalid, not empty.
// No methods open files, start services or retain invocation language/arguments.
type Catalog struct {
	modules map[string]ModuleInfo
	entries map[string]map[string]*entry
	ordered []*entry
	locales []language.Tag
	matcher language.Matcher
}

// Inspect returns every accepted localized entry, ordered by ID then English
// first and canonical locale byte order. Each result owns its nested slices.
func (catalog *Catalog) Inspect() ([]Definition, error) {
	if !catalog.valid() {
		return nil, reject(ErrCatalog)
	}
	result := make([]Definition, len(catalog.ordered))
	for index, item := range catalog.ordered {
		result[index] = describe(item)
	}
	return result, nil
}

func describe(item *entry) Definition {
	result := item.definition
	result.Arguments = slices.Clone(result.Arguments)
	result.Forms = slices.Clone(result.Forms)
	return result
}

func (catalog *Catalog) valid() bool { return catalog != nil && catalog.matcher != nil }

// LookupResult separates unknown identity from missing exact translation.
// Definition is zero unless TranslationExists; it never contains fallback text.
type LookupResult struct {
	MessageExists     bool
	TranslationExists bool
	Definition        Definition
}

// Lookup canonicalizes an explicit nonempty locale without matching/fallback.
// Unicode request extensions remain part of the exact key. IDs are not normalized;
// bounded unknown spellings report absence, including empty/unqualified IDs.
func (catalog *Catalog) Lookup(id, locale string) (LookupResult, error) {
	if !catalog.valid() {
		return LookupResult{}, reject(ErrCatalog)
	}
	if len(id) > 256 {
		return LookupResult{}, reject(ErrLimit)
	}
	tag, err := parseLocale(locale, false)
	if err != nil {
		return LookupResult{}, err
	}
	translations, exists := catalog.entries[id]
	result := LookupResult{MessageExists: exists}
	if item := translations[tag.String()]; item != nil {
		result.TranslationExists = true
		result.Definition = describe(item)
	}
	return result, nil
}

// Fallback explains whole-message English selection, independently of form fallback.
type Fallback string

const (
	NoFallback         Fallback = ""
	UnsupportedLocale  Fallback = "unsupported-locale"
	MissingTranslation Fallback = "missing-translation"
)

// SelectionInfo separates caller request, native matching and actual provenance.
// Confidence is the pinned native spelling: No, Low, High or Exact.
type SelectionInfo struct {
	Requested, Canonical, Matched, Candidate, Confidence string
	Fallback                                             Fallback
	Resource                                             Definition
}

// Selection is an opaque immutable selected resource. Its zero value is invalid.
// Copies may be rendered concurrently. Metadata cannot change the selected entry.
type Selection struct {
	item                                                 *entry
	requested, canonical, matched, candidate, confidence string
	fallback                                             Fallback
}

// Metadata returns owned selection/definition data; zero selections return ErrCatalog.
func (selection Selection) Metadata() (SelectionInfo, error) {
	if selection.item == nil {
		return SelectionInfo{}, reject(ErrCatalog)
	}
	return SelectionInfo{
		Requested: selection.requested, Canonical: selection.canonical,
		Matched: selection.matched, Candidate: selection.candidate, Confidence: selection.confidence,
		Fallback: selection.fallback, Resource: describe(selection.item),
	}, nil
}

// Resolve selects the whole message against the catalog-wide locale set. Empty
// locale explicitly means English. Invalid syntax and missing IDs are errors;
// unsupported matches and absent auxiliary entries successfully select English.
func (catalog *Catalog) Resolve(id, locale string) (Selection, error) {
	if !catalog.valid() {
		return Selection{}, reject(ErrCatalog)
	}
	if len(id) > 256 {
		return Selection{}, reject(ErrLimit)
	}
	requested := locale
	if locale == "" {
		locale = "en"
	}
	tag, err := parseLocale(locale, false)
	if err != nil {
		return Selection{}, err
	}
	translations := catalog.entries[id]
	if translations == nil {
		return Selection{}, reject(ErrMessage)
	}
	matched, index, confidence := catalog.matcher.Match(tag)
	candidate := catalog.locales[index]
	selection := Selection{
		requested: strings.Clone(requested), canonical: tag.String(),
		matched: matched.String(), candidate: candidate.String(), confidence: confidence.String(),
	}
	base, _, _ := tag.Raw()
	candidateBase, _, _ := candidate.Raw()
	script, _ := tag.Script()
	candidateScript, _ := candidate.Script()
	if confidence < language.High || base != candidateBase || script != candidateScript {
		selection.fallback = UnsupportedLocale
	} else {
		selection.item = translations[candidate.String()]
		if selection.item == nil {
			selection.fallback = MissingTranslation
		}
	}
	if selection.item == nil {
		selection.item = translations["en"]
	}
	return selection, nil
}

func parseLocale(input string, resource bool) (language.Tag, error) {
	if len(input) == 0 || len(input) > 128 {
		return language.Tag{}, reject(ErrLocale)
	}
	for index := range len(input) {
		char := input[index]
		if !(char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '-') {
			return language.Tag{}, reject(ErrLocale)
		}
	}
	tag, err := language.BCP47.Parse(input)
	if err != nil {
		return language.Tag{}, reject(ErrLocale)
	}
	base, _, _ := tag.Raw()
	if base.String() == "und" || len(tag.Variants()) != 0 {
		return language.Tag{}, reject(ErrLocale)
	}
	for _, extension := range tag.Extensions() {
		if resource || extension.String()[0] != 'u' {
			return language.Tag{}, reject(ErrLocale)
		}
	}
	return tag, nil
}
