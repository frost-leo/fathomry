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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"

	"golang.org/x/text/language"
)

// Prepare validates all sources before publishing a catalog. English is required
// for every ID. Duplicates (even identical), malformed or stale translations, and
// excess aggregate work reject the entire set. Input order never supplies override
// precedence. Source bytes/names are not borrowed after return.
func Prepare(sources ...Source) (*Catalog, error) {
	if len(sources) == 0 {
		return nil, reject(ErrResource)
	}
	if len(sources) > MaxSources {
		return nil, reject(ErrLimit)
	}
	total := 0
	for _, source := range sources {
		if len(source.Data) > MaxDocumentBytes || len(source.Data) > MaxInputBytes-total {
			return nil, reject(ErrLimit)
		}
		total += len(source.Data)
	}
	catalog := &Catalog{entries: map[string]map[string]*entry{}}
	names, locales := map[string]bool{}, map[string]language.Tag{}
	nodes, entries := 0, 0
	for _, source := range sources {
		if !identifier(source.Name, 128) || names[source.Name] {
			return nil, reject(ErrResource)
		}
		names[source.Name] = true
		document, err := decode(source.Data, &nodes)
		if err != nil {
			return nil, err
		}
		if notice, exists := document["license_notice"]; exists {
			lines, ok := notice.([]any)
			if !ok || len(lines) > 32 {
				return nil, reject(ErrResource)
			}
			for _, line := range lines {
				value, ok := line.(string)
				if !ok || len(value) > 256 {
					return nil, reject(ErrResource)
				}
			}
			delete(document, "license_notice")
		}
		if !fields(document, "schema", "profile", "owner", "locale", "messages") ||
			document["schema"] != Schema || document["profile"] != Profile {
			return nil, reject(ErrResource)
		}
		owner, ok := textField(document, "owner", 128)
		if !ok || !identifier(owner, 128) {
			return nil, reject(ErrResource)
		}
		locale, ok := textField(document, "locale", 128)
		if !ok {
			return nil, reject(ErrResource)
		}
		tag, err := parseLocale(locale, true)
		if err != nil {
			return nil, err
		}
		locale = tag.String()
		if _, exists := locales[locale]; !exists && len(locales) >= MaxLocales {
			return nil, reject(ErrLimit)
		}
		locales[locale] = tag
		messages, ok := document["messages"].([]any)
		if !ok || len(messages) == 0 {
			return nil, reject(ErrResource)
		}
		if len(messages) > MaxEntries-entries {
			return nil, reject(ErrLimit)
		}
		entries += len(messages)
		documentDigest := digestBytes(source.Data)
		for _, raw := range messages {
			message, ok := raw.(map[string]any)
			if !ok {
				return nil, reject(ErrResource)
			}
			definition, err := parseDefinition(message, owner, locale)
			if err != nil {
				return nil, err
			}
			definition.SourceName = strings.Clone(source.Name)
			definition.DocumentDigest = documentDigest
			translations := catalog.entries[definition.ID]
			if translations == nil {
				if len(catalog.entries) >= MaxMessages {
					return nil, reject(ErrLimit)
				}
				translations = map[string]*entry{}
				catalog.entries[definition.ID] = translations
			}
			if translations[locale] != nil {
				return nil, reject(ErrResource)
			}
			translations[locale] = &entry{definition: definition, tag: tag}
		}
	}
	ids := make([]string, 0, len(catalog.entries))
	for id := range catalog.entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	segments, inspection := 0, 0
	for _, id := range ids {
		translations := catalog.entries[id]
		english := translations["en"]
		if english == nil {
			return nil, reject(ErrResource)
		}
		english.definition.SourceDigest = sourceDigest(english.definition)
		orderedLocales := make([]string, 0, len(translations))
		for locale := range translations {
			if locale != "en" {
				orderedLocales = append(orderedLocales, locale)
			}
		}
		sort.Strings(orderedLocales)
		orderedLocales = append([]string{"en"}, orderedLocales...)
		for _, locale := range orderedLocales {
			item := translations[locale]
			definition := &item.definition
			if locale != "en" {
				if definition.SourceDigest != english.definition.SourceDigest {
					return nil, reject(ErrResource)
				}
				definition.Contract, definition.Context = english.definition.Contract, english.definition.Context
				definition.Cardinal, definition.Arguments = english.definition.Cardinal, english.definition.Arguments
			}
			if !definition.Cardinal && (len(definition.Forms) != 1 || definition.Forms[0].Category != Other) {
				return nil, reject(ErrResource)
			}
			item.programs = make(map[Form][]segment, len(definition.Forms))
			for _, form := range definition.Forms {
				program, err := compile(form.Pattern, definition.Arguments, definition.Cardinal)
				if err != nil {
					return nil, err
				}
				if len(program) > MaxTotalSegments-segments {
					return nil, reject(ErrLimit)
				}
				segments += len(program)
				item.programs[form.Category] = program
			}
			definition.EntryDigest = entryDigest(*definition)
			charge := inspectionCharge(*definition)
			if charge > MaxInspectionBytes-inspection {
				return nil, reject(ErrLimit)
			}
			inspection += charge
			catalog.ordered = append(catalog.ordered, item)
		}
	}
	localeNames := make([]string, 0, len(locales))
	for locale := range locales {
		if locale != "en" {
			localeNames = append(localeNames, locale)
		}
	}
	sort.Strings(localeNames)
	catalog.locales = append(catalog.locales, locales["en"])
	for _, locale := range localeNames {
		catalog.locales = append(catalog.locales, locales[locale])
	}
	catalog.matcher = language.NewMatcher(catalog.locales, language.PreferSameScript(false))
	return catalog, nil
}

func parseDefinition(message map[string]any, owner, locale string) (Definition, error) {
	result := Definition{Schema: Schema, Profile: Profile, Engine: Engine, LanguageData: LanguageData, PluralData: PluralData, Owner: owner, Locale: locale}
	id, ok := textField(message, "id", 256)
	if !ok || !strings.HasPrefix(id, owner+":") || !identifier(strings.TrimPrefix(id, owner+":"), 127) {
		return Definition{}, reject(ErrResource)
	}
	result.ID = id
	if locale == "en" {
		if !fields(message, "id", "contract", "context", "args", "cardinal", "forms") {
			return Definition{}, reject(ErrResource)
		}
		result.Contract, ok = textField(message, "contract", 64)
		if !ok || !identifier(result.Contract, 64) {
			return Definition{}, reject(ErrResource)
		}
		result.Context, ok = textField(message, "context", 1024)
		if !ok {
			return Definition{}, reject(ErrResource)
		}
		result.Cardinal, ok = message["cardinal"].(bool)
		if !ok {
			return Definition{}, reject(ErrResource)
		}
		args, ok := message["args"].(map[string]any)
		if !ok {
			return Definition{}, reject(ErrResource)
		}
		if len(args) > MaxArguments {
			return Definition{}, reject(ErrLimit)
		}
		result.Arguments = make([]Parameter, 0, len(args))
		for name, raw := range args {
			parameter, ok := raw.(map[string]any)
			if !ok || !identifier(name, 64) || name == "count" || !fields(parameter, "kind", "meaning") {
				return Definition{}, reject(ErrResource)
			}
			kind, ok := textField(parameter, "kind", 16)
			if !ok || !(kind == "string" || kind == "int64" || kind == "uint64" || kind == "bool") {
				return Definition{}, reject(ErrResource)
			}
			meaning, ok := textField(parameter, "meaning", 256)
			if !ok {
				return Definition{}, reject(ErrResource)
			}
			result.Arguments = append(result.Arguments, Parameter{Name: name, Kind: kind, Meaning: meaning})
		}
		sort.Slice(result.Arguments, func(left, right int) bool { return result.Arguments[left].Name < result.Arguments[right].Name })
	} else {
		if !fields(message, "id", "source", "forms") {
			return Definition{}, reject(ErrResource)
		}
		result.SourceDigest, ok = textField(message, "source", 64)
		if !ok || len(result.SourceDigest) != 64 {
			return Definition{}, reject(ErrResource)
		}
		for _, char := range result.SourceDigest {
			if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
				return Definition{}, reject(ErrResource)
			}
		}
	}
	forms, ok := message["forms"].(map[string]any)
	if !ok || len(forms) == 0 {
		return Definition{}, reject(ErrResource)
	}
	if len(forms) > MaxForms {
		return Definition{}, reject(ErrLimit)
	}
	if _, exists := forms["other"]; !exists {
		return Definition{}, reject(ErrResource)
	}
	result.Forms = make([]Variant, 0, len(forms))
	for category := range forms {
		switch Form(category) {
		case Zero, One, Two, Few, Many, Other:
		default:
			return Definition{}, reject(ErrResource)
		}
		pattern, ok := textField(forms, category, MaxPatternBytes)
		if !ok {
			return Definition{}, reject(ErrResource)
		}
		result.Forms = append(result.Forms, Variant{Category: Form(category), Pattern: pattern})
	}
	sort.Slice(result.Forms, func(left, right int) bool { return result.Forms[left].Category < result.Forms[right].Category })
	return result, nil
}

func digestBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func digestJSON(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	} // Only validated package-owned scalar arrays/maps.
	return digestBytes(data)
}

func sourceDigest(definition Definition) string {
	args := make([][3]string, 0, len(definition.Arguments))
	for _, arg := range definition.Arguments {
		args = append(args, [3]string{arg.Name, arg.Kind, arg.Meaning})
	}
	forms := make([][2]string, 0, len(definition.Forms))
	for _, form := range definition.Forms {
		forms = append(forms, [2]string{string(form.Category), form.Pattern})
	}
	return digestJSON([]any{Schema, Profile, definition.Owner, definition.ID, definition.Contract, definition.Context, definition.Cardinal, args, forms})
}

func entryDigest(definition Definition) string {
	forms := make(map[string]string, len(definition.Forms))
	for _, form := range definition.Forms {
		forms[string(form.Category)] = form.Pattern
	}
	return digestJSON([]any{Schema, Profile, definition.Owner, definition.ID, definition.Locale, definition.SourceDigest, forms})
}

func inspectionCharge(definition Definition) int {
	charge := 1024
	for _, value := range []string{definition.Owner, definition.ID, definition.Locale, definition.Contract, definition.Context, definition.SourceDigest, definition.SourceName, definition.DocumentDigest, definition.EntryDigest} {
		charge += 6 * len(value)
	}
	for _, arg := range definition.Arguments {
		charge += 128 + 6*(len(arg.Name)+len(arg.Kind)+len(arg.Meaning))
	}
	for _, form := range definition.Forms {
		charge += 64 + 6*(len(form.Category)+len(form.Pattern))
	}
	return charge
}
