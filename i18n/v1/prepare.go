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
	"io"
	"io/fs"
	"path"
	"reflect"
	"slices"
	"sort"
	"strings"

	"github.com/frost-leo/fathomry/failure/v1"
	"golang.org/x/text/language"
)

type resourceFile struct {
	name string
	data []byte
}
type admission struct{ documents, bytes, nodes, entries, segments, inspection int }

// Prepare atomically gathers explicit component resources and declarations. No
// override order, global registration or locale setting is consulted. Malformed,
// duplicate, stale or over-budget input returns no catalog. Opened files are closed.
func Prepare(components ...Component) (*Catalog, error) {
	if len(components) == 0 {
		return nil, reject(ErrResource)
	}
	if len(components) > MaxComponents {
		return nil, reject(ErrLimit)
	}
	state := &catalogState{entries: map[string]map[string]*entry{}, bindings: map[failure.Code]Binding{}}
	owners := map[owner]*componentState{}
	var declarations []failure.Definition
	var bindings []Binding
	for _, component := range components {
		key := owner{component.Module, component.Name}
		if len(key.module) > 128 || len(key.component) > 64 || !failure.Identifier(key.module+"."+key.component+".resource").Valid() || owners[key] != nil {
			return nil, reject(ErrResource)
		}
		tag, err := parseLocale(component.BaseLocale, true)
		if err != nil {
			return nil, err
		}
		current := &componentState{info: ComponentInfo{Module: strings.Clone(component.Module), Name: strings.Clone(component.Name), BaseLocale: tag.String()}}
		owners[key] = current
		state.components = append(state.components, current)
		if len(component.Definitions) > failure.MaxDefinitions-len(declarations) || len(component.Bindings) > MaxMessages-len(bindings) {
			return nil, reject(ErrLimit)
		}
		for _, definition := range component.Definitions {
			if definition.Module != key.module || definition.Component != key.component {
				return nil, reject(ErrBinding)
			}
			declarations = append(declarations, definition)
			current.info.Codes = append(current.info.Codes, definition.Code)
		}
		for _, binding := range component.Bindings {
			if binding.Project == nil || len(binding.Message) > MaxKeyBytes || !identifier(binding.MessageContract, 64) {
				return nil, reject(ErrBinding)
			}
			belongs := false
			for _, definition := range component.Definitions {
				if definition.Code == binding.Code {
					belongs = true
					break
				}
			}
			if !belongs {
				return nil, reject(ErrBinding)
			}
			bindings = append(bindings, binding)
		}
	}
	definitions, err := failure.Prepare(declarations...)
	if err != nil {
		return nil, err
	}
	state.failures = definitions
	budget := &admission{}
	locales := map[string]bool{}
	for _, component := range components {
		current := owners[owner{component.Module, component.Name}]
		files, err := load(component.Resources, component.Directory, budget)
		if err != nil {
			return nil, err
		}
		seenLocales := map[string]bool{}
		for _, file := range files {
			document, err := decode(file.data, &budget.nodes)
			if err != nil {
				return nil, err
			}
			if err := removeNotice(document); err != nil {
				return nil, err
			}
			if !fields(document, "schema", "profile", "module", "component", "locale", "messages") || document["schema"] != Schema || document["profile"] != Profile ||
				document["module"] != current.info.Module || document["component"] != current.info.Name {
				return nil, reject(ErrResource)
			}
			locale, ok := textField(document, "locale", 128)
			if !ok {
				return nil, reject(ErrLocale)
			}
			tag, err := parseLocale(locale, true)
			if err != nil {
				return nil, err
			}
			fileTag, err := parseLocale(strings.TrimSuffix(file.name, ".json"), true)
			if err != nil {
				return nil, err
			}
			locale = tag.String()
			if fileTag != tag || seenLocales[locale] {
				return nil, reject(ErrResource)
			}
			seenLocales[locale] = true
			if !locales[locale] && len(locales) >= MaxLocales {
				return nil, reject(ErrLimit)
			}
			locales[locale] = true
			messages, ok := document["messages"].([]any)
			if !ok || len(messages) == 0 {
				return nil, reject(ErrResource)
			}
			if len(messages) > MaxEntries-budget.entries {
				return nil, reject(ErrLimit)
			}
			budget.entries += len(messages)
			for _, raw := range messages {
				message, ok := raw.(map[string]any)
				if !ok {
					return nil, reject(ErrResource)
				}
				definition, err := parseDefinition(message, current.info, locale)
				if err != nil {
					return nil, err
				}
				definition.SourceName = strings.Clone(file.name)
				definition.DocumentDigest = digestBytes(file.data)
				translations := state.entries[definition.ID]
				if translations == nil {
					if len(state.entries) >= MaxMessages {
						return nil, reject(ErrLimit)
					}
					translations = map[string]*entry{}
					state.entries[definition.ID] = translations
					current.info.Messages = append(current.info.Messages, definition.ID)
				}
				for _, existing := range translations {
					if existing.owner != current {
						return nil, reject(ErrResource)
					}
					break
				}
				if translations[locale] != nil {
					return nil, reject(ErrResource)
				}
				translations[locale] = &entry{definition: definition, tag: tag, owner: current}
			}
		}
		if !seenLocales[current.info.BaseLocale] {
			return nil, reject(ErrResource)
		}
		current.info.Locales = append(current.info.Locales, current.info.BaseLocale)
		others := make([]string, 0, len(seenLocales)-1)
		for locale := range seenLocales {
			if locale != current.info.BaseLocale {
				others = append(others, locale)
			}
		}
		sort.Strings(others)
		current.info.Locales = append(current.info.Locales, others...)
		for _, locale := range current.info.Locales {
			tag, _ := parseLocale(locale, true)
			current.tags = append(current.tags, tag)
		}
		current.matcher = language.NewMatcher(current.tags, language.PreferSameScript(false))
		sort.Strings(current.info.Messages)
		slices.Sort(current.info.Codes)
	}
	for _, component := range state.components {
		for _, id := range component.info.Messages {
			translations := state.entries[id]
			base := translations[component.info.BaseLocale]
			if base == nil {
				return nil, reject(ErrResource)
			}
			base.definition.SourceDigest = sourceDigest(base.definition)
			for _, locale := range component.info.Locales {
				item := translations[locale]
				if item == nil {
					continue
				}
				definition := &item.definition
				if item != base {
					if definition.SourceDigest != base.definition.SourceDigest {
						return nil, reject(ErrResource)
					}
					definition.Contract, definition.Context = base.definition.Contract, base.definition.Context
					definition.Cardinal, definition.Arguments = base.definition.Cardinal, base.definition.Arguments
				}
				if !definition.Cardinal && (len(definition.Forms) != 1 || definition.Forms[0].Category != Other) {
					return nil, reject(ErrResource)
				}
				item.programs = map[Form][]segment{}
				for _, form := range definition.Forms {
					program, err := compile(form.Pattern, definition.Arguments, definition.Cardinal)
					if err != nil {
						return nil, err
					}
					if len(program) > MaxTotalSegments-budget.segments {
						return nil, reject(ErrLimit)
					}
					budget.segments += len(program)
					item.programs[form.Category] = program
				}
				charge := inspectionCharge(*definition)
				if charge > MaxInspectionBytes-budget.inspection {
					return nil, reject(ErrLimit)
				}
				budget.inspection += charge
				state.ordered = append(state.ordered, item)
			}
		}
	}
	for _, definition := range declarations {
		translations := state.entries[string(definition.Identifier)]
		component := owners[owner{definition.Module, definition.Component}]
		base := translations[component.info.BaseLocale]
		if base == nil || base.owner != component || base.definition.Cardinal || len(base.definition.Arguments) != 0 {
			return nil, reject(ErrBinding)
		}
		text, err := (Selection{item: base}).Render(nil, nil)
		if err != nil || text.Text != definition.Message {
			return nil, reject(ErrBinding, err)
		}
	}
	for _, binding := range bindings {
		definition, found, err := definitions.Lookup(binding.Code)
		if err != nil || !found || definition.Details != binding.Details || state.bindings[binding.Code].Project != nil {
			return nil, reject(ErrBinding, err)
		}
		component := owners[owner{definition.Module, definition.Component}]
		item := state.entries[binding.Message][component.info.BaseLocale]
		if item == nil || item.owner != component || item.definition.Contract != binding.MessageContract {
			return nil, reject(ErrBinding)
		}
		binding.Message = strings.Clone(binding.Message)
		binding.MessageContract = strings.Clone(binding.MessageContract)
		binding.Details.ID = failure.Identifier(strings.Clone(string(binding.Details.ID)))
		state.bindings[binding.Code] = binding
	}
	sort.Slice(state.ordered, func(left, right int) bool {
		first, second := state.ordered[left].definition, state.ordered[right].definition
		if first.ID != second.ID {
			return first.ID < second.ID
		}
		if first.Locale == second.Locale {
			return false
		}
		if first.Locale == first.BaseLocale {
			return true
		}
		if second.Locale == second.BaseLocale {
			return false
		}
		return first.Locale < second.Locale
	})
	sort.Slice(state.components, func(left, right int) bool {
		first, second := state.components[left].info, state.components[right].info
		if first.Module != second.Module {
			return first.Module < second.Module
		}
		return first.Name < second.Name
	})
	return &Catalog{state: state}, nil
}

func nilValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return reflected.IsNil()
	}
	return false
}
func load(filesystem fs.FS, directory string, budget *admission) ([]resourceFile, error) {
	if nilValue(filesystem) || !fs.ValidPath(directory) || len(directory) > MaxKeyBytes {
		return nil, reject(ErrResource)
	}
	opened, err := filesystem.Open(directory)
	if err != nil {
		if !nilValue(opened) {
			return nil, reject(ErrResource, err, opened.Close())
		}
		return nil, reject(ErrResource, err)
	}
	if nilValue(opened) {
		return nil, reject(ErrResource)
	}
	reader, ok := opened.(fs.ReadDirFile)
	if !ok {
		return nil, reject(ErrResource, opened.Close())
	}
	var entries []fs.DirEntry
	var readErr error
	for len(entries) <= MaxSources {
		batch, err := reader.ReadDir(MaxSources + 1 - len(entries))
		entries = append(entries, batch...)
		if err == io.EOF {
			break
		}
		if err != nil {
			readErr = err
			break
		}
		if len(batch) == 0 {
			readErr = io.ErrNoProgress
			break
		}
	}
	closeErr := opened.Close()
	if readErr != nil || closeErr != nil {
		return nil, reject(ErrResource, readErr, closeErr)
	}
	if len(entries) > MaxSources {
		return nil, reject(ErrLimit)
	}
	for _, entry := range entries {
		if nilValue(entry) {
			return nil, reject(ErrResource)
		}
	}
	sort.Slice(entries, func(left, right int) bool { return entries[left].Name() < entries[right].Name() })
	var result []resourceFile
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		if entry.IsDir() || !fs.ValidPath(name) || strings.Contains(name, "/") || len(name) > 132 {
			return nil, reject(ErrResource)
		}
		if budget.documents >= MaxSources {
			return nil, reject(ErrLimit)
		}
		budget.documents++
		file, err := filesystem.Open(path.Join(directory, name))
		if err != nil {
			if !nilValue(file) {
				return nil, reject(ErrResource, err, file.Close())
			}
			return nil, reject(ErrResource, err)
		}
		if nilValue(file) {
			return nil, reject(ErrResource)
		}
		content, readErr := io.ReadAll(io.LimitReader(file, MaxDocumentBytes+1))
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			return nil, reject(ErrResource, readErr, closeErr)
		}
		if len(content) > MaxDocumentBytes || len(content) > MaxInputBytes-budget.bytes {
			return nil, reject(ErrLimit)
		}
		budget.bytes += len(content)
		result = append(result, resourceFile{name: name, data: content})
	}
	return result, nil
}
func removeNotice(document map[string]any) error {
	notice, exists := document["license_notice"]
	if !exists {
		return nil
	}
	lines, ok := notice.([]any)
	if !ok || len(lines) > 32 {
		return reject(ErrResource)
	}
	for _, line := range lines {
		text, ok := line.(string)
		if !ok || len(text) > 256 {
			return reject(ErrResource)
		}
	}
	delete(document, "license_notice")
	return nil
}
func parseDefinition(message map[string]any, component ComponentInfo, locale string) (Definition, error) {
	result := Definition{Module: component.Module, Component: component.Name, Locale: locale, BaseLocale: component.BaseLocale}
	id, ok := textField(message, "id", MaxKeyBytes)
	if !ok || !strings.HasPrefix(id, component.Module+"."+component.Name+".") || !failure.Identifier(id).Valid() {
		return Definition{}, reject(ErrResource)
	}
	result.ID = id
	if locale == component.BaseLocale {
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
func sourceDigest(definition Definition) string {
	args := make([][3]string, 0, len(definition.Arguments))
	forms := make([][2]string, 0, len(definition.Forms))
	for _, arg := range definition.Arguments {
		args = append(args, [3]string{arg.Name, arg.Kind, arg.Meaning})
	}
	for _, form := range definition.Forms {
		forms = append(forms, [2]string{string(form.Category), form.Pattern})
	}
	data, _ := json.Marshal([]any{Schema, Profile, definition.Module, definition.Component, definition.ID, definition.BaseLocale, definition.Contract, definition.Context, definition.Cardinal, args, forms})
	return digestBytes(data)
}
func inspectionCharge(definition Definition) int {
	charge := 1024
	for _, value := range []string{definition.Module, definition.Component, definition.ID, definition.Locale, definition.BaseLocale, definition.Contract, definition.Context, definition.SourceDigest, definition.DocumentDigest, definition.SourceName} {
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
