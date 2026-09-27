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
)

const (
	// MaxBindings and MaxBindingBytes bound additional declaration storage/work.
	MaxBindings     = 1024
	MaxBindingBytes = 524288
	// MaxBindingInspectionBytes bounds the conservative complete JSON projection.
	MaxBindingInspectionBytes = 4194304
	// ErrBinding reports an invalid handle or incompatible binding declaration.
	ErrBinding failure.Condition = "fathomry.i18n.invalid_binding"
	// ErrBindingMissing distinguishes an unknown binding from an unknown message.
	ErrBindingMissing failure.Condition = "fathomry.i18n.missing_binding"
)

// ArgumentBinding maps one declared message argument to an approved input field.
type ArgumentBinding struct{ Argument, Fact string }

// Binding declares a presenter-owned association, not an error extractor.
// ID is module:name. Condition and ConditionContract are both absent or both set.
// Input and InputRevision identify a declared projection, or are both absent for
// fact-free bindings. ErrorFacts inputs must match the condition's own contract;
// PresentationInput belongs to the binding module and is not an error field.
// Message ownership may differ deliberately for application wording customization.
// Surface/Role are bounded machine identifiers, not predicate or fallback rules.
type Binding struct {
	ID                string
	Condition         failure.Condition
	ConditionContract string
	Input             string
	InputRevision     string
	Surface           string
	Role              string
	Message           string
	MessageContract   string
	Arguments         []ArgumentBinding
	CountFact         string
}

type compiledBinding struct {
	declaration Binding
	fields      []failure.FactField
}

// Bindings owns immutable checked associations and shares the selected resource
// catalog. Nil/zero handles are invalid. No runtime errors, projection callbacks,
// languages or argument values are retained. Copies share immutable storage.
type Bindings struct {
	catalog *Catalog
	entries map[string]*compiledBinding
	ordered []*compiledBinding
}

// PrepareBindings checks references/contracts before publishing anything. Mapped
// fields must be required and known by contract; optional/unknown fields require
// an owner-defined narrower projection. Runtime Render accepts only the mapped
// subset, not an entire error/object. It never extracts or traverses causes.
// Inputs are borrowed during this call and must not change concurrently.
func PrepareBindings(definitions *failure.DefinitionCatalog, catalog *Catalog, declarations ...Binding) (*Bindings, error) {
	if !catalog.valid() {
		return nil, reject(ErrCatalog)
	}
	if len(declarations) > MaxBindings {
		return nil, reject(ErrLimit)
	}
	if _, err := definitions.Modules(); err != nil {
		return nil, definitionError(err, ErrBinding)
	}
	result := &Bindings{catalog: catalog, entries: make(map[string]*compiledBinding)}
	total, inspection := 0, 0
	for _, declaration := range declarations {
		if len(declaration.Arguments) > MaxArguments {
			return nil, reject(ErrLimit)
		}
		cost := 512 + 128*len(declaration.Arguments)
		if cost > MaxBindingInspectionBytes-inspection {
			return nil, reject(ErrLimit)
		}
		inspection += cost
		for _, value := range []string{declaration.ID, string(declaration.Condition), declaration.ConditionContract, declaration.Input, declaration.InputRevision, declaration.Surface, declaration.Role, declaration.Message, declaration.MessageContract, declaration.CountFact} {
			if len(value) > MaxKeyBytes || len(value) > MaxBindingBytes-total {
				return nil, reject(ErrLimit)
			}
			if len(value) > (MaxBindingInspectionBytes-inspection)/6 {
				return nil, reject(ErrLimit)
			}
			inspection += 6 * len(value)
			total += len(value)
		}
		for _, argument := range declaration.Arguments {
			for _, value := range []string{argument.Argument, argument.Fact} {
				if len(value) > 64 || len(value) > MaxBindingBytes-total {
					return nil, reject(ErrLimit)
				}
				if len(value) > (MaxBindingInspectionBytes-inspection)/6 {
					return nil, reject(ErrLimit)
				}
				inspection += 6 * len(value)
				total += len(value)
			}
		}
		module, name, ok := strings.Cut(declaration.ID, ":")
		if !ok || !identifier(name, 127) || !identifier(declaration.Surface, 64) ||
			!identifier(declaration.Role, 64) || declaration.MessageContract == "" {
			return nil, reject(ErrBinding)
		}
		if _, exists, err := definitions.Module(module); err != nil {
			return nil, definitionError(err, ErrBinding)
		} else if !exists {
			return nil, reject(ErrBinding)
		}
		if catalog.modules != nil {
			if _, exists := catalog.modules[module]; !exists {
				return nil, reject(ErrBinding)
			}
		}
		if _, duplicate := result.entries[declaration.ID]; duplicate {
			return nil, reject(ErrBinding)
		}
		var condition failure.DefinitionInfo
		if declaration.Condition != "" {
			var exists bool
			var err error
			condition, exists, err = definitions.Lookup(declaration.Condition)
			if err != nil {
				return nil, definitionError(err, ErrBinding)
			}
			if !exists || declaration.ConditionContract != condition.Contract {
				return nil, reject(ErrBinding)
			}
		} else if declaration.ConditionContract != "" {
			return nil, reject(ErrBinding)
		}
		fields := make(map[string]failure.FactField)
		if declaration.Input != "" {
			input, exists, err := definitions.Contract(declaration.Input)
			if err != nil {
				return nil, definitionError(err, ErrBinding)
			}
			if !exists || declaration.InputRevision != input.Revision ||
				input.Use == failure.ErrorFacts && (declaration.Condition == "" || condition.Facts != input.ID) ||
				input.Use == failure.PresentationInput && input.Module != module {
				return nil, reject(ErrBinding)
			}
			for _, field := range input.Fields {
				fields[field.Name] = field
			}
		} else if declaration.InputRevision != "" {
			return nil, reject(ErrBinding)
		}
		message, err := catalog.Lookup(declaration.Message, "en")
		if err != nil {
			return nil, err
		}
		if !message.TranslationExists || message.Definition.Contract != declaration.MessageContract ||
			len(message.Definition.Arguments) != len(declaration.Arguments) ||
			message.Definition.Cardinal != (declaration.CountFact != "") {
			return nil, reject(ErrBinding)
		}
		arguments := make(map[string]string)
		used := make(map[string]bool)
		for _, argument := range declaration.Arguments {
			if !identifier(argument.Argument, 64) || !identifier(argument.Fact, 64) {
				return nil, reject(ErrBinding)
			}
			if _, duplicate := arguments[argument.Argument]; duplicate {
				return nil, reject(ErrBinding)
			}
			arguments[argument.Argument] = argument.Fact
		}
		for _, argument := range message.Definition.Arguments {
			field, exists := fields[arguments[argument.Name]]
			if !exists || !field.Required || field.UnknownAllowed ||
				string(field.Kind) != argument.Kind && !(field.Kind == failure.EnumFact && argument.Kind == "string") {
				return nil, reject(ErrBinding)
			}
			used[field.Name] = true
		}
		if declaration.CountFact != "" {
			field, exists := fields[declaration.CountFact]
			if !identifier(declaration.CountFact, 64) || !exists || field.Kind != failure.Uint64Fact || !field.Required || field.UnknownAllowed {
				return nil, reject(ErrBinding)
			}
			used[field.Name] = true
		}
		compiled := &compiledBinding{declaration: copyBinding(declaration)}
		for name := range used {
			field := fields[name]
			for _, value := range append([]string{field.Name, string(field.Kind), field.Unit}, field.Values...) {
				if len(value) > MaxBindingBytes-total {
					return nil, reject(ErrLimit)
				}
				total += len(value)
			}
			compiled.fields = append(compiled.fields, field)
		}
		slices.SortFunc(compiled.fields, func(a, b failure.FactField) int { return strings.Compare(a.Name, b.Name) })
		result.entries[compiled.declaration.ID] = compiled
		result.ordered = append(result.ordered, compiled)
	}
	slices.SortFunc(result.ordered, func(a, b *compiledBinding) int { return strings.Compare(a.declaration.ID, b.declaration.ID) })
	return result, nil
}

func copyBinding(binding Binding) Binding {
	binding.ID = strings.Clone(binding.ID)
	binding.Condition = failure.Condition(strings.Clone(string(binding.Condition)))
	binding.ConditionContract = strings.Clone(binding.ConditionContract)
	binding.Input, binding.InputRevision = strings.Clone(binding.Input), strings.Clone(binding.InputRevision)
	binding.Surface, binding.Role = strings.Clone(binding.Surface), strings.Clone(binding.Role)
	binding.Message, binding.MessageContract = strings.Clone(binding.Message), strings.Clone(binding.MessageContract)
	binding.CountFact = strings.Clone(binding.CountFact)
	binding.Arguments = slices.Clone(binding.Arguments)
	for index := range binding.Arguments {
		binding.Arguments[index].Argument = strings.Clone(binding.Arguments[index].Argument)
		binding.Arguments[index].Fact = strings.Clone(binding.Arguments[index].Fact)
	}
	slices.SortFunc(binding.Arguments, func(a, b ArgumentBinding) int { return strings.Compare(a.Argument, b.Argument) })
	return binding
}

func (bindings *Bindings) valid() bool {
	return bindings != nil && bindings.catalog != nil && bindings.entries != nil
}

// Inspect returns every binding, ordered by ID, with owned nested slices.
func (bindings *Bindings) Inspect() ([]Binding, error) {
	if !bindings.valid() {
		return nil, reject(ErrBinding)
	}
	result := make([]Binding, len(bindings.ordered))
	for index, entry := range bindings.ordered {
		result[index] = copyBinding(entry.declaration)
	}
	return result, nil
}

// Lookup performs exact binding lookup without resource or locale fallback.
func (bindings *Bindings) Lookup(id string) (Binding, bool, error) {
	if !bindings.valid() {
		return Binding{}, false, reject(ErrBinding)
	}
	if len(id) > MaxKeyBytes {
		return Binding{}, false, reject(ErrLimit)
	}
	entry, exists := bindings.entries[id]
	if !exists {
		return Binding{}, false, nil
	}
	return copyBinding(entry.declaration), true, nil
}

// BoundSelection pairs one checked association with its selected actual resource.
// It does not contain an error occurrence; its owner must select facts from the
// same occurrence/result as the condition. Zero is invalid; copies are concurrent.
type BoundSelection struct {
	selection Selection
	binding   *compiledBinding
}

// Resolve selects the binding's message using the ordinary resource policy.
func (bindings *Bindings) Resolve(id, locale string) (BoundSelection, error) {
	if !bindings.valid() {
		return BoundSelection{}, reject(ErrBinding)
	}
	if len(id) > MaxKeyBytes {
		return BoundSelection{}, reject(ErrLimit)
	}
	binding, exists := bindings.entries[id]
	if !exists {
		return BoundSelection{}, reject(ErrBindingMissing)
	}
	selection, err := bindings.catalog.Resolve(binding.declaration.Message, locale)
	if err != nil {
		return BoundSelection{}, err
	}
	return BoundSelection{selection: selection, binding: binding}, nil
}

// Metadata retains the ordinary requested/matched/actual-resource guarantees.
func (selection BoundSelection) Metadata() (SelectionInfo, error) {
	if selection.binding == nil {
		return SelectionInfo{}, reject(ErrBinding)
	}
	return selection.selection.Metadata()
}

// Render admits exactly the mapped known scalar fields, then uses the ordinary
// bounded renderer. Count is taken once from the declared uint64 field; there is
// no second count channel. Enum tokens are exact strings. Nil, optional absence,
// unknown values, named scalars and object formatting are not converted to zero.
func (selection BoundSelection) Render(facts []Argument) (Rendered, error) {
	if selection.binding == nil {
		return Rendered{}, reject(ErrBinding)
	}
	if len(facts) > failure.MaxFactFields {
		return Rendered{}, reject(ErrLimit)
	}
	if len(facts) != len(selection.binding.fields) {
		return Rendered{}, reject(ErrArguments)
	}
	values := make(map[string]any, len(facts))
	for _, fact := range facts {
		if !identifier(fact.Name, 64) {
			return Rendered{}, reject(ErrArguments)
		}
		if _, exists := values[fact.Name]; exists {
			return Rendered{}, reject(ErrArguments)
		}
		index := slices.IndexFunc(selection.binding.fields, func(field failure.FactField) bool { return field.Name == fact.Name })
		if index < 0 {
			return Rendered{}, reject(ErrArguments)
		}
		field := selection.binding.fields[index]
		kind := string(field.Kind)
		if field.Kind == failure.EnumFact {
			value, ok := fact.Value.(string)
			if !ok || !slices.Contains(field.Values, value) {
				return Rendered{}, reject(ErrArguments)
			}
			kind = "string"
		}
		_, err := scalar(fact.Value, kind)
		if err != nil {
			return Rendered{}, err
		}
		values[fact.Name] = fact.Value
	}
	declaration := &selection.binding.declaration
	arguments := make([]Argument, 0, len(declaration.Arguments))
	for _, argument := range declaration.Arguments {
		arguments = append(arguments, Argument{Name: argument.Argument, Value: values[argument.Fact]})
	}
	var count *uint64
	if declaration.CountFact != "" {
		number := values[declaration.CountFact].(uint64)
		count = &number
	}
	return selection.selection.Render(arguments, count)
}
