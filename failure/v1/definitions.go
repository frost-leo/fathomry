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

package failure

import (
	"cmp"
	"errors"
	"slices"
	"strings"
)

// Definition admission limits bound traversal, retained declarations and complete
// snapshots. They do not bound caller-owned input or process memory.
const (
	MaxModules                   = 64
	MaxModuleDepth               = 8
	MaxDefinitions               = 512
	MaxFactContracts             = 256
	MaxFactFields                = 16
	MaxEnumValues                = 16
	MaxDefinitionBytes           = 524288
	MaxDefinitionInspectionBytes = 4194304
	MaxDefinitionIDBytes         = 128
)

var (
	// ErrDefinitions rejects malformed, conflicting or dangling declarations.
	ErrDefinitions = errors.New("failure: invalid definitions")
	// ErrDefinitionLimit rejects admission/query bounds before excessive allocation.
	ErrDefinitionLimit = errors.New("failure: definition limit exceeded")
	// ErrDefinitionCatalog rejects a nil or zero definition catalog.
	ErrDefinitionCatalog = errors.New("failure: invalid definition catalog")
)

// FactKind describes an approved scalar projection, not a Go reflection schema.
type FactKind string

const (
	StringFact FactKind = "string"
	Int64Fact  FactKind = "int64"
	Uint64Fact FactKind = "uint64"
	BoolFact   FactKind = "bool"
	EnumFact   FactKind = "enum"
)

// FactUse separates occurrence facts from operation-owned presentation inputs.
type FactUse string

const (
	ErrorFacts        FactUse = "error"
	PresentationInput FactUse = "presentation"
)

// FactAccess states whether an owner separately supports public runtime access.
// PublicFacts does not itself create an accessor; OwnerFacts remain owner-only.
type FactAccess string

const (
	OwnerFacts  FactAccess = "owner"
	PublicFacts FactAccess = "public"
)

// FactField describes one machine field. Required and UnknownAllowed are
// independent semantic properties. Unknown is not automatically zero/false/empty.
// Owners specify its representation; binding arguments require known values.
// Values is nonempty only for EnumFact. Use keyed literals.
type FactField struct {
	Name           string
	Kind           FactKind
	Unit           string
	Required       bool
	UnknownAllowed bool
	Values         []string
}

// FactContract identifies an owner-qualified scalar projection contract.
// ID has the form module:name; Revision is a separate semantic revision.
// It does not encode runtime values or validate arbitrary Go objects.
type FactContract struct {
	ID       string
	Revision string
	Use      FactUse
	Access   FactAccess
	Fields   []FactField
}

// ConditionDefinition declares meaning and, optionally, one ErrorFacts contract.
// Contract is the semantic revision; Facts references an exact FactContract ID.
// Empty Facts means no declared additional facts, not absence of native causes.
type ConditionDefinition struct {
	Condition Condition
	Contract  string
	Facts     string
}

// ModuleDefinition explicitly supplies one namespace and its immediate children.
// ID uses Condition's owner grammar, with at most 126 bytes. Source is a bounded
// logical label, not a filesystem location or publisher authentication.
// Inputs are borrowed during PrepareDefinitions and must not change concurrently.
type ModuleDefinition struct {
	ID         string
	Source     string
	Conditions []ConditionDefinition
	Contracts  []FactContract
	Children   []ModuleDefinition
}

// ModuleInfo is a flat, owned module snapshot. Empty Parent identifies an explicit
// root. Children, Conditions and Contracts are exact, sorted direct members.
type ModuleInfo struct {
	ID         string
	Parent     string
	Source     string
	Children   []string
	Conditions []Condition
	Contracts  []string
}

// DefinitionInfo associates a condition declaration with its actual module source.
type DefinitionInfo struct {
	Module string
	Source string
	ConditionDefinition
}

// ContractInfo associates a fact contract with its actual module source.
type ContractInfo struct {
	Module string
	Source string
	FactContract
}

// DefinitionCatalog owns immutable declarations. Copies share immutable state;
// never overwrite a shared handle. Nil/zero is invalid; a prepared empty catalog
// is valid. Runtime New/Inspect/Is never consult this catalog.
type DefinitionCatalog struct {
	modules     map[string]ModuleInfo
	definitions map[Condition]DefinitionInfo
	contracts   map[string]ContractInfo
}

// PrepareDefinitions validates the complete explicit forest before cloning or
// publishing anything. Duplicates reject even when equal; no merge or overrides.
// Counts/depth/identity are checked before descending potentially cyclic slices.
// No errors, callbacks, runtime values or unvalidated source text are retained.
func PrepareDefinitions(roots ...ModuleDefinition) (*DefinitionCatalog, error) {
	if len(roots) > MaxModules {
		return nil, ErrDefinitionLimit
	}
	candidate := &DefinitionCatalog{
		modules:     make(map[string]ModuleInfo),
		definitions: make(map[Condition]DefinitionInfo),
		contracts:   make(map[string]ContractInfo),
	}
	bytes, inspection := 0, 0
	reserve := func(size int) error {
		if size > MaxDefinitionInspectionBytes-inspection {
			return ErrDefinitionLimit
		}
		inspection += size
		return nil
	}
	charge := func(value string) error {
		if len(value) > MaxDefinitionBytes-bytes {
			return ErrDefinitionLimit
		}
		if err := reserve(6 * len(value)); err != nil {
			return err
		}
		bytes += len(value)
		return nil
	}
	var visit func(ModuleDefinition, string, int) error
	visit = func(module ModuleDefinition, parent string, depth int) error {
		if depth > MaxModuleDepth || len(candidate.modules) >= MaxModules ||
			len(module.Children) > MaxModules-len(candidate.modules) ||
			len(module.Conditions) > MaxDefinitions-len(candidate.definitions) ||
			len(module.Contracts) > MaxFactContracts-len(candidate.contracts) {
			return ErrDefinitionLimit
		}
		if len(module.ID) > MaxDefinitionIDBytes-2 || len(module.Source) > MaxDefinitionIDBytes {
			return ErrDefinitionLimit
		}
		if !Condition(module.ID+".x").Valid() || !definitionToken(module.Source, MaxDefinitionIDBytes) {
			return ErrDefinitions
		}
		if parent != "" && owner(module.ID) != parent {
			return ErrDefinitions
		}
		if _, duplicate := candidate.modules[module.ID]; duplicate {
			return ErrDefinitions
		}
		if err := reserve(256); err != nil {
			return err
		}
		if err := charge(module.ID); err != nil {
			return err
		}
		if err := charge(parent); err != nil {
			return err
		}
		if err := charge(module.Source); err != nil {
			return err
		}
		info := ModuleInfo{ID: module.ID, Parent: parent, Source: module.Source}
		candidate.modules[module.ID] = info
		for _, contract := range module.Contracts {
			if err := reserve(256); err != nil {
				return err
			}
			if len(contract.Fields) > MaxFactFields {
				return ErrDefinitionLimit
			}
			if !qualifiedDefinition(contract.ID, module.ID) || !definitionToken(contract.Revision, 64) ||
				(contract.Use != ErrorFacts && contract.Use != PresentationInput) ||
				(contract.Access != OwnerFacts && contract.Access != PublicFacts) {
				return ErrDefinitions
			}
			if _, duplicate := candidate.contracts[contract.ID]; duplicate {
				return ErrDefinitions
			}
			for _, value := range []string{contract.ID, contract.Revision, string(contract.Use), string(contract.Access)} {
				if err := charge(value); err != nil {
					return err
				}
			}
			names := make(map[string]bool)
			for _, field := range contract.Fields {
				if err := reserve(192); err != nil {
					return err
				}
				if len(field.Values) > MaxEnumValues {
					return ErrDefinitionLimit
				}
				if !definitionToken(field.Name, 64) || (field.Unit != "" && !definitionToken(field.Unit, 32)) || names[field.Name] {
					return ErrDefinitions
				}
				names[field.Name] = true
				switch field.Kind {
				case StringFact, Int64Fact, Uint64Fact, BoolFact:
					if len(field.Values) != 0 {
						return ErrDefinitions
					}
				case EnumFact:
					if len(field.Values) == 0 {
						return ErrDefinitions
					}
				default:
					return ErrDefinitions
				}
				for _, value := range []string{field.Name, string(field.Kind), field.Unit} {
					if err := charge(value); err != nil {
						return err
					}
				}
				values := make(map[string]bool)
				for _, value := range field.Values {
					if err := reserve(32); err != nil {
						return err
					}
					if !definitionToken(value, 64) || values[value] {
						return ErrDefinitions
					}
					values[value] = true
					if err := charge(value); err != nil {
						return err
					}
				}
			}
			candidate.contracts[contract.ID] = ContractInfo{Module: module.ID, Source: module.Source, FactContract: contract}
			info.Contracts = append(info.Contracts, contract.ID)
		}
		for _, definition := range module.Conditions {
			if err := reserve(256); err != nil {
				return err
			}
			if !definition.Condition.Valid() || owner(string(definition.Condition)) != module.ID ||
				!definitionToken(definition.Contract, 64) ||
				(definition.Facts != "" && !qualifiedDefinition(definition.Facts, module.ID)) {
				return ErrDefinitions
			}
			if _, duplicate := candidate.definitions[definition.Condition]; duplicate {
				return ErrDefinitions
			}
			for _, value := range []string{string(definition.Condition), definition.Contract, definition.Facts} {
				if err := charge(value); err != nil {
					return err
				}
			}
			candidate.definitions[definition.Condition] = DefinitionInfo{Module: module.ID, Source: module.Source, ConditionDefinition: definition}
			info.Conditions = append(info.Conditions, definition.Condition)
		}
		for _, child := range module.Children {
			if err := visit(child, module.ID, depth+1); err != nil {
				return err
			}
			info.Children = append(info.Children, child.ID)
		}
		candidate.modules[module.ID] = info
		return nil
	}
	for _, root := range roots {
		if err := visit(root, "", 1); err != nil {
			return nil, err
		}
	}
	for _, root := range roots {
		for _, other := range roots {
			if strings.HasPrefix(root.ID, other.ID+".") {
				return nil, ErrDefinitions
			}
		}
	}
	for _, definition := range candidate.definitions {
		if definition.Facts != "" {
			contract, exists := candidate.contracts[definition.Facts]
			if !exists || contract.Use != ErrorFacts {
				return nil, ErrDefinitions
			}
		}
	}
	// Only admitted bounded declarations reach cloning. Strings are detached from
	// caller backing storage as well as all mutable slice layers.
	owned := &DefinitionCatalog{modules: make(map[string]ModuleInfo), definitions: make(map[Condition]DefinitionInfo), contracts: make(map[string]ContractInfo)}
	for _, module := range candidate.modules {
		module.ID, module.Parent, module.Source = strings.Clone(module.ID), strings.Clone(module.Parent), strings.Clone(module.Source)
		module.Children, module.Contracts = cloneStrings(module.Children), cloneStrings(module.Contracts)
		for index := range module.Conditions {
			module.Conditions[index] = Condition(strings.Clone(string(module.Conditions[index])))
		}
		slices.Sort(module.Children)
		slices.Sort(module.Contracts)
		slices.Sort(module.Conditions)
		owned.modules[module.ID] = module
	}
	for _, definition := range candidate.definitions {
		definition.Module, definition.Source = strings.Clone(definition.Module), strings.Clone(definition.Source)
		definition.Condition = Condition(strings.Clone(string(definition.Condition)))
		definition.Contract, definition.Facts = strings.Clone(definition.Contract), strings.Clone(definition.Facts)
		owned.definitions[definition.Condition] = definition
	}
	for _, contract := range candidate.contracts {
		contract = copyContract(contract)
		contract.Module, contract.Source = strings.Clone(contract.Module), strings.Clone(contract.Source)
		contract.ID, contract.Revision = strings.Clone(contract.ID), strings.Clone(contract.Revision)
		contract.Use, contract.Access = FactUse(strings.Clone(string(contract.Use))), FactAccess(strings.Clone(string(contract.Access)))
		for index := range contract.Fields {
			field := &contract.Fields[index]
			field.Name, field.Unit = strings.Clone(field.Name), strings.Clone(field.Unit)
			field.Kind = FactKind(strings.Clone(string(field.Kind)))
			field.Values = cloneStrings(field.Values)
			slices.Sort(field.Values)
		}
		slices.SortFunc(contract.Fields, func(a, b FactField) int { return cmp.Compare(a.Name, b.Name) })
		owned.contracts[contract.ID] = contract
	}
	return owned, nil
}

func owner(value string) string {
	index := strings.LastIndexByte(value, '.')
	if index < 0 {
		return ""
	}
	return value[:index]
}

func definitionToken(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for index := range len(value) {
		char := value[index]
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-' || char == '.' || char == '/') {
			return false
		}
	}
	return true
}

func qualifiedDefinition(value, module string) bool {
	return len(value) <= MaxDefinitionIDBytes && strings.HasPrefix(value, module+":") && definitionToken(strings.TrimPrefix(value, module+":"), 64)
}

func cloneStrings(values []string) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = strings.Clone(value)
	}
	return result
}

func copyModule(module ModuleInfo) ModuleInfo {
	module.Children = slices.Clone(module.Children)
	module.Conditions = slices.Clone(module.Conditions)
	module.Contracts = slices.Clone(module.Contracts)
	return module
}

func copyContract(contract ContractInfo) ContractInfo {
	contract.Fields = slices.Clone(contract.Fields)
	for index := range contract.Fields {
		contract.Fields[index].Values = slices.Clone(contract.Fields[index].Values)
	}
	return contract
}

func (catalog *DefinitionCatalog) valid() bool { return catalog != nil && catalog.modules != nil }

// Modules returns all modules ordered by exact ID, including empty/group modules.
func (catalog *DefinitionCatalog) Modules() ([]ModuleInfo, error) {
	if !catalog.valid() {
		return nil, ErrDefinitionCatalog
	}
	result := make([]ModuleInfo, 0, len(catalog.modules))
	for _, module := range catalog.modules {
		result = append(result, copyModule(module))
	}
	slices.SortFunc(result, func(a, b ModuleInfo) int { return cmp.Compare(a.ID, b.ID) })
	return result, nil
}

// Module performs exact lookup; bounded unknown spellings report absence.
func (catalog *DefinitionCatalog) Module(id string) (ModuleInfo, bool, error) {
	if !catalog.valid() {
		return ModuleInfo{}, false, ErrDefinitionCatalog
	}
	if len(id) > MaxDefinitionIDBytes {
		return ModuleInfo{}, false, ErrDefinitionLimit
	}
	module, exists := catalog.modules[id]
	return copyModule(module), exists, nil
}

// Inspect returns every condition ordered by exact identity.
func (catalog *DefinitionCatalog) Inspect() ([]DefinitionInfo, error) {
	if !catalog.valid() {
		return nil, ErrDefinitionCatalog
	}
	result := make([]DefinitionInfo, 0, len(catalog.definitions))
	for _, definition := range catalog.definitions {
		result = append(result, definition)
	}
	slices.SortFunc(result, func(a, b DefinitionInfo) int { return cmp.Compare(a.Condition, b.Condition) })
	return result, nil
}

// Lookup performs exact condition lookup. Missing metadata does not invalidate
// a runtime occurrence; malformed bounded identities also report absence.
func (catalog *DefinitionCatalog) Lookup(condition Condition) (DefinitionInfo, bool, error) {
	if !catalog.valid() {
		return DefinitionInfo{}, false, ErrDefinitionCatalog
	}
	if len(condition) > MaxConditionBytes {
		return DefinitionInfo{}, false, ErrDefinitionLimit
	}
	definition, exists := catalog.definitions[condition]
	return definition, exists, nil
}

// Definitions returns direct or explicit subtree conditions for one exact module.
// Its bool reports module existence, including a module with no conditions.
func (catalog *DefinitionCatalog) Definitions(module string, descendants bool) ([]DefinitionInfo, bool, error) {
	if _, exists, err := catalog.Module(module); err != nil || !exists {
		return nil, exists, err
	}
	all, _ := catalog.Inspect()
	result := all[:0]
	for _, definition := range all {
		if definition.Module == module || descendants && strings.HasPrefix(definition.Module, module+".") {
			result = append(result, definition)
		}
	}
	return result, true, nil
}

// Contracts returns all declared projections ordered by ID, with owned fields.
func (catalog *DefinitionCatalog) Contracts() ([]ContractInfo, error) {
	if !catalog.valid() {
		return nil, ErrDefinitionCatalog
	}
	result := make([]ContractInfo, 0, len(catalog.contracts))
	for _, contract := range catalog.contracts {
		result = append(result, copyContract(contract))
	}
	slices.SortFunc(result, func(a, b ContractInfo) int { return cmp.Compare(a.ID, b.ID) })
	return result, nil
}

// Contract returns an exact projection declaration, not runtime fact values.
func (catalog *DefinitionCatalog) Contract(id string) (ContractInfo, bool, error) {
	if !catalog.valid() {
		return ContractInfo{}, false, ErrDefinitionCatalog
	}
	if len(id) > MaxDefinitionIDBytes {
		return ContractInfo{}, false, ErrDefinitionLimit
	}
	contract, exists := catalog.contracts[id]
	return copyContract(contract), exists, nil
}
