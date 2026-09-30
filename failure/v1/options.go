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
	"strings"
	"unicode"
	"unicode/utf8"
)

// Limits bound common metadata, catalog admission and direct cause slots. They
// do not prescribe component detail types or the memory of borrowed native causes.
const (
	MaxIdentifierBytes  = 192
	MaxMessageBytes     = 512
	MaxDescriptionBytes = 4096
	MaxCauses           = 32
	MaxDefinitions      = 4096
	MaxCatalogBytes     = 4 << 20
)

// Contract identifies component-owned detail behavior independently of package
// or SDK versions. It references the owner's Go data/access contract, not a central
// field schema or mandatory representation. Zero means no declared contract.
// ID belongs to the same module/component as the Definition; Version is positive
// when ID is present. The owner supplies and tests its actual runtime accessor.
type Contract struct {
	ID      Identifier `json:"id,omitempty"`
	Version uint32     `json:"version,omitempty"`
}

// Definition describes an error class, not one occurrence. Module and Component
// are static namespaces; Identifier is exactly Module.Component.reason.
// Message is a required safe single-line developer explanation; Description adds
// optional explanatory prose. Neither may contain native text or per-call values.
// Revision versions the public semantic/detail contract, not the numeric Code.
// Changing a code's meaning is never justified by increasing Revision.
type Definition struct {
	Code        Code       `json:"code"`
	Identifier  Identifier `json:"identifier"`
	Module      string     `json:"module"`
	Component   string     `json:"component"`
	Revision    uint32     `json:"revision"`
	Message     string     `json:"message"`
	Description string     `json:"description,omitempty"`
	Details     Contract   `json:"details"`
}

// Location identifies this operation and an optional non-secret logical instance.
// Empty fields mean unknown/not supplied, never success. No stack inspection,
// timestamps, correlation generation or sensitive selector inference occurs.
type Location struct {
	Operation string `json:"operation,omitempty"`
	Instance  string `json:"instance,omitempty"`
}

// Diagnostic is an owned safe projection. Definition and Location contain only
// copied scalar metadata. CauseCount counts retained direct interfaces, including
// typed nils, not the size of a recursively reachable native error graph.
type Diagnostic struct {
	Definition Definition `json:"definition"`
	Location   Location   `json:"location"`
	CauseCount int        `json:"cause_count"`
}

// Valid checks declaration syntax and first-party allocation ownership, not author
// authentication, cross-catalog extension collisions or runtime detail behavior.
func (definition Definition) Valid() bool { return validateDefinition(definition) == 0 }

func validateDefinition(definition Definition) Code {
	if !definition.Code.Valid() {
		return ErrCode
	}
	if !namespace(definition.Module, 128) || !namespace(definition.Component, 64) ||
		!definition.Identifier.Valid() || definition.Revision == 0 ||
		!ownsFacility(definition) ||
		!safeText(definition.Message, MaxMessageBytes, false) ||
		definition.Description != "" && !safeText(definition.Description, MaxDescriptionBytes, true) {
		return ErrDefinition
	}
	prefix := definition.Module + "." + definition.Component + "."
	reason, found := strings.CutPrefix(string(definition.Identifier), prefix)
	if !found || !label(reason, 64) {
		return ErrDefinition
	}
	if definition.Details.ID == "" {
		if definition.Details.Version != 0 {
			return ErrDefinition
		}
	} else {
		name, found := strings.CutPrefix(string(definition.Details.ID), prefix)
		if !definition.Details.ID.Valid() || !found || !label(name, 64) || definition.Details.Version == 0 {
			return ErrDefinition
		}
	}
	return 0
}

// Valid checks bounded labels, not their truth or suitability for disclosure.
func (location Location) Valid() bool {
	return (location.Operation == "" || namespace(location.Operation, 64)) &&
		(location.Instance == "" || instanceLabel(location.Instance))
}

func instanceLabel(value string) bool {
	if len(value) > 128 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '.' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

func label(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, char := range value[1:] {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

func namespace(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for _, part := range strings.Split(value, ".") {
		if !label(part, maximum) {
			return false
		}
	}
	return true
}

func safeText(value string, maximum int, multiline bool) bool {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) || strings.TrimSpace(value) == "" {
		return false
	}
	for _, char := range value {
		if unicode.IsControl(char) && !(multiline && (char == '\n' || char == '\t')) {
			return false
		}
	}
	return true
}

func copyDefinition(value Definition) Definition {
	value.Identifier = Identifier(strings.Clone(string(value.Identifier)))
	value.Module, value.Component = strings.Clone(value.Module), strings.Clone(value.Component)
	value.Message, value.Description = strings.Clone(value.Message), strings.Clone(value.Description)
	value.Details.ID = Identifier(strings.Clone(string(value.Details.ID)))
	return value
}

func definitionBytes(value Definition) int {
	return 64 + len(value.Identifier) + len(value.Module) + len(value.Component) +
		len(value.Message) + len(value.Description) + len(value.Details.ID)
}
