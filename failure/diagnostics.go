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
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Diagnostic limits bound owned optional attribute storage, not borrowed causes,
// capability details, formatting destinations or the caller's input allocation.
const (
	MaxAttributes          = 16
	MaxAttributeNameBytes  = 64
	MaxAttributeValueBytes = 256
	MaxDiagnosticBytes     = 2048
)

// Attribute is optional public diagnostic data, NOT a required machine detail.
// Name uses a lowercase ASCII letter followed by lowercase letters, digits or
// underscores. Value is valid UTF-8 without control characters; empty is a present
// empty value, distinct from an absent attribute. Both are caller-approved public
// text, never credentials, payloads or unreviewed native text. Syntax is not secrecy.
type Attribute struct {
	Name  string
	Value string
}

// Diagnostic is a caller-owned snapshot, not a versioned transport schema.
// Attributes are sorted by name. Omitted means the entire requested attribute
// set was refused as malformed, duplicate or oversized; no partial set is emitted.
// Nil and empty attribute sets both mean no optional diagnostics were requested.
// Omitted says nothing about the completeness of capability-owned machine details.
type Diagnostic struct {
	Code       Code
	Attributes []Attribute
	Omitted    bool
}

// Diagnostic returns independent slice storage; mutating it never changes err.
// Its strings contain only cloned, immutable bytes owned by the occurrence.
func (err Error) Diagnostic() Diagnostic {
	result := Diagnostic{Code: err.Code()}
	if err.state != nil {
		result.Attributes = slices.Clone(err.state.attributes)
		result.Omitted = err.state.omitted
	}
	return result
}

func freezeAttributes(attributes []Attribute) ([]Attribute, bool) {
	if len(attributes) > MaxAttributes {
		return nil, false
	}
	bytes := 0
	for index, attribute := range attributes {
		if !attributeName(attribute.Name) || len(attribute.Value) > MaxAttributeValueBytes || !utf8.ValidString(attribute.Value) {
			return nil, false
		}
		for _, char := range attribute.Value {
			if unicode.IsControl(char) {
				return nil, false
			}
		}
		bytes += len(attribute.Name) + len(attribute.Value)
		if bytes > MaxDiagnosticBytes {
			return nil, false
		}
		for _, previous := range attributes[:index] {
			if attribute.Name == previous.Name {
				return nil, false
			}
		}
	}
	if len(attributes) == 0 {
		return nil, true
	}
	frozen := make([]Attribute, len(attributes))
	for index, attribute := range attributes {
		frozen[index] = Attribute{Name: strings.Clone(attribute.Name), Value: strings.Clone(attribute.Value)}
	}
	slices.SortFunc(frozen, func(left, right Attribute) int { return strings.Compare(left.Name, right.Name) })
	return frozen, true
}

func attributeName(name string) bool {
	if len(name) == 0 || len(name) > MaxAttributeNameBytes || name[0] < 'a' || name[0] > 'z' {
		return false
	}
	for index := 1; index < len(name); index++ {
		char := name[index]
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_') {
			return false
		}
	}
	return true
}
