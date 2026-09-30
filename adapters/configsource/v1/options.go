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

package configsource

import "context"

const (
	MaxDocumentBytes = 1 << 20
	MaxDepth         = 64
	MaxNodes         = 32768
)

// LayerKind specifies precedence, not position in the caller's slice.
type LayerKind uint8

const (
	Base        LayerKind = 1
	Environment LayerKind = 2
	Local       LayerKind = 3
	Variables   LayerKind = 4
)

// Encoding specifies original syntax, not native query semantics.
type Encoding string

const (
	JSON Encoding = "json"
	YAML Encoding = "yaml"
)

// Layer supplies one original document. Storage is borrowed until Prepare returns.
// Each kind occurs at most once; omission inherits. Empty/present input is invalid,
// not missing or an empty mapping. No source path or credentials enter provenance.
type Layer struct {
	Kind     LayerKind
	Encoding Encoding
	Content  []byte
}

// Schema owns the complete project data shape. Version must be nonzero; it is not
// a module, SDK or source revision. Defaults is borrowed during Prepare. Validate
// receives a separate value; its mutations are discarded. Cancellation is checked
// before and after it returns. Runtime handles and arbitrary codecs are not data.
type Schema[T any] struct {
	Version  uint32
	Defaults T
	Validate func(context.Context, T) error
}
