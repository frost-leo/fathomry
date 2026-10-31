/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package configuration

import "context"

// Schema declares the application's complete main-configuration value.
// Version is the configuration schema version, not a module or build version.
// Defaults may be incomplete; LoadConfiguration invokes Validate only after the
// selected configuration layers and permitted variables have been merged.
// Validate must be non-nil, pure,
// bounded and non-panicking. Its closure remains borrowed until Session shutdown.
//
// LoadConfiguration detaches supported mutable defaults before acquisition.
// A validator receives a separate value; mutations do not change accepted data.
// Runtime handles and arbitrary serialization callbacks do not belong in Config.
type Schema[T any] struct {
	Version  uint32
	Defaults T
	Validate func(context.Context, T) error
}

// SchemaFactory declares main configuration after bootstrap selection.
// It receives the admitted environment and may capture explicitly declared
// process inputs once, but must not create resource owners. Factory work and
// first configuration acceptance share one initial deadline.
type SchemaFactory[T any] func(context.Context, string) (Schema[T], error)

// Variable supplies one explicitly captured application override, not authority
// to read process environment. Path addresses declared struct fields using slash
// notation. Present=false still admits the declaration but contributes no value.
// JSON=false supplies literal text to string fields; JSON=true supplies strict
// JSON. Empty text and false are values, not absence. Duplicate or overlapping
// paths reject; individual map keys and list indices are not supported.
// A load accepts at most 64 bindings; supplied values and the prepared variable
// document are bounded to one MiB. Ordinary formatting hides captured values.
type Variable struct {
	Path    string
	Value   string
	Present bool
	JSON    bool
}
