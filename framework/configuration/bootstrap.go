/*
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

package configuration

import "context"

// LoadVariables prepares declared variables and defaults before acquisition of
// application settings. A nil lookup uses named process variables; a custom
// lookup can provide an isolated dotenv fallback. All schema, copying, privacy,
// cancellation and variable bounds are identical to Load. No file is discovered.
func LoadVariables[T any](ctx context.Context, schema Schema[T], variables []Variable, lookup VariableLookup) (Configuration[T], error) {
	return Load(ctx, schema, Request{
		Provider:       variableProvider{schema.SchemaVersion},
		Variables:      variables,
		LookupVariable: lookup,
	})
}

type variableProvider struct{ version uint32 }

func (provider variableProvider) ReadConfiguration(context.Context) (Input, error) {
	return Input{Provider: "variables", SchemaVersion: provider.version}, nil
}
