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

package sample

import (
	"context"

	implementation "github.com/frost-leo/fathomry/framework/workflow/implementation/v1"
)

// Memory is a separate explicit Activity entry.
func Memory(context.Context, Input) (Output, error) { return Output{Value: 21}, nil }

// MemoryBinding explicitly associates the declaration key with this Activity.
func MemoryBinding() implementation.Binding {
	return implementation.Binding{Key: "read_memory", Callable: implementation.Activity(Memory)}
}
