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

package health

import (
	"example.org/binding-consumer/workflows/example/server_health/v1/nodes/sample"
	"example.org/binding-consumer/workflows/example/server_health/v1/nodes/summary"
	cli "github.com/frost-leo/fathomry/cmd/fathomry/v1"
	"github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
	implementation "github.com/frost-leo/fathomry/framework/workflow/implementation/v1"
)

// Registration assembles one declaration and its explicit callable set.
func Registration() cli.Workflow {
	return cli.Workflow{Declaration: health.Declarations(), Implementations: []implementation.VersionBindings{
		{Version: "1", Bindings: []implementation.Binding{
			sample.CPUBinding(),
			sample.MemoryBinding(),
			summary.SummarizeBinding(),
		}},
	}}
}
