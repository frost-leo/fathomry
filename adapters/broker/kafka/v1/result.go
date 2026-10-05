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

package kafka

import (
	"github.com/frost-leo/fathomry/adapters/broker/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/broker/franz/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
)

func sourceInfo(value source.Info) broker.Info {
	return broker.Info{Scope: value.Scope, Provider: value.Configuration.Identity.Provider, Name: value.Configuration.Identity.Name,
		Revision: value.Configuration.Revision, FormatVersion: value.Configuration.Format}
}

// Result contains immutable process-local observations. Getter copies are owned
// by the caller; errors retain native causes for deliberate inspection. Results
// never grant native clients, receipt custody or callback authority.
type Result struct {
	private
	native      native.Result
	source      broker.Info
	attribution broker.Attribution
	attempts    broker.Attempts
	present     bool
}

func project(value invocation.Result[native.Result], attribution adapters.Info) Result {
	return Result{native: value.Outcome.Value, source: sourceInfo(value.Source), attribution: broker.Attribution{Runtime: attribution.Runtime, Operation: attribution.Operation, ID: attribution.ID, Sequence: attribution.Sequence, Parent: attribution.Parent, Depth: attribution.Depth, Source: attribution.Source},
		attempts: broker.Attempts{Observed: value.Attempts.Observed, Exact: value.Attempts.Exact}, present: value.Outcome.Present}
}
func (value Result) HasData() bool                   { return value.present }
func (value Result) Source() broker.Info             { return value.source }
func (value Result) Attribution() broker.Attribution { return value.attribution }
func (value Result) Attempts() broker.Attempts       { return value.attempts }
