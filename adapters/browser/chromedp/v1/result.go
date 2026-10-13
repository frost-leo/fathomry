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

package chromedp

import (
	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// Result retains independent operation facts. PayloadCopy deliberately exposes
// sensitive native data. Receipt delivery never repeats the browser operation.
type Result struct {
	private
	native      native.Result
	payload     []byte
	observation browser.Observation
	attribution browser.Attribution
	binding     BindingObservation
	object      ObjectObservation
	trace       TraceObservation
}

// BindingObservation separates handler return, encoded void and page settlement.
type BindingObservation struct {
	Returned, Undefined, ReplyAcknowledged bool
}

func (r Result) Binding() BindingObservation { return r.binding }
func (r Result) Object() ObjectObservation   { return r.object }
func (r Result) Trace() TraceObservation     { return r.trace }

func (r Result) Observation() browser.Observation    { return r.observation }
func (r Result) Attribution() browser.Attribution    { return r.attribution }
func (r Result) PayloadCopy() []byte                 { return append([]byte(nil), r.payload...) }
func (r Result) DataCopy(name string) ([]byte, bool) { return r.native.DataCopy(name) }
func (r Result) Commands() uint64                    { return r.native.Commands() }
func (r Result) PagesClosed() uint64                 { return r.native.PagesClosed() }
func (r Result) PagesDetached() uint64               { return r.native.PagesDetached() }
func attribution(info adapters.Info) browser.Attribution {
	return browser.Attribution{Runtime: info.Runtime, Sequence: info.Sequence, Parent: info.Parent, Source: info.Source}
}
