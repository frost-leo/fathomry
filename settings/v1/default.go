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

package settings

import "sync/atomic"

var application atomic.Pointer[cell]

// Configure explicitly installs the populated application's reader once. A zero
// or unpublished reader returns ErrUnconfigured without claiming the default;
// a subsequent populated-reader installation returns ErrConfigured, even for the
// same reader. Reader admission precedes the already-configured check.
// Later publications through the original store are visible to Default.
//
// The process default cannot be reset/replaced. Independent applications, tests,
// tenants and business configuration domains should pass their own Readers. This
// convenience retains data access, not source ownership or resource lifetime.
func Configure(reader Reader) error {
	if _, err := reader.Capture(); err != nil {
		return reject(ErrUnconfigured, "configure")
	}
	if !application.CompareAndSwap(nil, reader.cell) {
		return reject(ErrConfigured, "configure")
	}
	return nil
}

// Configured reports whether an explicit application reader has been installed,
// not application readiness or compatibility with every component's schema.
func Configured() bool { return application.Load() != nil }

// Default captures the currently published application snapshot. It returns
// ErrUnconfigured before Configure; it never reads files, selects a language,
// supplies schema defaults or prevents callers from using independent readers.
func Default() (View, error) {
	reader := Reader{cell: application.Load()}
	view, err := reader.Capture()
	if err != nil {
		return View{}, reject(ErrUnconfigured, "default")
	}
	return view, nil
}
