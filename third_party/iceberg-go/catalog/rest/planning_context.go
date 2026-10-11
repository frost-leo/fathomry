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

package rest

import (
	"context"

	"github.com/apache/iceberg-go/table"
)

// ScanPlanningContext prepares one remote-plan lifetime before request setup and
// I/O. It may reject admission or derive a context carrying source-local custody.
// It must preserve the caller's cancellation, deadline and values. The returned
// context is used for every retry of the initial request; later plan operations
// use their own caller contexts. A nil callback leaves native behavior unchanged.
type ScanPlanningContext func(context.Context, table.Identifier) (context.Context, error)

// WithScanPlanningContext installs a per-Catalog planning admission hook. It
// applies to both low-level PlanTableScan and high-level Scan planning.
func WithScanPlanningContext(prepare ScanPlanningContext) Option {
	return func(options *options) { options.scanPlanningContext = prepare }
}
