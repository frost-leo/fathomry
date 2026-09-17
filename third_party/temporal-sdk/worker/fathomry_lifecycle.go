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

package worker

import (
	"context"
	"errors"

	"go.temporal.io/sdk/internal"
)

// FathomryWaitStoppedV1 waits for a managed native worker's local cleanup after
// Stop. Enable Options.FathomryLifecycleV1 before construction. A deadline ends
// only this wait; it never confirms termination or releases dependencies.
// This is a local compatibility extension, not an upstream SDK API.
func FathomryWaitStoppedV1(ctx context.Context, worker Worker) error {
	native, ok := worker.(*internal.AggregatedWorker)
	if !ok || native == nil {
		return errors.New("temporal: native managed worker required")
	}
	return native.FathomryWaitStoppedV1(ctx)
}
