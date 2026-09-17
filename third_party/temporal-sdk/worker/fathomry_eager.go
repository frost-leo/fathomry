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
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/internal"
)

// FathomryNewWithEagerClientV1 is a local ownership extension for a managed Worker
// with a private polling client and same-source execution client. Both clients
// must remain owned through Stop and FathomryWaitStoppedV1. Namespace and transport
// target must match. No native client is exposed to plugin options.
func FathomryNewWithEagerClientV1(polling, execution client.Client, taskQueue string, options Options) (Worker, error) {
	native, err := internal.FathomryNewWorkerWithEagerClientV1(polling, execution, taskQueue, options)
	if native == nil {
		return nil, err
	}
	return native, err
}
