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

package internal

import "errors"

// FathomryNewWorkerWithEagerClientV1 associates only eager dispatch with an owned
// execution client. Polling retains its private transport and client ownership.
func FathomryNewWorkerWithEagerClientV1(polling Client, execution Client, taskQueue string, options WorkerOptions) (*AggregatedWorker, error) {
	poller, pollerOK := polling.(*WorkflowClient)
	caller, callerOK := execution.(*WorkflowClient)
	if !pollerOK || !callerOK || poller == nil || caller == nil || !options.FathomryLifecycleV1 ||
		poller.namespace != caller.namespace || poller.conn == nil || caller.conn == nil || poller.conn.Target() != caller.conn.Target() {
		return nil, errors.New("temporal: eager bridge requires owned native clients with identical namespace and target")
	}
	worker := NewAggregatedWorker(poller, taskQueue, options)
	worker.eagerDispatcher = caller.eagerDispatcher
	return worker, nil
}
