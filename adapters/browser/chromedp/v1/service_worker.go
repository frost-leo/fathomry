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
	"context"

	"github.com/chromedp/cdproto/cdp"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
)

// ServiceWorker is an observed native registration or version, not its owner.
// It keeps its original context/source and Control lifetime. DataCopy exposes
// sensitive native metadata without granting authority from mutable IDs.
type ServiceWorker struct {
	private
	native  native.ServiceWorker
	control *Control
}

func (worker ServiceWorker) DataCopy() []byte { return worker.native.DataCopy() }

// ServiceWorker selects an entry from the original registration/version event.
// ControlOptions.ServiceWorkers must be enabled before observation. Deleted,
// redundant and unknown registrations/versions cannot create an operational grant.
func (pending Pending) ServiceWorker(index int) (ServiceWorker, error) {
	if pending.control == nil {
		return ServiceWorker{}, fail(ErrInput, "service-worker-event")
	}
	value, err := pending.native.ServiceWorker(index)
	return ServiceWorker{native: value, control: pending.control}, translate(err, "service-worker-event")
}

// RunServiceWorkerCommand delegates native StopWorker, DeliverPushMessage,
// DispatchSyncEvent and DispatchPeriodicSyncEvent from observed identities.
// Parameters are copied and conflicting registration/version/origin values are
// rejected. A native acknowledgement is not proof that a worker handler ran.
func RunServiceWorkerCommand[P, R any](ctx context.Context, worker ServiceWorker, command cdp.Command[P, R], params P) (result R, receipt *adapters.Receipt[Result], err error) {
	if worker.control == nil || worker.control.state == nil {
		return result, nil, fail(ErrInput, "service-worker")
	}
	return runPageOperation(ctx, worker.control.state.page, "service_worker_command", func(work context.Context) (R, error) {
		return native.RunServiceWorkerCommand(work, worker.native, command, params)
	})
}

// RunServiceWorkerContextCommand delegates native StartWorker, SkipWaiting,
// UpdateRegistration, Unregister, StopAllWorkers and SetForceUpdateOnPageLoad.
// ControlOptions.ServiceWorkers explicitly grants its BrowserContext's native
// storage-partition controls. Borrowed contexts require SharedStorage. ScopeURL
// has native first-party selection semantics, not an observed registration-ID
// promise or an arbitrary partitioned-storage selector. The setting for forced
// update affects the partition; native Disable clears it, not a saved peer value.
// Close disables observation without automatically unregistering or stopping
// workers. Native acknowledgement and completed asynchronous effects are distinct.
func RunServiceWorkerContextCommand[P, R any](ctx context.Context, control *Control, command cdp.Command[P, R], params P) (result R, receipt *adapters.Receipt[Result], err error) {
	if control == nil || control.state == nil {
		return result, nil, fail(ErrInput, "service-worker-control")
	}
	return runPageOperation(ctx, control.state.page, "service_worker_context_command", func(work context.Context) (R, error) {
		return native.RunServiceWorkerContextCommand(work, control.state.native, command, params)
	})
}
