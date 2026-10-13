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
	"net/url"
	"reflect"

	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/serviceworker"
	sdk "github.com/chromedp/chromedp"
)

// ServiceWorker is a native registration or version observed by one Control.
// It does not own the worker's process or persistent registration.
type ServiceWorker struct {
	private
	control      *controlState
	registration *workerRegistration
	version      *workerVersion
	data         []byte
}

type workerRegistration struct {
	id            serviceworker.RegistrationID
	scope, origin string
	deleted       bool
}

type workerVersion struct {
	id           string
	registration serviceworker.RegistrationID
	retired      bool
}

type workerEvent struct{ values []ServiceWorker }

type workerLedger struct {
	registrations map[serviceworker.RegistrationID]*workerRegistration
	versions      map[string]*workerVersion
	events        map[uint64]workerEvent
	count         int
	bytes         int64
}

func newWorkerLedger() *workerLedger {
	return &workerLedger{registrations: make(map[serviceworker.RegistrationID]*workerRegistration), versions: make(map[string]*workerVersion), events: make(map[uint64]workerEvent)}
}

func (state *controlState) observeWorkers(method string, data jsonv2.Value, sequence uint64) error {
	if state.workers == nil {
		return nil
	}
	switch method {
	case serviceworker.WorkerRegistrationUpdated.Method, serviceworker.WorkerVersionUpdated.Method, serviceworker.WorkerErrorReported.Method:
	default:
		return nil
	}
	ledger, settings := state.workers, state.session.owner.settings
	if ledger.count >= settings.MaxEvents || int64(len(data))+512 > settings.MaxResultBytes-ledger.bytes || sequence == 0 {
		return failure(ErrLimit, "service-worker-origins")
	}
	ledger.count++
	ledger.bytes += int64(len(data)) + 512
	var observed workerEvent
	retain := func(registration *workerRegistration, version *workerVersion, metadata any) error {
		if ledger.count >= settings.MaxEvents || settings.MaxResultBytes-ledger.bytes < 512 {
			return failure(ErrLimit, "service-worker-identities")
		}
		buffer := boundedBuffer{limit: settings.MaxResultBytes - ledger.bytes - 512}
		if err := marshalBounded(&buffer, metadata); err != nil {
			return err
		}
		ledger.count++
		ledger.bytes += int64(len(buffer.data)) + 512
		observed.values = append(observed.values, ServiceWorker{control: state, registration: registration, version: version, data: buffer.data})
		return nil
	}
	switch method {
	case serviceworker.WorkerRegistrationUpdated.Method:
		var event serviceworker.EventWorkerRegistrationUpdated
		if err := jsonv2.Unmarshal(data, &event, sdk.DefaultUnmarshalOptions); err != nil {
			return err
		}
		if len(event.Registrations) > settings.MaxEvents-ledger.count {
			return failure(ErrLimit, "service-worker-registrations")
		}
		for _, value := range event.Registrations {
			if value == nil || value.RegistrationID == "" {
				return failure(ErrNative, "service-worker-registration")
			}
			address, err := url.Parse(value.ScopeURL)
			if err != nil || address.Host == "" || address.User != nil || address.Fragment != "" || !address.IsAbs() {
				return failure(ErrNative, "service-worker-scope")
			}
			entry := ledger.registrations[value.RegistrationID]
			if entry != nil && entry.scope != value.ScopeURL {
				return failure(ErrNative, "service-worker-registration-reused")
			}
			if entry == nil || entry.deleted && !value.IsDeleted {
				entry = &workerRegistration{id: value.RegistrationID, scope: value.ScopeURL, origin: address.Scheme + "://" + address.Host}
			}
			entry.deleted = value.IsDeleted
			if err := retain(entry, nil, value); err != nil {
				return err
			}
			ledger.registrations[value.RegistrationID] = entry
		}
	case serviceworker.WorkerVersionUpdated.Method:
		var event serviceworker.EventWorkerVersionUpdated
		if err := jsonv2.Unmarshal(data, &event, sdk.DefaultUnmarshalOptions); err != nil {
			return err
		}
		if len(event.Versions) > settings.MaxEvents-ledger.count {
			return failure(ErrLimit, "service-worker-versions")
		}
		for _, value := range event.Versions {
			if value == nil || value.VersionID == "" || value.RegistrationID == "" {
				return failure(ErrNative, "service-worker-version")
			}
			entry := ledger.versions[value.VersionID]
			if entry != nil && entry.registration != value.RegistrationID {
				return failure(ErrNative, "service-worker-version-reused")
			}
			if entry == nil || entry.retired && value.Status != serviceworker.VersionStatusRedundant {
				entry = &workerVersion{id: value.VersionID, registration: value.RegistrationID}
			}
			entry.retired = value.Status == serviceworker.VersionStatusRedundant
			if err := retain(ledger.registrations[value.RegistrationID], entry, value); err != nil {
				return err
			}
			ledger.versions[value.VersionID] = entry
		}
	case serviceworker.WorkerErrorReported.Method:
		var event serviceworker.EventWorkerErrorReported
		if err := jsonv2.Unmarshal(data, &event, sdk.DefaultUnmarshalOptions); err != nil {
			return err
		}
		if event.ErrorMessage == nil {
			return failure(ErrNative, "service-worker-error-event")
		}
	}
	ledger.events[sequence] = observed
	return nil
}

// ServiceWorker returns the index-th registration or version in its original
// native event. Deleted/redundant, unobserved and foreign identities are refused.
func (pending Pending) ServiceWorker(index int) (ServiceWorker, error) {
	if pending.state == nil || index < 0 || index >= len(pending.state.workers.values) {
		return ServiceWorker{}, failure(ErrInput, "service-worker-event-index")
	}
	worker := pending.state.workers.values[index]
	if err := worker.check(); err != nil {
		return ServiceWorker{}, err
	}
	return worker, nil
}

func (worker ServiceWorker) DataCopy() []byte { return append([]byte(nil), worker.data...) }

func (worker ServiceWorker) check() error {
	if worker.control == nil {
		return failure(ErrInput, "service-worker")
	}
	worker.control.mu.Lock()
	defer worker.control.mu.Unlock()
	return worker.checkLocked()
}

func (worker ServiceWorker) checkLocked() error {
	state := worker.control
	if state == nil || state.workers == nil || worker.registration == nil {
		return failure(ErrInput, "service-worker-origin")
	}
	if state.closing || state.closed || state.life.Err() != nil || worker.registration.deleted || state.workers.registrations[worker.registration.id] != worker.registration {
		return failure(ErrState, "service-worker-registration")
	}
	if worker.version != nil && (worker.version.retired || worker.version.registration != worker.registration.id || state.workers.versions[worker.version.id] != worker.version) {
		return failure(ErrState, "service-worker-version")
	}
	return nil
}

// RunServiceWorkerCommand accepts ID-based operations from an observed grant.
// URL-based registration operations use the explicitly context-wide control
// route, because native CDP resolves those URLs as first-party storage keys.
func RunServiceWorkerCommand[P, R any](ctx context.Context, worker ServiceWorker, command cdp.Command[P, R], params P) (result R, err error) {
	if worker.control == nil {
		return result, failure(ErrInput, "service-worker")
	}
	return runWorkerCommand(ctx, worker.control, command, params, worker.checkLocked, func(params any) error {
		fill := func(origin *string, id *serviceworker.RegistrationID) error {
			if *origin != "" && *origin != worker.registration.origin || *id != "" && *id != worker.registration.id {
				return failure(ErrInput, "service-worker-parameters")
			}
			*origin, *id = worker.registration.origin, worker.registration.id
			return nil
		}
		switch value := params.(type) {
		case *serviceworker.DeliverPushMessageParams:
			return fill(&value.Origin, &value.RegistrationID)
		case *serviceworker.DispatchSyncEventParams:
			return fill(&value.Origin, &value.RegistrationID)
		case *serviceworker.DispatchPeriodicSyncEventParams:
			return fill(&value.Origin, &value.RegistrationID)
		case *serviceworker.StopWorkerParams:
			if worker.version == nil || value.VersionID != "" && value.VersionID != worker.version.id {
				return failure(ErrInput, "service-worker-version")
			}
			value.VersionID = worker.version.id
			return nil
		}
		return failure(ErrUnsupported, "service-worker-command")
	})
}

// RunServiceWorkerContextCommand grants context-wide native controls, not a
// registration-ID operation. ScopeURL selects the native first-party scope.
// Native acknowledgement need not mean that asynchronous worker work completed.
func RunServiceWorkerContextCommand[P, R any](ctx context.Context, control *Control, command cdp.Command[P, R], params P) (result R, err error) {
	if control == nil || control.state == nil {
		return result, failure(ErrInput, "service-worker-control")
	}
	return runWorkerCommand(ctx, control.state, command, params, func() error {
		if control.state.workers == nil {
			return failure(ErrState, "service-worker-control")
		}
		return nil
	}, func(params any) error {
		var scope string
		switch value := params.(type) {
		case *serviceworker.StartWorkerParams:
			scope = value.ScopeURL
		case *serviceworker.SkipWaitingParams:
			scope = value.ScopeURL
		case *serviceworker.UpdateRegistrationParams:
			scope = value.ScopeURL
		case *serviceworker.UnregisterParams:
			scope = value.ScopeURL
		case *serviceworker.SetForceUpdateOnPageLoadParams:
			return nil
		case *cdp.Empty:
			if command.Method == serviceworker.StopAllWorkers.Method {
				return nil
			}
			return failure(ErrUnsupported, "service-worker-command")
		default:
			return failure(ErrUnsupported, "service-worker-command")
		}
		address, err := url.Parse(scope)
		if err != nil || address.Host == "" || address.User != nil || address.Fragment != "" || !address.IsAbs() {
			return failure(ErrInput, "service-worker-scope")
		}
		return nil
	})
}

func runWorkerCommand[P, R any](ctx context.Context, state *controlState, command cdp.Command[P, R], params P, check func() error, fill func(any) error) (result R, err error) {
	work, finish, err := state.enterCommand(ctx, check)
	if err != nil {
		return result, err
	}
	defer finish()
	return RunAction(work, state.session, Action[R]{local: func(work context.Context, session *Session) (result R, err error) {
		if _, err := session.storageContext(); err != nil {
			return result, err
		}
		schema, known := protocolCommands[command.Method]
		if !known || !nativeCommandType(reflect.TypeFor[P](), schema.params) || !nativeCommandType(reflect.TypeFor[R](), schema.result) {
			return result, failure(ErrInput, "service-worker-command-types")
		}
		if err := session.reserveCommand(); err != nil {
			return result, err
		}
		buffer := boundedBuffer{limit: session.owner.settings.MaxCommandBytes}
		if err := marshalBounded(&buffer, params); err != nil {
			return result, err
		}
		var copied P
		if err := jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions); err != nil {
			return result, err
		}
		if err := fill(&copied); err != nil {
			return result, err
		}
		verify := func() error {
			state.mu.Lock()
			defer state.mu.Unlock()
			return check()
		}
		return cdp.Call(session.commandWork(work, false, verify), sdk.FromContext(session.native).Target, command, copied)
	}})
}
