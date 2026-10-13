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
	"errors"
	"testing"

	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/serviceworker"
)

func TestServiceWorkerOriginsRevokeBeforeDelivery(t *testing.T) {
	state := &controlState{session: unitSession(t), life: context.Background(), workers: newWorkerLedger()}
	registration := jsonv2.Value(`{"registrations":[{"registrationId":"17","scopeURL":"https://fixture.invalid/","isDeleted":false}]}`)
	version := jsonv2.Value(`{"versions":[{"versionId":"23","registrationId":"17","scriptURL":"https://fixture.invalid/worker.js","status":"activated","runningStatus":"running"}]}`)
	if err := state.observeWorkers(serviceworker.WorkerRegistrationUpdated.Method, registration, 1); err != nil {
		t.Fatal(err)
	}
	if err := state.observeWorkers(serviceworker.WorkerVersionUpdated.Method, version, 2); err != nil {
		t.Fatal(err)
	}
	pending := Pending{state: &pendingState{workers: state.workers.events[2]}}
	worker, err := pending.ServiceWorker(0)
	if err != nil || worker.version.id != "23" || worker.registration.id != "17" {
		t.Fatal("native version lost registration lineage", err)
	}
	if _, err := pending.ServiceWorker(-1); !errors.Is(err, ErrInput) {
		t.Fatal("invalid event index accepted", err)
	}
	if err := state.observeWorkers(serviceworker.WorkerRegistrationUpdated.Method, jsonv2.Value(`{"registrations":[{"registrationId":"17","scopeURL":"https://fixture.invalid/","isDeleted":true}]}`), 3); err != nil {
		t.Fatal(err)
	}
	if _, err := pending.ServiceWorker(0); !errors.Is(err, ErrState) {
		t.Fatal("queued historical event revived a deleted worker", err)
	}
	if err := state.observeWorkers(serviceworker.WorkerRegistrationUpdated.Method, registration, 4); err != nil {
		t.Fatal(err)
	}
	if err := worker.check(); !errors.Is(err, ErrState) {
		t.Fatal("old grant followed a replacement registration", err)
	}
}

func TestServiceWorkerProducerBoundsAndMalformedData(t *testing.T) {
	for _, data := range []string{`{`, `{"registrations":[null]}`, `{"registrations":[{"scopeURL":"https://fixture.invalid/"}]}`, `{"registrations":[{"registrationId":"one","scopeURL":"https://credential@fixture.invalid/"}]}`} {
		state := &controlState{session: unitSession(t), life: context.Background(), workers: newWorkerLedger()}
		if err := state.observeWorkers(serviceworker.WorkerRegistrationUpdated.Method, jsonv2.Value(data), 1); err == nil {
			t.Fatal("malformed registration accepted", data)
		}
	}
	state := &controlState{session: unitSession(t), life: context.Background(), workers: newWorkerLedger()}
	state.session.owner.settings.MaxEvents = 1
	if err := state.observeWorkers(serviceworker.WorkerRegistrationUpdated.Method, jsonv2.Value(`{"registrations":[{"registrationId":"one","scopeURL":"https://fixture.invalid/"}]}`), 1); !errors.Is(err, ErrLimit) || len(state.workers.registrations) != 0 {
		t.Fatal("registration retention preceded count admission", err)
	}
	state = &controlState{session: unitSession(t), life: context.Background(), workers: newWorkerLedger()}
	if err := state.observeWorkers(serviceworker.WorkerRegistrationUpdated.Method, jsonv2.Value(`{"registrations":[{"registrationId":"browser-native","scopeURL":"chrome-extension://synthetic-extension/"}]}`), 1); err != nil {
		t.Fatal("native browser scheme was incorrectly treated as malformed", err)
	}
	state = &controlState{session: unitSession(t), life: context.Background(), workers: newWorkerLedger()}
	state.session.owner.settings.MaxResultBytes = 1024
	if err := state.observeWorkers(serviceworker.WorkerRegistrationUpdated.Method, jsonv2.Value(`{"registrations":[{"registrationId":"one","scopeURL":"https://fixture.invalid/"}]}`), 1); !errors.Is(err, ErrLimit) || len(state.workers.registrations) != 0 {
		t.Fatal("identity retention preceded metadata byte admission", err)
	}
}

func TestServiceWorkerRedundantVersionCannotReviveAnOldGrant(t *testing.T) {
	state := &controlState{session: unitSession(t), life: context.Background(), workers: newWorkerLedger()}
	registration := jsonv2.Value(`{"registrations":[{"registrationId":"17","scopeURL":"https://fixture.invalid/"}]}`)
	active := jsonv2.Value(`{"versions":[{"versionId":"23","registrationId":"17","status":"activated"}]}`)
	if err := state.observeWorkers(serviceworker.WorkerRegistrationUpdated.Method, registration, 1); err != nil {
		t.Fatal(err)
	}
	if err := state.observeWorkers(serviceworker.WorkerVersionUpdated.Method, active, 2); err != nil {
		t.Fatal(err)
	}
	worker := state.workers.events[2].values[0]
	if err := state.observeWorkers(serviceworker.WorkerVersionUpdated.Method, jsonv2.Value(`{"versions":[{"versionId":"23","registrationId":"17","status":"redundant"}]}`), 3); err != nil {
		t.Fatal(err)
	}
	if err := state.observeWorkers(serviceworker.WorkerVersionUpdated.Method, active, 4); err != nil {
		t.Fatal(err)
	}
	if err := worker.check(); !errors.Is(err, ErrState) {
		t.Fatal("old version grant followed a reused native identity", err)
	}
	if err := state.workers.events[4].values[0].check(); err != nil {
		t.Fatal("current observed version lost its own grant", err)
	}
}

func FuzzServiceWorkerOrigins(f *testing.F) {
	f.Add(uint8(0), uint64(1), []byte(`{"registrations":[{"registrationId":"one","scopeURL":"https://fixture.invalid/"}]}`))
	f.Add(uint8(1), uint64(2), []byte(`{"versions":[{"versionId":"two","registrationId":"one","status":"activated"}]}`))
	f.Add(uint8(0), uint64(0), []byte(`{"registrations":[null]}`))
	f.Fuzz(func(t *testing.T, kind uint8, sequence uint64, data []byte) {
		state := &controlState{session: unitSession(t), life: context.Background(), workers: newWorkerLedger()}
		state.session.owner.settings.MaxEvents = 4
		state.session.owner.settings.MaxResultBytes = 4096
		methods := []string{serviceworker.WorkerRegistrationUpdated.Method, serviceworker.WorkerVersionUpdated.Method, serviceworker.WorkerErrorReported.Method}
		_ = state.observeWorkers(methods[int(kind)%len(methods)], data, sequence)
		if state.workers.count > 4 || state.workers.bytes > 4096 || len(state.workers.registrations)+len(state.workers.versions) > 3 || len(state.workers.events) > 1 {
			t.Fatal("native worker data exceeded its count/byte reservation")
		}
		for id, value := range state.workers.registrations {
			if id == "" || value.scope == "" || value.origin == "" || value.id != id || sequence == 0 {
				t.Fatal("unvalidated worker identity was retained")
			}
		}
	})
}
