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

package temporal

import (
	"context"
	"errors"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
)

type interceptedReentryHandle struct {
	sdk.ActivityHandle
	get func(context.Context) error
}

func (*interceptedReentryHandle) GetID() string    { return "activity" }
func (*interceptedReentryHandle) GetRunID() string { return "run" }
func (handle *interceptedReentryHandle) Get(ctx context.Context, _ any) error {
	return handle.get(ctx)
}

func TestInterceptedResultReentryKeepsOnlyItsLiveOriginFrame(t *testing.T) {
	client, assembly, selected := bridgeFixture(t, 8)
	rawInbox, _ := invocation.NewInbox[RPCResult](1, 1024)
	raw, err := Bind(assembly, selected, rawInbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	client, _, err = WithWorkEnvelope(client, raw, 16*client.owner.settings.reservation())
	if err != nil {
		t.Fatal(err)
	}
	runtime := bridgeRuntime(t)
	gate, records := bridgeEndpoint[Execution](t, runtime, 8)
	client = WithAdmissions(client, gate, nil, nil)
	factory := &clientFactory{owner: client.owner, setup: &client.owner.setup}
	outer := newClientContinuation[any, nativePollOutput](factory, &interceptor.ClientOutboundInterceptorBase{}, nil, false)
	inner := newClientContinuation[any, nativePollOutput](factory, &interceptor.ClientOutboundInterceptorBase{}, nil, true)
	var retained sdk.ActivityHandle
	var nestedCalls int
	native := &interceptedReentryHandle{get: func(ctx context.Context) error {
		_, err := executeNative(ctx, client, fault.Correlation{Call: "nested-converter"}, Execution{Operation: "bridge.convert"}, func(context.Context, *Execution) (struct{}, error) {
			nestedCalls++
			return struct{}{}, nil
		})
		return err
	}}
	_, err = executeNative(context.Background(), client, fault.Correlation{Call: "origin"}, Execution{Operation: "bridge.origin"}, func(work context.Context, _ *Execution) (struct{}, error) {
		return clientContinue(work, outer, func(continued context.Context) (struct{}, error) {
			retained = inner.resultActivity(continued, native)
			return struct{}{}, retained.Get(continued, nil)
		})
	})
	if err != nil || nestedCalls != 1 {
		t.Fatal("intercepted result lost its live parent's bounded admission frame", err)
	}
	bridgeReceive(t, records)
	bridgeReceive(t, records)
	_, err = executeNative(context.Background(), client, fault.Correlation{Call: "different-origin"}, Execution{Operation: "bridge.new"}, func(work context.Context, _ *Execution) (struct{}, error) {
		if err := retained.Get(work, nil); !errors.Is(err, ErrAuthority) {
			t.Error("new supplied context revived an expired original handle", err)
		}
		return struct{}{}, nil
	})
	if err != nil || nestedCalls != 1 {
		t.Fatal("expired returned handle reached another native entry", err)
	}
	bridgeReceive(t, records)
}
