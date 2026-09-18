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

package lark

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestWebSocketOptionsAndRuntimePrivacy(t *testing.T) {
	base := OptionsV1{Name: "test", Profile: "application", AppID: "app_fixture", AppSecret: "secret", WebSocket: &WebSocketOptions{}}
	for _, change := range []func(*WebSocketOptions){
		func(v *WebSocketOptions) { v.MaxPending = -1 }, func(v *WebSocketOptions) { v.MaxFrameBytes = 2 << 20 },
		func(v *WebSocketOptions) { v.MaxFragments = 99999 }, func(v *WebSocketOptions) { v.MaxAssemblies = 100 },
		func(v *WebSocketOptions) { v.AckTimeout = 4 * time.Second }, func(v *WebSocketOptions) { v.MaxConnectAttempts = 999 },
		func(v *WebSocketOptions) { v.AllowedHosts = []string{"evil.example/path"} },
	} {
		input := base
		socket := *base.WebSocket
		change(&socket)
		input.WebSocket = &socket
		if _, err := Select(input); err == nil {
			t.Fatal("invalid receiver options accepted")
		}
	}
	if LimitsV1(base).MaxLeases != 18 {
		t.Fatal("pending ACKs lack shared retention allowance")
	}
	secret := "socket-private-canary"
	conformance.Runtime(t, WebSocketOptions{AllowedHosts: []string{secret}}, new(WebSocketOptions), secret)
	conformance.Runtime(t, WebSocketEvent{event: Event{data: secret}}, new(WebSocketEvent), secret)
	conformance.Runtime(t, WebSocketAck{frameID: secret}, new(WebSocketAck), secret)
	conformance.Runtime(t, WebSocketResult{lastFailure: errors.New(secret)}, new(WebSocketResult), secret)
	peer := newSocketPeer(t, false, nil)
	fixture := bindReceiverTest(t, socketTestOptions(peer), 8, 2)
	conformance.Facade(t, fixture.receiver, "String", "GoString", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON", "Profile", "Status", "Listen")
	if _, err := fixture.receiver.Listen(context.Background(), fault.Correlation{Call: "uncancellable"}, fixture.events); err == nil {
		t.Fatal("unowned infinite lifetime allowed")
	}
	if peer.bootstraps.Load() != 0 {
		t.Fatal("construction/binding performed I/O")
	}
	tiny := resource.WithLimits(fixture.selected, resource.Limits{Active: 1, Bytes: 1, MaxLeases: 1})
	assembly, err := resource.Assemble(context.Background(), context.Background(), "tiny", tiny)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(context.Background())
	if _, err = BindReceiver(assembly, tiny, fixture.results, nil); err == nil {
		t.Fatal("undersized receiver bound")
	}
}
