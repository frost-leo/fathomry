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
	"bytes"
	"context"
	"errors"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
	"strconv"
	"testing"
	"time"
)

func fragmentedFrame(id string, sum, seq int, payload []byte) (larkws.Frame, map[string]string) {
	frame := eventFrame(id, payload)
	frame.Headers = append(frame.Headers, larkws.Header{Key: "sum", Value: strconv.Itoa(sum)}, larkws.Header{Key: "seq", Value: strconv.Itoa(seq)})
	headers := map[string]string{}
	for _, header := range frame.Headers {
		headers[header.Key] = header.Value
	}
	return frame, headers
}
func TestWebSocketFragmentOrderingConflictAndBounds(t *testing.T) {
	options := socketDefaults(&WebSocketOptions{MaxMessageBytes: 1024, MaxFragmentBytes: 1024, MaxAssemblies: 1, MaxFragments: 3}, "https://open.feishu.cn")
	store := fragmentStore{options: options, items: map[string]*fragmentedMessage{}}
	now := time.Now()
	for _, seq := range []int{2, 0, 0, 1} {
		frame, headers := fragmentedFrame("one", 3, seq, []byte(strconv.Itoa(seq)))
		payload, ready, err := store.add(frame, headers, now)
		if err != nil {
			t.Fatal(err)
		}
		if ready && string(payload) != "012" {
			t.Fatal("fragment ordering changed")
		}
	}
	if store.bytes != 0 || len(store.items) != 0 {
		t.Fatal("completed fragments retained storage")
	}
	frame, headers := fragmentedFrame("one", 2, 0, []byte("first"))
	if _, _, err := store.add(frame, headers, now); err != nil {
		t.Fatal(err)
	}
	frame.Payload = []byte("conflict")
	if _, _, err := store.add(frame, headers, now); !errors.Is(err, ErrProtocol) {
		t.Fatal("conflicting duplicate accepted")
	}
	frame, headers = fragmentedFrame("two", 2, 0, []byte("other"))
	if _, _, err := store.add(frame, headers, now); !errors.Is(err, ErrLimit) {
		t.Fatal("assembly count unbounded")
	}
	if store.expire(now.Add(options.FragmentTimeout)) != 1 || store.bytes != 0 {
		t.Fatal("expired fragments retained")
	}
	for _, pair := range [][2]int{{2, -1}, {2, 2}, {0, 0}, {999999999, 0}} {
		frame, headers = fragmentedFrame("bad", pair[0], pair[1], []byte("data"))
		if _, _, err := store.add(frame, headers, now); err == nil {
			t.Fatal("unbounded or invalid fragment allocation accepted")
		}
	}
	frame, headers = fragmentedFrame("large", 2, 0, bytes.Repeat([]byte("x"), 700))
	_, _, _ = store.add(frame, headers, now)
	frame, headers = fragmentedFrame("large", 2, 1, bytes.Repeat([]byte("x"), 700))
	if _, _, err := store.add(frame, headers, now); !errors.Is(err, ErrLimit) {
		t.Fatal("assembled byte limit ignored")
	}
}
func TestWebSocketFragmentedWireAndMalformedRejection(t *testing.T) {
	peer := newSocketPeer(t, false, nil)
	fixture := bindReceiverTest(t, socketTestOptions(peer), 16, 4)
	cancel, done := startReceiver(t, fixture)
	socket := nextSocket(t, peer)
	data := notificationEvent()
	middle := len(data) / 2
	second, _ := fragmentedFrame("fragmented", 2, 1, data[middle:])
	first, _ := fragmentedFrame("fragmented", 2, 0, data[:middle])
	_ = socket.send(second)
	_ = socket.send(first)
	event := nextEvent(t, fixture)
	if event.Event().ID() != "event_fixture" {
		t.Fatal("wire reassembly lost event")
	}
	_, err := event.Acknowledge(context.Background(), JSON{})
	if err != nil {
		t.Fatal(err)
	}
	socketACK(t, peer)
	bad, _ := fragmentedFrame("invalid", 2, -1, []byte("malformed"))
	_ = socket.send(bad)
	select {
	case result := <-done:
		got := awaitSocketResult(t, result.receipt)
		if !errors.Is(got.Err(), ErrProtocol) {
			t.Fatal("invalid native index not rejected")
		}
	case <-time.After(time.Second):
		cancel(context.Canceled)
		t.Fatal("bad fragments did not stop receiver")
	}
	drainSocketResults(t, fixture)
}
func TestWebSocketConfigEndpointAndHeaderRefusal(t *testing.T) {
	options := socketDefaults(&WebSocketOptions{}, "https://open.feishu.cn")
	for _, data := range []string{`{"PingInterval":0}`, `{"PingInterval":-1}`, `{"PingInterval":9999999999999}`, `{"ReconnectNonce":-1}`, `{"ReconnectCount":-9}`, `{"ReconnectCount":null}`, `{"PingInterval":1,"PingInterval":2}`} {
		if _, err := parseSocketConfiguration([]byte(data), options); err == nil {
			t.Fatal("dangerous native configuration accepted")
		}
	}
	for _, address := range []string{"ws://msg-frontier.feishu.cn/ws?service_id=1", "wss://evil.example/ws?service_id=1", "wss://msg-frontier.feishu.cn/ws?service_id=99999999999", "wss://msg-frontier.feishu.cn/ws?service_id=1&service_id=2", "wss://user:secret@msg-frontier.feishu.cn/ws?service_id=1"} {
		if _, _, err := socketAddress(address, options.AllowedHosts); err == nil {
			t.Fatal("untrusted gateway accepted")
		}
	}
	frame := eventFrame("duplicate", []byte("{}"))
	frame.Headers = append(frame.Headers, larkws.Header{Key: "type", Value: "event"})
	encoded, _ := frame.Marshal()
	if _, _, err := decodeSocketFrame(encoded); err == nil {
		t.Fatal("ambiguous headers accepted")
	}
	for range 100 {
		delay := reconnectDelay(time.Minute, time.Minute, options)
		if delay < time.Minute || delay > options.ReconnectMax {
			t.Fatal("backoff escaped bounds")
		}
	}
}
func FuzzWebSocketFrame(f *testing.F) {
	frame := eventFrame("one", notificationEvent())
	data, _ := frame.Marshal()
	f.Add(data)
	f.Add([]byte{255, 0, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 256<<10 {
			return
		}
		_, _, _ = decodeSocketFrame(data)
	})
}
