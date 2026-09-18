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
	"encoding/json"
	"math/rand/v2"
	"strconv"
	"time"

	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

type socketConfiguration struct {
	pingInterval, reconnectFloor time.Duration
	connectLimit                 int
	service                      int32
}

func parseSocketConfiguration(data []byte, options socketSettings) (socketConfiguration, error) {
	var result socketConfiguration
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return result, nil
	}
	if err := checkJSON(data, 4096); err != nil {
		return result, failure(ErrProtocol, "websocket-configuration", err)
	}
	fields, err := exactFields(data, "PingInterval", "ReconnectInterval", "ReconnectCount", "ReconnectNonce")
	if err != nil {
		return result, failure(ErrProtocol, "websocket-configuration", err)
	}
	for _, name := range []string{"PingInterval", "ReconnectInterval", "ReconnectCount", "ReconnectNonce"} {
		if raw, ok := fields[name]; ok {
			var number *int
			if json.Unmarshal(raw, &number) != nil || number == nil {
				return result, failure(ErrProtocol, "websocket-configuration")
			}
			switch name {
			case "PingInterval":
				if *number < 1 || *number > 300 {
					return result, failure(ErrProtocol, "websocket-ping-interval")
				}
				result.pingInterval = time.Duration(*number) * time.Second
			case "ReconnectInterval":
				if *number < 0 || *number > 600 {
					return result, failure(ErrProtocol, "websocket-reconnect-interval")
				}
				result.reconnectFloor = min(time.Duration(*number)*time.Second, options.ReconnectMax)
			case "ReconnectCount":
				if *number < -1 || *number > 10000 {
					return result, failure(ErrProtocol, "websocket-reconnect-count")
				}
				if *number >= 0 {
					result.connectLimit = min(*number+1, options.MaxConnectAttempts)
				}
			case "ReconnectNonce":
				if *number < 0 || *number > 600 {
					return result, failure(ErrProtocol, "websocket-reconnect-nonce")
				}
			}
		}
	}
	return result, nil
}
func reconnectDelay(previous, floor time.Duration, options socketSettings) time.Duration {
	lower := min(options.ReconnectMax, max(options.ReconnectMin, floor))
	upper := min(options.ReconnectMax, max(lower, previous*2))
	if upper <= lower {
		return lower
	}
	return lower + time.Duration(rand.Int64N(int64(upper-lower)+1))
}
func decodeSocketFrame(data []byte) (larkws.Frame, map[string]string, error) {
	var frame larkws.Frame
	if len(data) == 0 {
		return frame, nil, failure(ErrProtocol, "websocket-frame")
	}
	if err := frame.Unmarshal(data); err != nil {
		return frame, nil, failure(ErrProtocol, "websocket-frame", err)
	}
	if !shortText(frame.LogIDNew, socketRoutingLimit) || frame.Size()-len(frame.Payload) > socketRoutingLimit {
		return frame, nil, failure(ErrLimit, "websocket-routing")
	}
	if frame.Method != int32(larkws.FrameTypeData) && frame.Method != int32(larkws.FrameTypeControl) || len(frame.Headers) > 64 {
		return frame, nil, failure(ErrProtocol, "websocket-frame")
	}
	if frame.PayloadEncoding != "" && frame.PayloadEncoding != "json" || frame.PayloadType != "" && frame.PayloadType != "json" && frame.PayloadType != "application/json" {
		return frame, nil, failure(ErrUnsupported, "websocket-encoding")
	}
	headers := make(map[string]string, len(frame.Headers))
	for _, header := range frame.Headers {
		if header.Key == "" || !shortText(header.Key, 64) || !shortText(header.Value, 2048) {
			return frame, nil, failure(ErrProtocol, "websocket-header")
		}
		if _, exists := headers[header.Key]; exists {
			return frame, nil, failure(ErrProtocol, "websocket-duplicate-header")
		}
		headers[header.Key] = header.Value
	}
	if headers[larkws.HeaderType] == "" {
		return frame, nil, failure(ErrProtocol, "websocket-frame-type")
	}
	return frame, headers, nil
}

type fragmentedMessage struct {
	first                 time.Time
	parts                 [][]byte
	count, total          int
	kind, trace, instance string
	service               int32
}
type fragmentStore struct {
	options socketSettings
	items   map[string]*fragmentedMessage
	bytes   int
}

func (store *fragmentStore) add(frame larkws.Frame, headers map[string]string, now time.Time) ([]byte, bool, error) {
	if headers[larkws.HeaderType] != "event" {
		return nil, false, failure(ErrUnsupported, "websocket-event-type")
	}
	id := headers[larkws.HeaderMessageID]
	if id == "" || !shortText(id, 256) {
		return nil, false, failure(ErrProtocol, "websocket-message-id")
	}
	sum, seq := 1, 0
	rawSum, hasSum := headers[larkws.HeaderSum]
	rawSeq, hasSeq := headers[larkws.HeaderSeq]
	if hasSum != hasSeq {
		return nil, false, failure(ErrProtocol, "websocket-fragments")
	}
	if hasSum {
		var err error
		sum, err = strconv.Atoi(rawSum)
		if err != nil {
			return nil, false, failure(ErrProtocol, "websocket-fragments")
		}
		seq, err = strconv.Atoi(rawSeq)
		if err != nil {
			return nil, false, failure(ErrProtocol, "websocket-fragments")
		}
	}
	if sum < 1 || sum > store.options.MaxFragments || seq < 0 || seq >= sum || len(frame.Payload) == 0 || len(frame.Payload) > store.options.MaxMessageBytes {
		return nil, false, failure(ErrProtocol, "websocket-fragments")
	}
	part := store.items[id]
	if sum == 1 {
		if part != nil {
			return nil, false, failure(ErrProtocol, "websocket-fragment-conflict")
		}
		return frame.Payload, true, nil
	}
	if part == nil {
		if len(store.items) >= store.options.MaxAssemblies {
			return nil, false, failure(ErrLimit, "websocket-assemblies")
		}
		part = &fragmentedMessage{first: now, parts: make([][]byte, sum), kind: headers[larkws.HeaderType], trace: headers[larkws.HeaderTraceID], instance: headers[larkws.HeaderInstanceID], service: frame.Service}
		store.items[id] = part
	}
	if len(part.parts) != sum || part.kind != headers[larkws.HeaderType] || part.trace != headers[larkws.HeaderTraceID] || part.instance != headers[larkws.HeaderInstanceID] || part.service != frame.Service {
		return nil, false, failure(ErrProtocol, "websocket-fragment-conflict")
	}
	if previous := part.parts[seq]; previous != nil {
		if !bytes.Equal(previous, frame.Payload) {
			return nil, false, failure(ErrProtocol, "websocket-fragment-conflict")
		}
		return nil, false, nil
	}
	if part.total+len(frame.Payload) > store.options.MaxMessageBytes || store.bytes+len(frame.Payload) > store.options.MaxFragmentBytes {
		return nil, false, failure(ErrLimit, "websocket-fragment-bytes")
	}
	part.parts[seq] = frame.Payload
	part.count++
	part.total += len(frame.Payload)
	store.bytes += len(frame.Payload)
	if part.count != sum {
		return nil, false, nil
	}
	payload := make([]byte, 0, part.total)
	for _, data := range part.parts {
		payload = append(payload, data...)
	}
	delete(store.items, id)
	store.bytes -= part.total
	return payload, true, nil
}
func (store *fragmentStore) deadline() time.Time {
	var deadline time.Time
	for _, part := range store.items {
		next := part.first.Add(store.options.FragmentTimeout)
		if deadline.IsZero() || next.Before(deadline) {
			deadline = next
		}
	}
	return deadline
}
func (store *fragmentStore) expire(now time.Time) int {
	count := 0
	for id, part := range store.items {
		if !now.Before(part.first.Add(store.options.FragmentTimeout)) {
			delete(store.items, id)
			store.bytes -= part.total
			count++
		}
	}
	return count
}
