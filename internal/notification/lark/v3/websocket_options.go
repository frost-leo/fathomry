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
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/internal/compatibility"
)

// WebSocketOptions enables the self-built application receiver when supplied to
// OptionsV1. It is borrowed during Select and frozen with the named source.
// AllowedHosts contains exact gateway host[:port] authorities, never wildcards.
// The Feishu/Lark public base URLs default to their corresponding frontier host;
// custom API bases require an explicit gateway allowlist.
//
// Defaults: 256 KiB/frame, 1 MiB/event, 16 pending acknowledgements, 8 partial
// messages, 64 fragments/message, 2 MiB total fragment bytes, 5 s fragment TTL,
// 2 s event acknowledgement deadline, 1 s writes, 120 s fallback ping interval,
// 15 s pong timeout, 8 connection attempts and jittered reconnect delays bounded
// by 1 s..2 min. Lifetime zero requires a cancellable caller context or deadline.
// Nonzero Lifetime also bounds source-admission waiting. Encoded routing metadata
// has a separate fixed 8 KiB bound, including LogIDNew and protobuf overhead.
// All durations are ns. These are process-local limits, not account-wide quotas.
type WebSocketOptions struct {
	private
	AllowedHosts                                                                              []string
	MaxFrameBytes, MaxMessageBytes, MaxPending, MaxAssemblies, MaxFragments, MaxFragmentBytes int
	FragmentTimeout, AckTimeout, WriteTimeout, PingInterval, PongTimeout                      time.Duration
	MaxConnectAttempts                                                                        int
	ReconnectMin, ReconnectMax, Lifetime                                                      time.Duration
}
type socketSettings struct {
	Enabled            bool          `json:"enabled"`
	AllowedHosts       []string      `json:"allowed_hosts"`
	MaxFrameBytes      int           `json:"max_frame_bytes"`
	MaxMessageBytes    int           `json:"max_message_bytes"`
	MaxPending         int           `json:"max_pending"`
	MaxAssemblies      int           `json:"max_assemblies"`
	MaxFragments       int           `json:"max_fragments"`
	MaxFragmentBytes   int           `json:"max_fragment_bytes"`
	FragmentTimeout    time.Duration `json:"fragment_timeout_ns"`
	AckTimeout         time.Duration `json:"ack_timeout_ns"`
	WriteTimeout       time.Duration `json:"write_timeout_ns"`
	PingInterval       time.Duration `json:"ping_interval_ns"`
	PongTimeout        time.Duration `json:"pong_timeout_ns"`
	MaxConnectAttempts int           `json:"max_connect_attempts"`
	ReconnectMin       time.Duration `json:"reconnect_min_ns"`
	ReconnectMax       time.Duration `json:"reconnect_max_ns"`
	Lifetime           time.Duration `json:"lifetime_ns"`
}

func socketDefaults(input *WebSocketOptions, base string) socketSettings {
	value := socketSettings{Enabled: input != nil, MaxFrameBytes: 256 << 10, MaxMessageBytes: 1 << 20, MaxPending: 16, MaxAssemblies: 8, MaxFragments: 64, MaxFragmentBytes: 2 << 20,
		FragmentTimeout: 5 * time.Second, AckTimeout: 2 * time.Second, WriteTimeout: time.Second, PingInterval: 120 * time.Second, PongTimeout: 15 * time.Second,
		MaxConnectAttempts: 8, ReconnectMin: time.Second, ReconnectMax: 2 * time.Minute}
	switch base {
	case "https://open.feishu.cn":
		value.AllowedHosts = []string{"msg-frontier.feishu.cn"}
	case "https://open.larksuite.com":
		value.AllowedHosts = []string{"msg-frontier.larksuite.com"}
	}
	if input == nil {
		return value
	}
	if input.AllowedHosts != nil {
		value.AllowedHosts = append([]string(nil), input.AllowedHosts...)
	}
	for _, pair := range []struct {
		in  int
		out *int
	}{
		{input.MaxFrameBytes, &value.MaxFrameBytes}, {input.MaxMessageBytes, &value.MaxMessageBytes}, {input.MaxPending, &value.MaxPending},
		{input.MaxAssemblies, &value.MaxAssemblies}, {input.MaxFragments, &value.MaxFragments}, {input.MaxFragmentBytes, &value.MaxFragmentBytes}, {input.MaxConnectAttempts, &value.MaxConnectAttempts},
	} {
		if pair.in != 0 {
			*pair.out = pair.in
		}
	}
	for _, pair := range []struct {
		in  time.Duration
		out *time.Duration
	}{
		{input.FragmentTimeout, &value.FragmentTimeout}, {input.AckTimeout, &value.AckTimeout}, {input.WriteTimeout, &value.WriteTimeout},
		{input.PingInterval, &value.PingInterval}, {input.PongTimeout, &value.PongTimeout}, {input.ReconnectMin, &value.ReconnectMin}, {input.ReconnectMax, &value.ReconnectMax},
	} {
		if pair.in != 0 {
			*pair.out = pair.in
		}
	}
	value.Lifetime = input.Lifetime
	return value
}
func (value socketSettings) validate(profile string) error {
	if !value.Enabled {
		return nil
	}
	if profile != "application" {
		return failure(ErrUnsupported, "websocket-profile")
	}
	if len(value.AllowedHosts) == 0 || len(value.AllowedHosts) > 8 || value.MaxFrameBytes < 1024 || value.MaxFrameBytes > 1<<20 ||
		value.MaxMessageBytes < 1024 || value.MaxMessageBytes > 4<<20 || value.MaxPending < 1 || value.MaxPending > 64 ||
		value.MaxAssemblies < 1 || value.MaxAssemblies > 64 || value.MaxFragments < 1 || value.MaxFragments > 128 ||
		value.MaxFragmentBytes < value.MaxMessageBytes || value.MaxFragmentBytes > 32<<20 ||
		value.FragmentTimeout < time.Millisecond || value.FragmentTimeout > 30*time.Second || value.AckTimeout < time.Millisecond || value.AckTimeout > 3*time.Second ||
		value.WriteTimeout < time.Millisecond || value.WriteTimeout > 3*time.Second || value.PingInterval < time.Millisecond || value.PingInterval > 5*time.Minute ||
		value.PongTimeout < time.Millisecond || value.PongTimeout > time.Minute || value.MaxConnectAttempts < 1 || value.MaxConnectAttempts > 100 ||
		value.ReconnectMin < time.Millisecond || value.ReconnectMax < value.ReconnectMin || value.ReconnectMax > 10*time.Minute || value.Lifetime < 0 || value.Lifetime > 24*time.Hour {
		return failure(ErrInput, "websocket-options")
	}
	seen := map[string]bool{}
	for _, host := range value.AllowedHosts {
		parsed, err := url.Parse("wss://" + host)
		if err != nil || parsed.Host != host || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" ||
			host != strings.ToLower(host) || len(host) > 300 || parsed.Hostname() == "" || seen[host] {
			return failure(ErrInput, "websocket-host")
		}
		if port := parsed.Port(); port != "" {
			number, err := strconv.Atoi(port)
			if err != nil || number < 1 || number > 65535 {
				return failure(ErrInput, "websocket-port")
			}
		}
		if strings.ContainsAny(parsed.Hostname(), "\r\n\x00 %") || net.ParseIP(parsed.Hostname()) == nil && !identifier(strings.ReplaceAll(parsed.Hostname(), "-", "_")) {
			return failure(ErrInput, "websocket-host")
		}
		seen[host] = true
	}
	return nil
}
func (value socketSettings) reservation() int64 {
	if !value.Enabled {
		return 0
	}
	return int64(value.MaxFrameBytes)*8 + int64(value.MaxMessageBytes)*16 + int64(value.MaxFragmentBytes)*4 + int64(value.MaxPending)*(4*socketReplyLimit+16<<10)
}

const socketEvidenceBytes int64 = 256 << 10
const socketReplyLimit = 30 << 10
const socketRoutingLimit = 8 << 10

// WebSocketEvidenceBytes is required for each receiver result-inbox slot.
func WebSocketEvidenceBytes() int64 { return socketEvidenceBytes }

// WebSocketEventBytesV1 is the required per-slot event-inbox reservation.
func WebSocketEventBytesV1(options OptionsV1) int64 { return defaults(options).WebSocket.eventBytes() }
func (value socketSettings) eventBytes() int64      { return int64(value.MaxMessageBytes)*2 + 16<<10 }
func (value settings) maxLeases() int {
	if value.WebSocket.Enabled {
		return value.WebSocket.MaxPending + 2
	}
	return 1
}

// Profile reports declarations, not evidence of subscription or service readiness.
func (receiver *Receiver) Profile() compatibility.Profile {
	if receiver == nil || receiver.owner == nil {
		return compatibility.Profile{}
	}
	base := (&Client{owner: receiver.owner}).Profile()
	base.SDKMode = "lark-websocket"
	base.Protocol = compatibility.Fact{Kind: compatibility.Declared, Value: "wss-pbbp2"}
	value := receiver.owner.settings.WebSocket
	base.Options = append(base.Options, compatibility.Option{Name: "acknowledgement", Value: "explicit-after-handoff"})
	for _, pair := range []struct {
		name  string
		value int64
	}{
		{"ws-frame-bytes", int64(value.MaxFrameBytes)}, {"ws-message-bytes", int64(value.MaxMessageBytes)}, {"ws-pending", int64(value.MaxPending)},
		{"ws-fragment-bytes", int64(value.MaxFragmentBytes)}, {"ws-ack-timeout-ns", int64(value.AckTimeout)}, {"ws-attempts", int64(value.MaxConnectAttempts)},
		{"ws-pong-timeout-ns", int64(value.PongTimeout)}, {"ws-lifetime-ns", int64(value.Lifetime)},
	} {
		base.Options = append(base.Options, compatibility.Option{Name: pair.name, Value: strconv.FormatInt(pair.value, 10)})
	}
	return base
}
