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
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

// Source is an opaque composition capability, not an SDK or close handle.
type Source struct {
	private
	owner *owner
}
type owner struct {
	settings     settings
	native       *larkcore.Config
	transport    *http.Transport
	tokenLock    chan struct{}
	token        string
	expires      time.Time
	socketGate   chan struct{}
	socketMu     sync.Mutex
	socketStatus WebSocketStats
}

// Client is a concurrent non-owning facade. Copies and borrowed selections share
// transport, token, admission and shutdown authority. It starts no goroutines.
type Client struct {
	private
	owner    *owner
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}

// Select validates and freezes strict versioned configuration without network I/O.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	version := options.Version
	if version == 0 {
		version = 1
	}
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: validate},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: version, Layers: layers})
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return resource.Select(prepared, func(ctx context.Context, value settings) (resource.Resource[Source], error) {
		if ctx.Err() != nil {
			return resource.Resource[Source]{}, failure(ErrInput, "construct", ctx.Err(), context.Cause(ctx))
		}
		config, _ := value.tls()
		owned := &owner{settings: value, tokenLock: make(chan struct{}, 1), socketGate: make(chan struct{}, 1)}
		owned.transport = &http.Transport{TLSClientConfig: config, Proxy: nil, DisableCompression: true, DisableKeepAlives: true,
			ForceAttemptHTTP2: false, TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{},
			DialTLSContext: owned.dialTLS, TLSHandshakeTimeout: value.Timeout,
			ResponseHeaderTimeout: value.Timeout, MaxResponseHeaderBytes: 32 << 10, MaxConnsPerHost: value.MaxActive}
		// Do not call root NewClient/NewCache: the native token managers are global.
		owned.native = &larkcore.Config{BaseUrl: value.BaseURL, AppId: value.AppID, AppSecret: value.AppSecret,
			EnableTokenCache: false, HttpClient: owned, Logger: quietLogger{}, Serializable: &larkcore.DefaultSerialization{}}
		if value.Profile == "webhook" {
			owned.native.AppId = "custom-bot"
			owned.native.AppSecret = "custom-bot"
		}
		return resource.Resource[Source]{Acquired: true, Capability: Source{owner: owned}, Release: owned.close}, nil
	}), nil
}

// Bind requires a separately owned evidence inbox. Draining/releasing it is a
// composition responsibility even when the direct caller handles every error.
func Bind(assembly *resource.Assembly, selected resource.Selection[Source], inbox *invocation.Inbox[Result], observer *invocation.Observer) (*Client, error) {
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		return nil, err
	}
	access, err := resource.AccessFor(assembly, selected)
	if err != nil {
		return nil, err
	}
	if source.owner == nil || inbox == nil {
		return nil, failure(ErrInput, "bind")
	}
	value, limits := source.owner.settings, access.Limits()
	if limits.Active > value.MaxActive || limits.Bytes < value.reservation() || limits.MaxLeases < 1 ||
		limits.Queued > 32 || limits.Queued > 0 && limits.QueuedBytes < value.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Client{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}
func (owned *owner) close(context.Context) resource.ReleaseResult {
	owned.transport.CloseIdleConnections()
	owned.token = ""
	owned.expires = time.Time{}
	// Assembly invokes release only after every admitted call has relinquished use.
	return resource.ReleaseResult{Quiescent: true, Released: true}
}

type quietLogger struct{}

func (owned *owner) dialTLS(ctx context.Context, network, address string) (net.Conn, error) {
	state, ok := ctx.Value(requestKey{}).(*requestState)
	if !ok || state.owner != owned {
		return nil, failure(ErrUnsupported, "dial")
	}
	state.dialMu.Lock()
	if state.dialClosed || state.dialStarted {
		state.dialMu.Unlock()
		return nil, failure(ErrUnsupported, "dial")
	}
	state.dialStarted = true
	state.dialDone = make(chan struct{})
	work, done := state.wireContext, state.dialDone
	state.dialMu.Unlock()
	defer close(done)
	// net/http detaches cancellation from dial contexts to support connection reuse.
	// This no-reuse profile restores the owning request's deadline and joins the
	// callback before relinquishing its invocation reservation.
	raw, err := (&net.Dialer{Timeout: owned.settings.Timeout}).DialContext(work, network, address)
	if err != nil {
		return nil, err
	}
	conn := &ownedConn{Conn: raw}
	state.dialMu.Lock()
	state.connection = conn
	state.dialMu.Unlock()
	config := owned.transport.TLSClientConfig.Clone()
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	config.ServerName = host
	config.NextProtos = []string{"http/1.1"}
	encrypted := tls.Client(conn, config)
	if err := encrypted.HandshakeContext(work); err != nil {
		return nil, err
	}
	return encrypted, nil
}

type ownedConn struct {
	net.Conn
	once sync.Once
	err  error
}

func (conn *ownedConn) Close() error {
	conn.once.Do(func() { conn.err = conn.Conn.Close() })
	return conn.err
}

func (quietLogger) Debug(context.Context, ...interface{}) {}
func (quietLogger) Info(context.Context, ...interface{})  {}
func (quietLogger) Warn(context.Context, ...interface{})  {}
func (quietLogger) Error(context.Context, ...interface{}) {}
