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
	"crypto/x509"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/gorilla/websocket"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

func (run *socketRun) connect() (*websocket.Conn, socketConfiguration, error) {
	var configuration socketConfiguration
	value := run.receiver.owner.settings
	work, cancel, err := (invocation.Budget{Limit: value.Timeout}).Context(run.ctx, invocation.Establish)
	if err != nil {
		return nil, configuration, err
	}
	defer cancel()
	state, requestErr := run.receiver.owner.nativeRequest(work, run.call, requestSpec{operation: "websocket-bootstrap", method: "POST", path: larkws.GenEndpointUri,
		body: &larkws.BootstrapRequest{AppID: value.AppID, AppSecret: value.AppSecret}, responseLimit: 64 << 10}, "")
	run.bootstrap = exchange(state)
	if state.cleanup != nil {
		return nil, configuration, failure(ErrCleanup, "websocket-bootstrap", state.cleanup, requestErr)
	}
	if requestErr != nil {
		return nil, configuration, requestErr
	}
	observed, data, err := decodeResponse(state, false)
	run.bootstrap = observed
	if err != nil {
		if observed.codeKnown && observed.code != larkws.SystemBusy && observed.code != larkws.InternalError && observed.status < 500 && observed.status != 429 {
			err = failure(ErrAuth, "websocket-bootstrap", err)
		}
		return nil, configuration, err
	}
	if _, err := exactFields(data, "URL", "ClientConfig"); err != nil {
		return nil, configuration, failure(ErrProtocol, "websocket-endpoint", err)
	}
	var endpoint struct {
		URL           string          `json:"URL"`
		Configuration json.RawMessage `json:"ClientConfig"`
	}
	if json.Unmarshal(data, &endpoint) != nil {
		return nil, configuration, failure(ErrProtocol, "websocket-endpoint")
	}
	address, service, err := socketAddress(endpoint.URL, run.options.AllowedHosts)
	if err != nil {
		return nil, configuration, err
	}
	configuration, err = parseSocketConfiguration(endpoint.Configuration, run.options)
	if err != nil {
		return nil, configuration, err
	}
	configuration.service = service
	var raw *ownedConn
	var wire *upgradeConn
	stop := func() {}
	dialer := websocket.Dialer{HandshakeTimeout: value.Timeout, ReadBufferSize: 4096, WriteBufferSize: 4096, EnableCompression: false, Proxy: nil,
		NetDialTLSContext: func(ctx context.Context, network, hostPort string) (net.Conn, error) {
			if raw != nil {
				return nil, failure(ErrUnsupported, "websocket-redial")
			}
			socket, err := (&net.Dialer{Timeout: value.Timeout}).DialContext(ctx, network, hostPort)
			if err != nil {
				return nil, err
			}
			raw = &ownedConn{Conn: socket}
			done := make(chan struct{})
			after := context.AfterFunc(work, func() { _ = raw.Close(); close(done) })
			stop = func() {
				if !after() {
					<-done
				}
			}
			config := run.receiver.owner.transport.TLSClientConfig.Clone()
			config.ServerName = address.Hostname()
			config.NextProtos = []string{"http/1.1"}
			encrypted := tls.Client(raw, config)
			if err = encrypted.HandshakeContext(ctx); err != nil {
				return nil, err
			}
			wire = &upgradeConn{Conn: encrypted, remaining: 32 << 10}
			return wire, nil
		}}
	if _, err = run.call.Attempt(); err != nil {
		return nil, configuration, err
	}
	conn, response, dialErr := dialer.DialContext(work, endpoint.URL, http.Header{"User-Agent": {larkcore.UserAgent("fathomry")}})
	stop()
	if response != nil && response.Body != nil {
		if closeErr := response.Body.Close(); closeErr != nil {
			dialErr = errors.Join(dialErr, failure(ErrCleanup, "websocket-upgrade", closeErr))
		}
	}
	if response != nil {
		run.stats.LastHandshakeHTTP = response.StatusCode
		if response.StatusCode == 401 || response.StatusCode == 403 || response.Header.Get(larkws.HeaderHandshakeStatus) == "403" ||
			response.Header.Get(larkws.HeaderHandshakeAuthErrCode) == strconv.Itoa(larkws.ExceedConnLimit) {
			dialErr = failure(ErrAuth, "websocket-upgrade", dialErr)
		}
		if response.Header.Get("Sec-WebSocket-Extensions") != "" {
			dialErr = failure(ErrUnsupported, "websocket-compression", dialErr)
		}
	}
	var unknown x509.UnknownAuthorityError
	var hostname x509.HostnameError
	if errors.As(dialErr, &unknown) || errors.As(dialErr, &hostname) {
		dialErr = failure(ErrAuth, "websocket-trust", dialErr)
	}
	if work.Err() != nil {
		dialErr = errors.Join(dialErr, work.Err(), context.Cause(work))
	}
	if dialErr == nil && (conn == nil || response == nil || response.StatusCode != 101 || wire == nil) {
		dialErr = failure(ErrProtocol, "websocket-upgrade")
	}
	if dialErr != nil {
		if conn != nil {
			_ = conn.Close()
		}
		if raw != nil {
			_ = raw.Close()
		}
		return nil, configuration, dialErr
	}
	wire.remaining = -1
	conn.SetReadLimit(int64(run.options.MaxFrameBytes))
	return conn, configuration, nil
}
func socketAddress(value string, allowed []string) (*url.URL, int32, error) {
	if value == "" || len(value) > 16<<10 {
		return nil, 0, failure(ErrProtocol, "websocket-endpoint")
	}
	address, err := url.Parse(value)
	if err != nil || address.Scheme != "wss" || address.User != nil || address.Fragment != "" || address.Opaque != "" || address.Host == "" {
		return nil, 0, failure(ErrProtocol, "websocket-endpoint")
	}
	matched := false
	for _, host := range allowed {
		if strings.EqualFold(address.Host, host) {
			matched = true
		}
	}
	if !matched {
		return nil, 0, failure(ErrUnsupported, "websocket-host")
	}
	query, err := url.ParseQuery(address.RawQuery)
	if err != nil || len(query[larkws.ServiceID]) != 1 {
		return nil, 0, failure(ErrProtocol, "websocket-service")
	}
	service, err := strconv.ParseInt(query.Get(larkws.ServiceID), 10, 32)
	if err != nil || service < 0 {
		return nil, 0, failure(ErrProtocol, "websocket-service")
	}
	return address, int32(service), nil
}

// Read limits the plaintext HTTP Upgrade response before gorilla's ReadResponse
// allocates header storage. It is disabled only after a validated handshake.
type upgradeConn struct {
	net.Conn
	remaining int
}

func (conn *upgradeConn) Read(data []byte) (int, error) {
	if conn.remaining == 0 {
		return 0, failure(ErrLimit, "websocket-upgrade-headers")
	}
	if conn.remaining > 0 && len(data) > conn.remaining {
		data = data[:conn.remaining]
	}
	count, err := conn.Conn.Read(data)
	if conn.remaining > 0 {
		conn.remaining -= count
	}
	return count, err
}
