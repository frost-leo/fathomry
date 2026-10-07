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

package nethttp

import (
	"context"
	"errors"
	"net"
	"net/url"

	"golang.org/x/net/idna"
	"golang.org/x/net/proxy"
)

// SOCKS establishment must end inside the owned dial callback: the standard
// transport otherwise has no post-SOCKS event before its detached TLS handshake.
// This pinned helper uses the same native SOCKS framing/authentication algorithm.
func socksProxy(address *url.URL) bool {
	return address != nil && (address.Scheme == "socks5" || address.Scheme == "socks5h")
}

type socksForward struct {
	dial func(context.Context, string, string) (net.Conn, error)
}

func (forward socksForward) Dial(string, string) (net.Conn, error) {
	return nil, failure(ErrUnsupported, "contextless-socks-dial")
}
func (forward socksForward) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return forward.dial(ctx, network, address)
}

func (own *owner) dialSOCKS(ctx context.Context, network, target string, address *url.URL) (net.Conn, error) {
	op, ok := ctx.Value(operationKey{}).(*operation)
	if !ok || !op.callbacks.enter() {
		return nil, failure(ErrState, "socks-dial")
	}
	defer op.callbacks.leave()
	if !own.callbacks.enter() {
		return nil, failure(ErrState, "socks-dial")
	}
	defer own.callbacks.leave()
	work, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(op.ctx, cancel)
	defer stop()
	defer cancel()
	if op.ctx.Err() != nil {
		return nil, op.ctx.Err()
	}
	host := address.Hostname()
	ascii := true
	for index := range len(host) {
		if host[index] >= 128 {
			ascii = false
			break
		}
	}
	if !ascii {
		if converted, err := idna.Lookup.ToASCII(host); err == nil {
			host = converted
		}
	}
	port := address.Port()
	if port == "" {
		port = "1080"
	}
	var authentication *proxy.Auth
	if address.User != nil {
		password, _ := address.User.Password()
		authentication = &proxy.Auth{User: address.User.Username(), Password: password}
	}
	var physical net.Conn
	dialer, err := proxy.SOCKS5("tcp", net.JoinHostPort(host, port), authentication, socksForward{
		dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			var err error
			physical, err = own.dial(context.WithValue(ctx, firstTLSKey{}, false), network, address)
			return physical, err
		},
	})
	if err != nil {
		return nil, err
	}
	contextual, ok := dialer.(proxy.ContextDialer)
	if !ok {
		return nil, failure(ErrUnsupported, "socks-context")
	}
	if _, err = contextual.DialContext(work, network, target); err != nil {
		return nil, err
	}
	if err := op.ctx.Err(); err != nil {
		return nil, errors.Join(err, physical.Close())
	}
	if planned, _ := ctx.Value(targetTLSKey{}).(bool); planned {
		if !op.callbacks.enter() {
			return nil, errors.Join(failure(ErrState, "tls-entry"), physical.Close())
		}
	}
	// Return the original tracked socket so GotConn can claim its cancellation
	// fence. The helper's transparent Conn wrapper would hide that identity.
	return physical, nil
}
