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

package nacos

import (
	"context"
	"crypto/tls"
	"errors"
	"google.golang.org/grpc/credentials"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/netip"
	"time"
)

func (client *Client) dial(ctx context.Context, address string) (net.Conn, error) {
	work, done, err := client.native(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	work, cancel := context.WithTimeout(work, client.settings.Timeout)
	defer cancel()
	if work.Err() != nil {
		return nil, fail(ErrClosed, "dial", work.Err(), context.Cause(work))
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	addresses := []string{address}
	if _, err := netip.ParseAddr(host); err != nil {
		// A canceled resolver wait does not join native DNS. Retain bounded native
		// responsibility until resolution actually returns; Close can report pending.
		resolved, err := (&net.Resolver{}).LookupIPAddr(context.WithoutCancel(work), host)
		if err != nil {
			return nil, fail(ErrUnavailable, "resolve", err, work.Err(), context.Cause(work))
		}
		if len(resolved) == 0 || len(resolved) > 64 {
			return nil, fail(ErrLimit, "resolve")
		}
		addresses = make([]string, len(resolved))
		for index, ip := range resolved {
			addresses[index] = net.JoinHostPort(ip.String(), port)
		}
	}
	dialer := &net.Dialer{Timeout: client.settings.Timeout, KeepAlive: 30 * time.Second, FallbackDelay: -1}
	deadline, _ := work.Deadline()
	var causes []error
	for index, candidate := range addresses {
		if work.Err() != nil {
			return nil, fail(ErrUnavailable, "dial", append(causes, work.Err(), context.Cause(work))...)
		}
		attempt, stop := context.WithTimeout(work, time.Until(deadline)/time.Duration(len(addresses)-index))
		socket, err := dialer.DialContext(attempt, "tcp", candidate)
		stop()
		if err == nil {
			if work.Err() != nil {
				_ = socket.Close()
				return nil, fail(ErrUnavailable, "dial", work.Err(), context.Cause(work))
			}
			return client.own(socket)
		}
		causes = append(causes, err)
	}
	return nil, fail(ErrUnavailable, "dial", append(causes, work.Err(), context.Cause(work))...)
}

func (client *Client) handshake(ctx context.Context, authority string, socket net.Conn, protocol string) (net.Conn, credentials.AuthInfo, error) {
	work, done, err := client.native(ctx)
	if err != nil {
		_ = socket.Close()
		return nil, nil, err
	}
	defer done()
	work, cancel := context.WithTimeout(work, client.settings.Timeout)
	defer cancel()
	host, _, err := net.SplitHostPort(authority)
	if err != nil {
		host = authority
	}
	config := client.trust.Clone()
	config.ServerName = host
	config.NextProtos = []string{protocol}
	secure := tls.Client(socket, config)
	trace := httptrace.ContextClientTrace(work)
	if protocol == "http/1.1" && trace != nil && trace.TLSHandshakeStart != nil {
		trace.TLSHandshakeStart()
	}
	err = secure.HandshakeContext(work)
	if err != nil {
		err = errors.Join(err, socket.Close())
	}
	state := secure.ConnectionState()
	if protocol == "http/1.1" && trace != nil && trace.TLSHandshakeDone != nil {
		trace.TLSHandshakeDone(state, err)
	}
	if err != nil {
		return nil, nil, err
	}
	if work.Err() != nil {
		_ = socket.Close()
		return nil, nil, fail(ErrUnavailable, "handshake", work.Err(), context.Cause(work))
	}
	if protocol == "h2" && state.NegotiatedProtocol != "h2" {
		_ = socket.Close()
		return nil, nil, fail(ErrUnsupported, "alpn")
	}
	return secure, credentials.TLSInfo{State: state, CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity}}, nil
}
func (client *Client) newHTTPTransport() *http.Transport {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	return &http.Transport{Proxy: nil, Protocols: protocols, DisableCompression: true,
		MaxConnsPerHost: client.settings.Active, MaxIdleConns: client.settings.Active, MaxIdleConnsPerHost: client.settings.Active,
		MaxResponseHeaderBytes: 64 << 10, IdleConnTimeout: 30 * time.Second,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) { return client.dial(ctx, address) },
		DialTLSContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			// Keep the whole HTTP acquisition owned across TCP/TLS handoff.
			work, done, err := client.enter(ctx)
			if err != nil {
				return nil, err
			}
			defer done()
			socket, err := client.dial(work, address)
			if err != nil {
				return nil, err
			}
			secure, _, err := client.handshake(work, address, socket, "http/1.1")
			if err != nil {
				return nil, err
			}
			// Avoid net/http repeating TLS hooks outside the owned acquisition.
			return struct{ net.Conn }{secure}, nil
		}}
}

type sessionCredentials struct{ session *session }

func (value *sessionCredentials) ClientHandshake(ctx context.Context, authority string, socket net.Conn) (net.Conn, credentials.AuthInfo, error) {
	work, cancel := context.WithCancelCause(value.session.ctx)
	defer cancel(nil)
	stop := context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	defer stop()
	if ctx.Err() != nil {
		cancel(context.Cause(ctx))
	}
	secure, info, err := value.session.owner.handshake(work, authority, socket, "h2")
	if err != nil {
		value.session.cancel(err)
	}
	return secure, info, err
}
func (*sessionCredentials) ServerHandshake(net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, fail(ErrUnsupported, "server-handshake")
}
func (*sessionCredentials) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls", SecurityVersion: "1.2"}
}
func (value *sessionCredentials) Clone() credentials.TransportCredentials {
	return &sessionCredentials{value.session}
}
func (*sessionCredentials) OverrideServerName(string) error {
	return fail(ErrUnsupported, "server-name-override")
}
