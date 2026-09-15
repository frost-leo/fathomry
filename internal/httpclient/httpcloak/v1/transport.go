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

package httpcloak

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/sardanioss/httpcloak/transport"
	"golang.org/x/net/proxy"
)

type trackedConn struct {
	net.Conn
	owner   *owner
	release func()
	mu      sync.Mutex
	closed  bool
	err     error
}

func (conn *trackedConn) Close() error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.closed {
		return conn.err
	}
	err := conn.Conn.Close()
	if err == nil || errors.Is(err, net.ErrClosed) {
		conn.closed = true
		conn.release()
		conn.owner.mu.Lock()
		delete(conn.owner.sockets, conn)
		conn.owner.mu.Unlock()
	} else {
		conn.err = errors.Join(conn.err, err)
		conn.owner.recordCleanup(err)
	}
	return conn.err
}
func (own *owner) dialEndpoint(ctx context.Context, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	var last error
	for _, resolved := range addresses {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(last, err, context.Cause(ctx))
		}
		local := own.native.Transport.LocalAddr
		if local != "" && (net.ParseIP(local).To4() == nil) != (resolved.IP.To4() == nil) {
			continue
		}
		release, err := own.acquireConnection()
		if err != nil {
			return nil, err
		}
		dialer := &net.Dialer{Control: transport.BuildDialControl(&own.native.Preset.TCPFingerprint, local)}
		if local != "" {
			dialer.LocalAddr = &net.TCPAddr{IP: net.ParseIP(local)}
		}
		network := "tcp4"
		if resolved.IP.To4() == nil {
			network = "tcp6"
		}
		target := net.JoinHostPort(resolved.String(), port)
		raw, err := dialer.DialContext(ctx, network, target)
		if err != nil {
			release()
			last = err
			continue
		}
		conn := &trackedConn{Conn: raw, owner: own, release: release}
		own.mu.Lock()
		own.sockets[conn] = struct{}{}
		own.mu.Unlock()
		return conn, nil
	}
	if last == nil {
		last = failure(ErrTransport, "address-family")
	}
	return nil, last
}

type existingDialer struct{ conn net.Conn }

func (dialer existingDialer) Dial(string, string) (net.Conn, error) { return dialer.conn, nil }
func (dialer existingDialer) DialContext(context.Context, string, string) (net.Conn, error) {
	return dialer.conn, nil
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (conn *bufferedConn) Read(data []byte) (int, error) { return conn.reader.Read(data) }

type proxyHeaderReader struct {
	reader    io.Reader
	remaining int64
	tail      uint32
	finished  bool
}

func (reader *proxyHeaderReader) Read(data []byte) (int, error) {
	if reader.finished {
		return reader.reader.Read(data)
	}
	if reader.remaining == 0 {
		return 0, failure(ErrLimit, "connect-headers")
	}
	if int64(len(data)) > reader.remaining {
		data = data[:reader.remaining]
	}
	n, err := reader.reader.Read(data)
	for _, char := range data[:n] {
		reader.remaining--
		reader.tail = reader.tail<<8 | uint32(char)
		if reader.tail == 0x0d0a0d0a {
			reader.finished = true
			break
		}
	}
	return n, err
}
func (own *owner) dialRoute(ctx context.Context, host, port string, option RequestOptionsV1, current *binding) (result net.Conn, resultErr error) {
	target := net.JoinHostPort(host, port)
	if option.ProxyURL == "" {
		return own.dialEndpoint(ctx, target)
	}
	address, err := parseProxy(option.ProxyURL)
	if err != nil {
		return nil, err
	}
	proxyPort := address.Port()
	if proxyPort == "" {
		proxyPort = "1080"
		if address.Scheme == "http" {
			proxyPort = "80"
		}
		if address.Scheme == "https" {
			proxyPort = "443"
		}
	}
	raw, err := own.dialEndpoint(ctx, net.JoinHostPort(address.Hostname(), proxyPort))
	if err != nil {
		return nil, err
	}
	watchDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(watchDone); _ = raw.Close() })
	defer func() {
		if !stop() {
			<-watchDone
		}
		if resultErr != nil {
			resultErr = errors.Join(resultErr, raw.Close(), ctx.Err(), context.Cause(ctx))
		}
	}()
	var conn net.Conn = raw
	if address.Scheme == "socks5" || address.Scheme == "socks5h" {
		var auth *proxy.Auth
		if address.User != nil {
			password, _ := address.User.Password()
			auth = &proxy.Auth{User: address.User.Username(), Password: password}
		}
		dialer, err := proxy.SOCKS5("tcp", address.Host, auth, existingDialer{raw})
		if err != nil {
			return nil, err
		}
		return dialer.(proxy.ContextDialer).DialContext(ctx, "tcp", target)
	}
	if address.Scheme == "https" {
		config := &tls.Config{MinVersion: tls.VersionTLS12}
		if verify := current.verification(own.native.ProxyVerify); verify != nil {
			config.RootCAs = verify.RootCAs
			config.VerifyConnection = verify.VerifyConnection
			config.VerifyPeerCertificate = verify.VerifyPeerCertificate
		}
		config.ServerName = address.Hostname()
		config.NextProtos = []string{"http/1.1"}
		secured := tls.Client(raw, config)
		if err := secured.HandshakeContext(ctx); err != nil {
			return nil, err
		}
		conn = secured
	}
	headers := stdhttp.Header(option.ConnectHeaders.Clone())
	if headers == nil {
		headers = make(stdhttp.Header)
	}
	if address.User != nil {
		if headers.Get("Proxy-Authorization") != "" {
			return nil, failure(ErrInput, "proxy-auth-conflict")
		}
		password, _ := address.User.Password()
		headers.Set("Proxy-Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(address.User.Username()+":"+password)))
	}
	request := &stdhttp.Request{Method: "CONNECT", URL: &url.URL{Opaque: target}, Host: target, Header: headers}
	if err := request.Write(conn); err != nil {
		return nil, err
	}
	reader := bufio.NewReader(&proxyHeaderReader{reader: conn, remaining: own.settings.MaxHeaderBytes})
	response, err := stdhttp.ReadResponse(reader, &stdhttp.Request{Method: "HEAD"})
	if err != nil {
		return nil, err
	}
	if response.StatusCode != 200 {
		return nil, failure(ErrTransport, "connect-status")
	}
	return &bufferedConn{Conn: conn, reader: reader}, nil
}
func bindingKey(address *url.URL, option RequestOptionsV1) string {
	headers, _ := json.Marshal(option.ConnectHeaders)
	sum := sha256.Sum256(headers)
	return address.Scheme + "\x00" + strings.ToLower(address.Host) + "\x00" + option.ProxyURL + "\x00" + string(sum[:])
}
func (own *owner) checkout(ctx context.Context, address *url.URL, option RequestOptionsV1) (*binding, error) {
	key := bindingKey(address, option)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		own.mu.Lock()
		if own.stopping {
			own.mu.Unlock()
			return nil, failure(ErrState, "source-stopped")
		}
		for _, current := range own.bindings {
			if !current.busy && current.key == key {
				current.busy = true
				own.mu.Unlock()
				return current, nil
			}
		}
		if len(own.bindings)+own.retiring >= own.settings.MaxBindings {
			var old *binding
			for index, candidate := range own.bindings {
				if !candidate.busy {
					old = candidate
					own.bindings = append(own.bindings[:index], own.bindings[index+1:]...)
					break
				}
			}
			if old == nil {
				own.mu.Unlock()
				return nil, failure(ErrLimit, "bindings")
			}
			own.retiring++
			own.mu.Unlock()
			err := own.retire(old)
			own.mu.Lock()
			own.retiring--
			own.mu.Unlock()
			if err != nil {
				return nil, failure(ErrCleanup, "retire", err)
			}
			continue
		}
		current := &binding{key: key, busy: true, notifyDone: make(chan struct{}), callbackLimit: 2*own.settings.MaxReplays + 2*own.settings.MaxExchanges + 16}
		own.bindings = append(own.bindings, current)
		own.mu.Unlock()
		config := *own.native.Transport
		config.FathomryDialTCP = func(ctx context.Context, host, port string) (net.Conn, error) {
			return own.dialRoute(ctx, host, port, option, current)
		}
		if config.KeyLogWriter != nil {
			config.KeyLogWriter = keyLogWriter{current, config.KeyLogWriter}
		}
		protocol := transport.ProtocolHTTP2
		switch own.settings.Protocol {
		case HTTP1:
			protocol = transport.ProtocolHTTP1
		case HTTP3:
			protocol = transport.ProtocolHTTP3
		}
		var err error
		if protocol == transport.ProtocolHTTP3 {
			current.udpRelease, err = own.acquireConnection()
			if err != nil {
				return nil, errors.Join(err, own.returnBinding(current, true))
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, own.returnBinding(current, true))
		}
		current.native, err = transport.NewFathomryTransport(own.native.Preset, &config, protocol, current.verification(own.native.Verify), own.settings.InsecureSkipVerify, own.settings.DisableECH, own.settings.MaxHeaderBytes)
		if err != nil {
			return nil, errors.Join(err, own.returnBinding(current, true))
		}
		return current, nil
	}
}
func (own *owner) returnBinding(current *binding, retire bool) error {
	if retire {
		own.mu.Lock()
		own.retiring++
		for index, candidate := range own.bindings {
			if candidate == current {
				own.bindings = append(own.bindings[:index], own.bindings[index+1:]...)
				break
			}
		}
		own.mu.Unlock()
		err := own.retire(current)
		own.mu.Lock()
		own.retiring--
		own.mu.Unlock()
		return err
	}
	own.mu.Lock()
	current.busy = false
	own.mu.Unlock()
	return nil
}
func (own *owner) retire(current *binding) error {
	callbacks := current.stopCallbacks()
	var err error
	if current.native != nil {
		err = current.native.Close()
	}
	<-callbacks
	if current.udpRelease != nil {
		if current.native == nil || current.native.ResourcesReleased() {
			current.udpRelease()
			current.udpRelease = nil
			own.mu.Lock()
			delete(own.held, current)
			own.mu.Unlock()
		} else {
			own.mu.Lock()
			if own.held == nil {
				own.held = make(map[*binding]struct{})
			}
			own.held[current] = struct{}{}
			own.mu.Unlock()
			err = errors.Join(err, failure(ErrCleanup, "udp-release-unconfirmed"))
		}
	}
	own.recordCleanup(err)
	return err
}
