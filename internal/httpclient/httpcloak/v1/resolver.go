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
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	dnswire "github.com/miekg/dns"
	"github.com/sardanioss/httpcloak/transport"
)

// The selected DNS parser expands compressed names to at most 1004 presentation
// bytes. Account for name/string overlap plus decoded RR and list containers;
// an EDNS advertisement is not a hard bound on a received message.
const dnsResidenceBytes = int64(dnswire.MaxMsgSize)*(1024+128) + 64<<10

type echEntry struct {
	config  []byte
	expires time.Time
	stored  time.Time
}

func (own *owner) acquireWork(kind string) (func(), error) {
	own.mu.Lock()
	var count *int
	var limit int
	switch kind {
	case "dns":
		count, limit = &own.dnsActive, own.settings.MaxDNSActive
	case "quic":
		count, limit = &own.quicActive, own.settings.MaxQUICConnections
	case "control":
		count, limit = &own.controlActive, own.settings.MaxControlStreams
	}
	if count == nil || own.stopping || *count >= limit {
		own.mu.Unlock()
		return nil, failure(ErrLimit, kind+"-capacity")
	}
	*count++
	own.workers.Add(1)
	own.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { own.mu.Lock(); *count--; own.mu.Unlock(); own.workers.Done() }) }, nil
}

func (own *owner) trackConn(raw net.Conn, release func()) (net.Conn, error) {
	conn := &trackedConn{Conn: raw, owner: own, release: release}
	own.mu.Lock()
	own.sockets[conn] = struct{}{}
	stopping := own.stopping
	own.mu.Unlock()
	if stopping {
		return nil, errors.Join(failure(ErrState, "source-stopped"), conn.Close())
	}
	return conn, nil
}

func (own *owner) exchangeDNS(parent context.Context, host string, kind uint16) (_ *dnswire.Msg, resultErr error) {
	if len(host) > 253 || host == "" || own.settings.ResolverAddress == "" {
		return nil, failure(ErrInput, "resolver-authority")
	}
	if _, valid := dnswire.IsDomainName(host); !valid {
		return nil, failure(ErrInput, "resolver-host")
	}
	finish, err := own.acquireWork("dns")
	if err != nil {
		return nil, err
	}
	defer finish()
	ctx, cancel := context.WithTimeout(parent, own.settings.ResolverTimeout)
	defer cancel()
	release, err := own.acquireConnection()
	if err != nil {
		return nil, err
	}
	raw, err := (&net.Dialer{}).DialContext(ctx, own.settings.ResolverNetwork, own.settings.ResolverAddress)
	if err != nil {
		release()
		return nil, failure(ErrTransport, "resolver-dial", err, context.Cause(ctx))
	}
	tracked, err := own.trackConn(raw, release)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); _ = tracked.Close() })
	defer func() {
		if !stop() {
			<-done
		}
		resultErr = errors.Join(resultErr, tracked.Close())
	}()
	request := new(dnswire.Msg)
	request.SetQuestion(dnswire.Fqdn(host), kind)
	request.SetEdns0(4096, false)
	client := &dnswire.Client{Net: own.settings.ResolverNetwork, Timeout: own.settings.ResolverTimeout, UDPSize: dnswire.MaxMsgSize}
	response, _, err := client.ExchangeWithConnContext(ctx, request, &dnswire.Conn{Conn: raw, UDPSize: dnswire.MaxMsgSize})
	if err != nil {
		return nil, failure(ErrTransport, "resolver-query", err, context.Cause(ctx))
	}
	if response == nil || response.Truncated || response.Rcode != dnswire.RcodeSuccess && response.Rcode != dnswire.RcodeNameError {
		return nil, failure(ErrTransport, "resolver-response")
	}
	return response, nil
}

func (own *owner) resolveIPs(ctx context.Context, host string) ([]net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		return []net.IP{bytes.Clone(ip)}, nil
	}
	if len(host) > 253 {
		return nil, failure(ErrInput, "resolver-host")
	}
	var result []net.IP
	var causes error
	for _, kind := range []uint16{dnswire.TypeAAAA, dnswire.TypeA} {
		response, err := own.exchangeDNS(ctx, host, kind)
		if err != nil {
			causes = errors.Join(causes, err)
			continue
		}
		for _, record := range response.Answer {
			var ip net.IP
			switch value := record.(type) {
			case *dnswire.A:
				ip = value.A
			case *dnswire.AAAA:
				ip = value.AAAA
			}
			if ip == nil {
				continue
			}
			if len(result) >= own.settings.MaxResolvedAddresses {
				return nil, failure(ErrLimit, "resolved-addresses")
			}
			result = append(result, bytes.Clone(ip))
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(causes, err, context.Cause(ctx))
	}
	if len(result) == 0 {
		return nil, failure(ErrTransport, "resolved-addresses", causes)
	}
	return result, nil
}

func (own *owner) echConfig(ctx context.Context, target string) ([]byte, error) {
	if own.settings.DisableECH {
		return nil, nil
	}
	if raw := own.native.Transport.ECHConfig; len(raw) > 0 {
		return bytes.Clone(raw), nil
	}
	host := own.native.Transport.ECHConfigDomain
	if host == "" {
		host = target
	}
	if net.ParseIP(host) != nil {
		return nil, nil
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if len(host) > 253 {
		return nil, failure(ErrInput, "ech-host")
	}
	now := time.Now()
	own.mu.Lock()
	entry, found := own.ech[host]
	stopping := own.stopping
	own.mu.Unlock()
	if stopping {
		return nil, failure(ErrState, "source-stopped")
	}
	if found && now.Before(entry.expires) {
		return bytes.Clone(entry.config), nil
	}
	response, err := own.exchangeDNS(ctx, host, dnswire.TypeHTTPS)
	if err != nil {
		if ctx.Err() != nil {
			return nil, errors.Join(err, ctx.Err(), context.Cause(ctx))
		}
		own.mu.Lock()
		stopping := own.stopping
		own.mu.Unlock()
		if stopping {
			return nil, failure(ErrState, "source-stopped", err)
		}
		if found && now.Before(entry.expires.Add(5*time.Minute)) {
			return bytes.Clone(entry.config), nil
		}
		return nil, err
	}
	var config []byte
	ttl := uint32(300)
	for _, record := range response.Answer {
		value, ok := record.(*dnswire.HTTPS)
		if !ok {
			continue
		}
		for _, parameter := range value.Value {
			ech, ok := parameter.(*dnswire.SVCBECHConfig)
			if !ok || len(ech.ECH) == 0 {
				continue
			}
			if len(ech.ECH) > own.settings.MaxECHConfigBytes {
				return nil, failure(ErrLimit, "ech-config")
			}
			config, ttl = bytes.Clone(ech.ECH), value.Hdr.Ttl
			break
		}
		if config != nil {
			break
		}
	}
	own.mu.Lock()
	defer own.mu.Unlock()
	if own.stopping {
		return nil, failure(ErrState, "source-stopped")
	}
	if _, exists := own.ech[host]; !exists && len(own.ech) >= own.settings.MaxECHEntries {
		oldest := ""
		var when time.Time
		for name, cached := range own.ech {
			if oldest == "" || cached.stored.Before(when) || cached.stored.Equal(when) && name < oldest {
				oldest, when = name, cached.stored
			}
		}
		delete(own.ech, oldest)
	}
	own.ech[host] = echEntry{config: config, expires: time.Now().Add(time.Duration(ttl) * time.Second), stored: time.Now()}
	return bytes.Clone(config), nil
}

func (own *owner) invalidateECH(target string) {
	if own.native.Transport.ECHConfigDomain != "" {
		target = own.native.Transport.ECHConfigDomain
	}
	key := strings.ToLower(strings.TrimSuffix(target, "."))
	own.mu.Lock()
	delete(own.ech, key)
	own.mu.Unlock()
}

func (own *owner) listenUDP(network string, address *net.UDPAddr) (*net.UDPConn, error) {
	release, err := own.acquireConnection()
	if err != nil {
		return nil, err
	}
	local := ""
	if configured := own.native.Transport.LocalAddr; configured != "" {
		if address == nil {
			address = &net.UDPAddr{IP: net.ParseIP(configured)}
		}
		if address.IP.Equal(net.ParseIP(configured)) {
			local = configured
		}
	}
	conn, err := transport.ListenUDPWithLocalAddr(network, address, local)
	if err != nil {
		release()
		return nil, err
	}
	return own.trackUDP(conn, release)
}

func (own *owner) dialUDP(network string, local, remote *net.UDPAddr) (*net.UDPConn, error) {
	release, err := own.acquireConnection()
	if err != nil {
		return nil, err
	}
	if local == nil && own.native.Transport.LocalAddr != "" {
		local = &net.UDPAddr{IP: net.ParseIP(own.native.Transport.LocalAddr)}
	}
	dialer := &net.Dialer{LocalAddr: local}
	transport.ApplyLocalAddrControl(dialer, own.native.Transport.LocalAddr)
	raw, err := dialer.Dial(network, remote.String())
	if err != nil {
		release()
		return nil, err
	}
	return own.trackUDP(raw.(*net.UDPConn), release)
}

func (own *owner) trackUDP(conn *net.UDPConn, release func()) (*net.UDPConn, error) {
	own.mu.Lock()
	own.packets[conn] = release
	stopping := own.stopping
	own.mu.Unlock()
	if stopping {
		return nil, errors.Join(failure(ErrState, "source-stopped"), own.closeUDP(conn))
	}
	return conn, nil
}

func (own *owner) closeUDP(conn *net.UDPConn) error {
	if conn == nil {
		return nil
	}
	err := conn.Close()
	if err != nil && !errors.Is(err, net.ErrClosed) {
		own.recordCleanup(err)
		return err
	}
	own.mu.Lock()
	release := own.packets[conn]
	delete(own.packets, conn)
	own.mu.Unlock()
	if release != nil {
		release()
	}
	return nil
}
