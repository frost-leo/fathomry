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
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	dnswire "github.com/miekg/dns"
	"github.com/sardanioss/httpcloak/transport"
)

type socksUDPRoute struct{ password, host, target string }
type socksUDPFixture struct {
	address     string
	active      atomic.Int32
	denied      atomic.Int32
	packets     atomic.Int32
	problems    chan error
	mu          sync.Mutex
	connections map[net.Conn]bool
	workers     sync.WaitGroup
}

func socksUDPRelay(t *testing.T, routes map[string]socksUDPRoute) *socksUDPFixture {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fixture := &socksUDPFixture{address: listener.Addr().String(), problems: make(chan error, 16), connections: make(map[net.Conn]bool)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			fixture.mu.Lock()
			fixture.connections[conn] = true
			fixture.mu.Unlock()
			fixture.workers.Add(1)
			go func() {
				defer fixture.workers.Done()
				defer func() { _ = conn.Close(); fixture.mu.Lock(); delete(fixture.connections, conn); fixture.mu.Unlock() }()
				fixture.serveSOCKSUDP(conn, routes)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
		fixture.mu.Lock()
		for conn := range fixture.connections {
			_ = conn.Close()
		}
		fixture.mu.Unlock()
		joined := make(chan struct{})
		go func() { fixture.workers.Wait(); close(joined) }()
		select {
		case <-joined:
		case <-time.After(time.Second):
			t.Error("SOCKS UDP peer retained workers")
		}
		select {
		case err := <-fixture.problems:
			t.Error("SOCKS UDP independent peer", err)
		default:
		}
	})
	return fixture
}

func (fixture *socksUDPFixture) serveSOCKSUDP(control net.Conn, routes map[string]socksUDPRoute) {
	_ = control.SetDeadline(time.Now().Add(5 * time.Second))
	var prefix [2]byte
	if _, err := io.ReadFull(control, prefix[:]); err != nil || prefix[0] != 5 {
		return
	}
	methods := make([]byte, int(prefix[1]))
	if _, err := io.ReadFull(control, methods); err != nil {
		return
	}
	if _, err := control.Write([]byte{5, 2}); err != nil {
		return
	}
	if _, err := io.ReadFull(control, prefix[:]); err != nil || prefix[0] != 1 {
		return
	}
	user := make([]byte, int(prefix[1]))
	if _, err := io.ReadFull(control, user); err != nil {
		return
	}
	var length [1]byte
	if _, err := io.ReadFull(control, length[:]); err != nil {
		return
	}
	password := make([]byte, int(length[0]))
	if _, err := io.ReadFull(control, password); err != nil {
		return
	}
	route, valid := routes[string(user)]
	if !valid || string(password) != route.password {
		fixture.denied.Add(1)
		_, _ = control.Write([]byte{1, 1})
		return
	}
	if _, err := control.Write([]byte{1, 0}); err != nil {
		return
	}
	var associate [10]byte
	if _, err := io.ReadFull(control, associate[:]); err != nil || associate[0] != 5 || associate[1] != 3 || associate[3] != 1 {
		return
	}
	relay, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		fixture.problems <- err
		return
	}
	defer relay.Close()
	target, err := net.ResolveUDPAddr("udp", route.target)
	if err != nil {
		fixture.problems <- err
		return
	}
	upstream, err := net.DialUDP("udp", nil, target)
	if err != nil {
		fixture.problems <- err
		return
	}
	defer upstream.Close()
	port := relay.LocalAddr().(*net.UDPAddr).Port
	if _, err := control.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, byte(port >> 8), byte(port)}); err != nil {
		return
	}
	_ = control.SetDeadline(time.Time{})
	fixture.active.Add(1)
	defer fixture.active.Add(-1)
	var destination *net.UDPAddr
	var destinationMu sync.Mutex
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		packet := make([]byte, 65535)
		for {
			n, client, err := relay.ReadFromUDP(packet)
			if err != nil {
				return
			}
			host, port, body, err := decodeSOCKSDatagram(packet[:n])
			if err != nil || host != route.host || port != target.Port {
				select {
				case fixture.problems <- errors.New("target or credential route crossed"):
				default:
				}
				continue
			}
			fixture.packets.Add(1)
			destinationMu.Lock()
			destination = client
			destinationMu.Unlock()
			if _, err := upstream.Write(body); err != nil {
				return
			}
		}
	}()
	go func() {
		defer workers.Done()
		packet := make([]byte, 65535)
		for {
			n, err := upstream.Read(packet)
			if err != nil {
				return
			}
			destinationMu.Lock()
			client := destination
			destinationMu.Unlock()
			if client == nil {
				continue
			}
			response := []byte{0, 0, 0, 1, 127, 0, 0, 1, byte(target.Port >> 8), byte(target.Port)}
			response = append(response, packet[:n]...)
			if _, err := relay.WriteToUDP(response, client); err != nil {
				return
			}
		}
	}()
	_, _ = io.Copy(io.Discard, control)
	_ = relay.Close()
	_ = upstream.Close()
	workers.Wait()
}

func decodeSOCKSDatagram(packet []byte) (string, int, []byte, error) {
	if len(packet) < 7 || packet[0] != 0 || packet[1] != 0 || packet[2] != 0 {
		return "", 0, nil, errors.New("invalid SOCKS datagram")
	}
	position, count := 4, 0
	switch packet[3] {
	case 1:
		count = 4
	case 4:
		count = 16
	case 3:
		count = int(packet[4])
		position++
	default:
		return "", 0, nil, errors.New("invalid SOCKS address")
	}
	if len(packet) < position+count+2 {
		return "", 0, nil, io.ErrUnexpectedEOF
	}
	host := string(packet[position : position+count])
	if packet[3] != 3 {
		host = net.IP(packet[position : position+count]).String()
	}
	position += count
	return host, int(binary.BigEndian.Uint16(packet[position : position+2])), packet[position+2:], nil
}

func TestManagedSOCKSUDPTargetIsolationAndNoDirectFallback(t *testing.T) {
	for _, configured := range []bool{false, true} {
		name := "no-local-resolver"
		if configured {
			name = "controlled-empty-resolver"
		}
		t.Run(name, func(t *testing.T) { testManagedSOCKSUDP(t, configured) })
	}
}

func testManagedSOCKSUDP(t *testing.T, configured bool) {
	certificate, roots := protocolCertificate(t, "one.invalid", "two.invalid")
	var firstCalls, secondCalls atomic.Int32
	first := protocolH3Peer(t, &tls.Config{Certificates: []tls.Certificate{certificate}}, http.HandlerFunc(func(writer http.ResponseWriter, input *http.Request) {
		firstCalls.Add(1)
		if input.Header.Get("Proxy-Authorization") != "" {
			t.Error("proxy credentials reached origin")
		}
		_, _ = writer.Write([]byte("one"))
	}))
	second := protocolH3Peer(t, &tls.Config{Certificates: []tls.Certificate{certificate}}, http.HandlerFunc(func(writer http.ResponseWriter, input *http.Request) {
		secondCalls.Add(1)
		if input.Header.Get("Proxy-Authorization") != "" {
			t.Error("proxy credentials reached second origin")
		}
		_, _ = writer.Write([]byte("two"))
	}))
	firstURL, _ := url.Parse(first)
	secondURL, _ := url.Parse(second)
	relay := socksUDPRelay(t, map[string]socksUDPRoute{"alpha": {password: "first", host: "one.invalid", target: firstURL.Host}, "beta": {password: "second", host: "two.invalid", target: secondURL.Host}})
	options := OptionsV1{Name: "udp-routes", PresetName: "chrome-148", Protocol: HTTP3, DisableECH: true, MaxActive: 2, MaxBindings: 2, MaxConnections: 8, MaxQUICConnections: 2, Native: NativeOptionsV1{Verify: &transport.TLSVerify{RootCAs: roots}}}
	var queries atomic.Int32
	if configured {
		options.ResolverAddress = dnsPeer(t, func(writer dnswire.ResponseWriter, request *dnswire.Msg) {
			queries.Add(1)
			response := new(dnswire.Msg)
			response.SetReply(request)
			_ = writer.WriteMsg(response)
		})
	}
	fixture := bindFixture(t, options, 4)
	ctx := testContext(t)
	var workers sync.WaitGroup
	for index, entry := range []struct{ user, password, host, port, want string }{{"alpha", "first", "one.invalid", firstURL.Port(), "one"}, {"beta", "second", "two.invalid", secondURL.Port(), "two"}} {
		workers.Add(1)
		go func() {
			defer workers.Done()
			proxy := &url.URL{Scheme: "socks5h", Host: relay.address, User: url.UserPassword(entry.user, entry.password)}
			receipt, err := fixture.client.Do(ctx, ctx, fault.Correlation{Call: entry.want}, request(t, "GET", "https://"+net.JoinHostPort(entry.host, entry.port), nil), RequestOptionsV1{Proxy: ProxyAddress, ProxyURL: proxy.String()})
			if err != nil {
				t.Error("SOCKS UDP route", index, err)
				return
			}
			result, err := receipt.WaitReleased(ctx)
			if err != nil || !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != entry.want {
				t.Error("SOCKS UDP response crossed target", index, err)
			}
		}()
	}
	workers.Wait()
	for range 2 {
		delivery, err := fixture.inbox.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := delivery.Receipt().WaitReleased(ctx); err != nil {
			t.Fatal(err)
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 || relay.active.Load() != 2 || relay.packets.Load() == 0 {
		t.Fatal("UDP routes not independently observed", firstCalls.Load(), secondCalls.Load(), relay.active.Load(), relay.packets.Load())
	}
	if configured && queries.Load() != 4 || !configured && queries.Load() != 0 {
		t.Fatal("response-dispatch DNS bypassed explicit source authority", queries.Load())
	}
	badProxy := &url.URL{Scheme: "socks5", Host: relay.address, User: url.UserPassword("alpha", "incorrect")}
	receipt, err := fixture.client.Do(ctx, ctx, fault.Correlation{Call: "denied"}, request(t, "GET", first, nil), RequestOptionsV1{Proxy: ProxyAddress, ProxyURL: badProxy.String()})
	if err == nil {
		t.Fatal("proxy authentication failure silently succeeded")
	}
	settle(t, fixture, receipt)
	if relay.denied.Load() != 1 || firstCalls.Load() != 1 {
		t.Fatal("proxy failure fell back direct", relay.denied.Load(), firstCalls.Load())
	}
	if err := fixture.assembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
	fixture.client.owner.mu.Lock()
	sockets, quic := fixture.client.owner.connections, fixture.client.owner.quicActive
	fixture.client.owner.mu.Unlock()
	if sockets != 0 || quic != 0 {
		t.Fatal("UDP route retained physical resources", sockets, quic)
	}
}
