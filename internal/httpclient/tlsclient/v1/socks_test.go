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

package tlsclient

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/bogdanfinn/tls-client"
)

type udpProxyPeer struct {
	url           string
	packets       atomic.Int64
	authenticated atomic.Bool
	closed        chan struct{}
}

func socksUDPRelay(t *testing.T, target *net.UDPAddr) *udpProxyPeer {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	relay, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	peer := &udpProxyPeer{url: "socks5://user:pass@" + listener.Addr().String(), closed: make(chan struct{})}
	var mu sync.Mutex
	var control net.Conn
	udpDone := make(chan struct{})
	header := []byte{0, 0, 0, 1}
	header = append(header, target.IP.To4()...)
	header = binary.BigEndian.AppendUint16(header, uint16(target.Port))
	go func() {
		defer close(udpDone)
		data := make([]byte, 65535)
		var client *net.UDPAddr
		for {
			count, address, err := relay.ReadFromUDP(data)
			if err != nil {
				return
			}
			if address.Port == target.Port && address.IP.Equal(target.IP) {
				if client != nil {
					packet := append(append([]byte{}, header...), data[:count]...)
					_, _ = relay.WriteToUDP(packet, client)
				}
				continue
			}
			if count < 10 || string(data[:10]) != string(header) || !address.IP.IsLoopback() {
				continue
			}
			if client != nil && (client.Port != address.Port || !client.IP.Equal(address.IP)) {
				continue
			}
			client = address
			peer.packets.Add(1)
			_, _ = relay.WriteToUDP(data[10:count], target)
		}
	}()
	go func() {
		defer close(peer.closed)
		defer relay.Close()
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		mu.Lock()
		control = conn
		mu.Unlock()
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		hello := make([]byte, 2)
		if _, err = io.ReadFull(conn, hello); err != nil || hello[0] != 5 {
			return
		}
		methods := make([]byte, int(hello[1]))
		if _, err = io.ReadFull(conn, methods); err != nil {
			return
		}
		if _, err = conn.Write([]byte{5, 2}); err != nil {
			return
		}
		if _, err = io.ReadFull(conn, hello); err != nil || hello[0] != 1 {
			return
		}
		username := make([]byte, int(hello[1]))
		if _, err = io.ReadFull(conn, username); err != nil {
			return
		}
		size := make([]byte, 1)
		if _, err = io.ReadFull(conn, size); err != nil {
			return
		}
		password := make([]byte, int(size[0]))
		if _, err = io.ReadFull(conn, password); err != nil {
			return
		}
		if string(username) != "user" || string(password) != "pass" {
			_, _ = conn.Write([]byte{1, 1})
			return
		}
		peer.authenticated.Store(true)
		if _, err = conn.Write([]byte{1, 0}); err != nil {
			return
		}
		request := make([]byte, 4)
		if _, err = io.ReadFull(conn, request); err != nil || request[0] != 5 || request[1] != 3 {
			return
		}
		length := 6
		if request[3] == 4 {
			length = 18
		} else if request[3] != 1 {
			return
		}
		if _, err = io.CopyN(io.Discard, conn, int64(length)); err != nil {
			return
		}
		response := []byte{5, 0, 0, 1, 127, 0, 0, 1}
		response = binary.BigEndian.AppendUint16(response, uint16(relay.LocalAddr().(*net.UDPAddr).Port))
		if _, err = conn.Write(response); err != nil {
			return
		}
		_ = conn.SetDeadline(time.Time{})
		_, _ = io.Copy(io.Discard, conn)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		conn := control
		mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
		_ = relay.Close()
		for _, done := range []<-chan struct{}{peer.closed, udpDone} {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("local SOCKS relay did not terminate")
			}
		}
	})
	return peer
}
func TestProviderH3AuthenticatedSOCKSRelayAndCleanup(t *testing.T) {
	peers := newNativePeers(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("TCP request unexpectedly won")
		w.WriteHeader(500)
	}, func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "proxied-h3") })
	target, err := net.ResolveUDPAddr("udp4", peers.tcp.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	proxy := socksUDPRelay(t, target)
	options := providerOptions()
	options.Mode = HTTP3Racing
	options.ProxyURL = proxy.url
	options.Native.Transport = &sdk.TransportOptions{RootCAs: peers.roots}
	fixture := bindProvider(t, options, 1)
	before := socketIdentities(t)
	receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("udp"), providerRequest(t, "GET", peers.tcp.URL, nil))
	if err != nil {
		t.Fatal("local SOCKS H3 request failed", err)
	}
	result := settleProvider(t, fixture, receipt)
	if !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "proxied-h3" || result.Outcome.Value.Metadata().Protocol() != "HTTP/3.0" || proxy.packets.Load() == 0 || !proxy.authenticated.Load() {
		t.Fatal("native H3 bypassed authenticated relay")
	}
	var owned []string
	for socket := range socketIdentities(t) {
		if !before[socket] {
			owned = append(owned, socket)
		}
	}
	if runtime.GOOS == "linux" && len(owned) < 2 {
		t.Fatal("test did not observe SOCKS control and UDP sockets")
	}
	if err := fixture.assembly.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-proxy.closed:
	case <-time.After(time.Second):
		t.Fatal("SOCKS control survived source close")
	}
	until := time.Now().Add(time.Second)
	for {
		after := socketIdentities(t)
		remaining := false
		for _, socket := range owned {
			remaining = remaining || after[socket]
		}
		if !remaining {
			break
		}
		if time.Now().After(until) {
			t.Fatal("source retained observed SOCKS control or UDP socket")
		}
		time.Sleep(time.Millisecond)
	}
	fixture.client.owner.mu.Lock()
	defer fixture.client.owner.mu.Unlock()
	if fixture.client.owner.h3 != 0 || fixture.client.owner.tcp != 0 {
		t.Fatal("SOCKS H3 native quota not released")
	}
}
func TestProviderH3RemoteDNSIsNotSilentlyBypassed(t *testing.T) {
	options := providerOptions()
	options.Mode = HTTP3Racing
	options.ProxyURL = "socks5h://127.0.0.1:1"
	fixture := bindProvider(t, options, 1)
	receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("dns"), providerRequest(t, "GET", "https://unused.invalid", nil))
	if receipt != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatal("socks5h promised remote DNS while native H3 resolves locally", err)
	}
	if !strings.Contains(err.Error(), "unsupported") {
		t.Fatal("wrong restriction class", err)
	}
}
