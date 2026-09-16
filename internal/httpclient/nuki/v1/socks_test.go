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

package nuki

import (
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	nativetls "github.com/nukilabs/utls"
	"github.com/quic-go/quic-go/http3"
)

func socksRelay(t *testing.T, target *net.UDPAddr) (string, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	relay, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	closed, done := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var control net.Conn
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		defer close(closed)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		mu.Lock()
		control = conn
		mu.Unlock()
		defer conn.Close()
		var greeting [2]byte
		if _, err := io.ReadFull(conn, greeting[:]); err != nil {
			return
		}
		methods := make([]byte, int(greeting[1]))
		if _, err := io.ReadFull(conn, methods); err != nil {
			return
		}
		if _, err := conn.Write([]byte{5, 0}); err != nil {
			return
		}
		var prefix [4]byte
		if _, err := io.ReadFull(conn, prefix[:]); err != nil {
			return
		}
		if prefix[0] != 5 || prefix[1] != 3 {
			t.Error("unexpected SOCKS command")
			return
		}
		addressBytes := 0
		switch prefix[3] {
		case 1:
			addressBytes = 4
		case 4:
			addressBytes = 16
		case 3:
			var size [1]byte
			if _, err := io.ReadFull(conn, size[:]); err != nil {
				return
			}
			addressBytes = int(size[0])
		default:
			t.Error("invalid SOCKS address")
			return
		}
		if _, err := io.CopyN(io.Discard, conn, int64(addressBytes+2)); err != nil {
			return
		}
		port := relay.LocalAddr().(*net.UDPAddr).Port
		if _, err := conn.Write([]byte{5, 0, 0, 1, 127, 0, 0, 1, byte(port >> 8), byte(port)}); err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, conn)
	}()
	go func() {
		defer workers.Done()
		var client *net.UDPAddr
		buffer := make([]byte, 65535)
		for {
			count, from, err := relay.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			if from.Port == target.Port && from.IP.Equal(target.IP) {
				if client == nil {
					t.Error("response without relay client")
					return
				}
				header := []byte{0, 0, 0, 1, 127, 0, 0, 1, byte(target.Port >> 8), byte(target.Port)}
				_, _ = relay.WriteToUDP(append(header, buffer[:count]...), client)
				continue
			}
			if count < 10 || buffer[3] != 1 || !net.IP(buffer[4:8]).Equal(target.IP) || int(binary.BigEndian.Uint16(buffer[8:10])) != target.Port {
				t.Error("relay target changed")
				return
			}
			client = from
			_, _ = relay.WriteToUDP(buffer[10:count], target)
		}
	}()
	go func() { workers.Wait(); close(done) }()
	t.Cleanup(func() {
		listener.Close()
		relay.Close()
		mu.Lock()
		conn := control
		mu.Unlock()
		if conn != nil {
			conn.Close()
		}
		select {
		case <-done:
		case <-testContext(t).Done():
			t.Error("relay cleanup did not join")
		}
	})
	return "socks5://" + listener.Addr().String(), closed
}

func TestProviderSOCKSH3OwnsControlAndDatagramSockets(t *testing.T) {
	for _, limit := range []int{1, 4} {
		certificate := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		defer certificate.Close()
		packet, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		peer := &http3.Server{TLSConfig: &tls.Config{Certificates: certificate.TLS.Certificates},
			Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, "socks-h3") })}
		done := make(chan error, 1)
		go func() { done <- peer.Serve(packet) }()
		defer func() { peer.Close(); packet.Close(); <-done }()
		proxy, closed := socksRelay(t, packet.LocalAddr().(*net.UDPAddr))
		options := providerOptions()
		options.Mode = HTTP3Only
		options.ProxyURL = proxy
		options.MaxConnections = limit
		options.Native.TLS = &nativetls.Config{InsecureSkipVerify: true}
		fixture := bindProvider(t, options)
		receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "socks"}, nativeRequest(t, "GET", "https://"+packet.LocalAddr().String(), nil))
		if err != nil {
			t.Fatal(err)
		}
		result := outcome(t, fixture, receipt)
		if limit == 1 {
			if !errors.Is(result.Err(), ErrCapacity) {
				t.Fatal("physical socket quota bypassed", result.Err())
			}
		} else if result.Err() != nil || string(result.Outcome.Value.DataCopy()) != "socks-h3" || !result.Outcome.Value.Complete() {
			t.Fatal("native SOCKS/H3 failed", result.Err())
		}
		if err := fixture.assembly.Close(testContext(t)); err != nil {
			t.Fatal("owned cleanup", err)
		}
		select {
		case <-closed:
		case <-testContext(t).Done():
			t.Fatal("SOCKS control connection survived shutdown")
		}
	}
}
