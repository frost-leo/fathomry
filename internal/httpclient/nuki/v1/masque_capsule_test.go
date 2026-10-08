// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package nuki

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	nativequic "github.com/nukilabs/quic-go"
	nativeh3 "github.com/nukilabs/quic-go/http3"
	nativeproxy "github.com/nukilabs/tlsclient/proxy"
	nativetls "github.com/nukilabs/utls"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
)

func capsuleBytes(kind uint64, data []byte) []byte {
	wire := quicvarint.Append(nil, kind)
	wire = quicvarint.Append(wire, uint64(len(data)))
	return append(wire, data...)
}

func TestMASQUECapsuleAndDatagramIngress(t *testing.T) {
	for _, negotiation := range []string{"?1", `?1; opaque="ignored"; number=7; flag`} {
		t.Run(negotiation, func(t *testing.T) {
			certificate, roots := masqueCertificate(t, "proxy-capsules")
			var workers sync.WaitGroup
			endpoint, stop := masqueServer(t, certificate, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				workers.Add(1)
				defer workers.Done()
				w.Header().Set(http3.CapsuleProtocolHeader, negotiation)
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				stream := w.(http3.HTTPStreamer).HTTPStream()
				if err := stream.SendDatagram([]byte{0, 'q', 'u', 'i', 'c'}); err != nil {
					return
				}
				unknown := capsuleBytes(0x123, bytes.Repeat([]byte{'x'}, 128<<10))
				unknown = append(unknown, capsuleBytes(0, []byte{7, 'u', 'n', 'k', 'n', 'o', 'w', 'n'})...)
				if _, err := stream.Write(unknown); err != nil {
					return
				}
				for _, payload := range [][]byte{{0, 'c', 'a', 'p', 's', 'u', 'l', 'e'}, {0}, {0x40, 0, 'w', 'i', 'd', 'e'}} {
					if _, err := stream.Write(capsuleBytes(0, payload)); err != nil {
						return
					}
				}
				_, _ = io.Copy(io.Discard, stream)
				_ = stream.Close()
			}))
			defer stop()
			address, _ := url.Parse(endpoint + "/udp/{target_host}/{target_port}/")
			dialer, err := nativeproxy.NewWithDialer(address, time.Second, &nativetls.Config{RootCAs: roots}, nativeproxy.Direct(nil, time.Second), nil, 65536)
			if err != nil {
				t.Fatal(err)
			}
			defer dialer.(io.Closer).Close()
			tunnel, err := dialer.ListenPacket(testContext(t), "udp", "127.0.0.1:443")
			if err != nil {
				t.Fatal("legal capsule negotiation refused", err)
			}
			defer tunnel.Close()
			if err := tunnel.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			seen := make(map[string]bool)
			for range 4 {
				var data [64]byte
				count, remote, err := tunnel.ReadFrom(data[:])
				if err != nil || remote == nil || remote.String() != "127.0.0.1:443" {
					t.Fatal("capsule/QUIC ingress was not delivered", count, remote, err)
				}
				seen[string(data[:count])] = true
			}
			if len(seen) != 4 || !seen["quic"] || !seen["capsule"] || !seen[""] || !seen["wide"] {
				t.Fatal("unknown capsule/context leaked or context-zero data disappeared", seen)
			}
			if err := tunnel.Close(); err != nil {
				t.Fatal(err)
			}
			if err := dialer.(io.Closer).Close(); err != nil {
				t.Fatal(err)
			}
			stop()
			workers.Wait()
		})
	}
}

func TestMASQUECapsuleTerminationPreservesCauseAndSibling(t *testing.T) {
	for _, mode := range []string{"eof", "type-truncated", "length-truncated", "value-truncated", "missing-context", "context-truncated", "unknown-truncated", "oversized", "reset"} {
		t.Run(mode, func(t *testing.T) {
			certificate, roots := masqueCertificate(t, "proxy-capsule-end")
			var workers sync.WaitGroup
			var identityMu sync.Mutex
			var peerIdentity string
			established := make(chan struct{})
			endpoint, stop := masqueServer(t, certificate, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				workers.Add(1)
				defer workers.Done()
				identityMu.Lock()
				if peerIdentity == "" {
					peerIdentity = r.RemoteAddr
				} else if peerIdentity != r.RemoteAddr {
					t.Error("stream failure changed the healthy outer UDP peer tuple")
				}
				identityMu.Unlock()
				w.Header().Set(http3.CapsuleProtocolHeader, "?1")
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				stream := w.(http3.HTTPStreamer).HTTPStream()
				if strings.HasSuffix(r.URL.Path, "/444/") {
					_ = stream.SendDatagram([]byte{0, 'o', 'k'})
				} else {
					select {
					case <-established:
					case <-r.Context().Done():
						return
					}
					var prefix []byte
					switch mode {
					case "type-truncated":
						prefix = []byte{0x40}
					case "length-truncated":
						prefix = []byte{0, 0x40}
					case "value-truncated":
						prefix = []byte{0, 4, 0, 'x'}
					case "missing-context":
						prefix = []byte{0, 0}
					case "context-truncated":
						prefix = []byte{0, 1, 0x40}
					case "unknown-truncated":
						prefix = []byte{0x20, 4, 'x'}
					case "oversized":
						prefix = append(quicvarint.Append([]byte{0}, 65509), 0)
					}
					if len(prefix) > 0 {
						_, _ = stream.Write(prefix)
					}
					if mode == "reset" {
						stream.CancelWrite(0x121)
					} else if mode != "oversized" {
						_ = stream.Close()
					}
				}
				_, _ = io.Copy(io.Discard, stream)
				_ = stream.Close()
			}))
			defer stop()
			address, _ := url.Parse(endpoint + "/udp/{target_host}/{target_port}/")
			dialer, err := nativeproxy.NewWithDialer(address, time.Second, &nativetls.Config{RootCAs: roots}, nativeproxy.Direct(nil, time.Second), nil, 65536)
			if err != nil {
				t.Fatal(err)
			}
			defer dialer.(io.Closer).Close()
			first, err := dialer.ListenPacket(testContext(t), "udp", "127.0.0.1:443")
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			close(established)
			if err := first.SetReadDeadline(time.Now().Add(time.Second)); err != nil && mode == "reset" {
				var reset *nativeh3.Error
				if !errors.As(err, &reset) || !reset.Remote || reset.ErrorCode != 0x121 {
					t.Fatal("terminal deadline update lost stream reset", err)
				}
			}
			var data [64]byte
			_, _, err = first.ReadFrom(data[:])
			switch mode {
			case "eof":
				if !errors.Is(err, io.EOF) {
					t.Fatal("clean control EOF lost its meaning", err)
				}
			case "reset":
				var reset *nativeh3.Error
				if !errors.As(err, &reset) || !reset.Remote || reset.ErrorCode != 0x121 {
					t.Fatal("peer reset cause lost", err)
				}
			case "oversized":
				var limit *nativequic.DatagramTooLargeError
				if !errors.As(err, &limit) || limit.MaxDatagramPayloadSize != 65507 {
					t.Fatal("oversized capsule acquired its absent body instead of refusing its declared bound", err)
				}
			default:
				if !errors.Is(err, io.ErrUnexpectedEOF) {
					t.Fatal("malformed capsule was mistaken for normal EOF", err)
				}
			}
			if err := first.Close(); err != nil {
				t.Fatal("observed peer failure was confused with cleanup failure", err)
			}
			second, err := dialer.ListenPacket(testContext(t), "udp", "127.0.0.1:444")
			if err != nil {
				t.Fatal("sibling admission failed", err)
			}
			defer second.Close()
			_ = second.SetReadDeadline(time.Now().Add(time.Second))
			count, _, err := second.ReadFrom(data[:])
			if err != nil || string(data[:count]) != "ok" {
				t.Fatal("control termination harmed sibling datagrams", count, err)
			}
			if _, err := second.WriteTo([]byte("still-open"), &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 444}); err != nil {
				t.Fatal("sibling send ended", err)
			}
			_ = second.Close()
			_ = dialer.(io.Closer).Close()
			stop()
			workers.Wait()
		})
	}
}

func TestMASQUECapsuleNegotiationRefusesBeforeTunnelTransfer(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		status int
		fields http.Header
	}{
		{"false", 200, http.Header{http3.CapsuleProtocolHeader: {"?0"}}},
		{"duplicate", 200, http.Header{http3.CapsuleProtocolHeader: {"?1", "?1"}}},
		{"malformed", 200, http.Header{http3.CapsuleProtocolHeader: {`?1; broken="`}}},
		{"length", 200, http.Header{"Content-Length": {"0"}}},
		{"type", 200, http.Header{"Content-Type": {"application/octet-stream"}}},
		{"no-content", 204, nil},
		{"reset-content", 205, nil},
		{"partial-content", 206, nil},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			certificate, roots := masqueCertificate(t, "proxy-negotiation")
			var workers sync.WaitGroup
			var requests, active atomic.Int32
			endpoint, stop := masqueServer(t, certificate, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				workers.Add(1)
				defer workers.Done()
				w.Header().Set(http3.CapsuleProtocolHeader, "?1")
				for name, values := range scenario.fields {
					w.Header()[name] = values
				}
				w.WriteHeader(scenario.status)
				w.(http.Flusher).Flush()
				stream := w.(http3.HTTPStreamer).HTTPStream()
				_, _ = io.Copy(io.Discard, stream)
				_ = stream.Close()
			}))
			defer stop()
			address, _ := url.Parse(endpoint + "/udp/{target_host}/{target_port}/")
			dialer, err := nativeproxy.NewWithDialer(address, time.Second, &nativetls.Config{RootCAs: roots}, nativeproxy.Direct(nil, time.Second), nil, 65536)
			if err != nil {
				t.Fatal(err)
			}
			defer dialer.(io.Closer).Close()
			dialer.(*nativeproxy.Dialer).ConfigureFathomry(nil, func() (func(), error) {
				active.Add(1)
				return sync.OnceFunc(func() { active.Add(-1) }), nil
			})
			tunnel, err := dialer.ListenPacket(testContext(t), "udp", "127.0.0.1:443")
			if tunnel != nil {
				_ = tunnel.Close()
			}
			if err == nil || tunnel != nil || requests.Load() != 1 || active.Load() != 0 {
				t.Fatal("invalid response transferred a tunnel or retained admission", err, tunnel != nil, requests.Load(), active.Load())
			}
			if err := dialer.(io.Closer).Close(); err != nil {
				t.Fatal(err)
			}
			stop()
			workers.Wait()
		})
	}
}
