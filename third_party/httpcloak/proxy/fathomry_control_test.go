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

package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	official "github.com/quic-go/quic-go"
	officialh3 "github.com/quic-go/quic-go/http3"
	nativehttp "github.com/sardanioss/http"
	"github.com/sardanioss/quic-go"
	utls "github.com/sardanioss/utls"
)

type masqueControlPeer struct {
	ctx    context.Context
	conn   *MASQUEConn
	stream *officialh3.Stream
}

func newMASQUEControlPeer(t *testing.T, target string) *masqueControlPeer {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	certificate := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	certificates := certificate.TLS.Certificates
	roots := x509.NewCertPool()
	roots.AddCert(certificate.Certificate())
	certificate.Close()
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan *officialh3.Stream, 1)
	server := &officialh3.Server{TLSConfig: &tls.Config{Certificates: certificates}, EnableDatagrams: true, Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == "GET" && request.URL.Path == "/healthy" {
			_, _ = io.WriteString(writer, "healthy")
			return
		}
		if request.Method != "CONNECT" || request.Proto != "connect-udp" || request.URL.EscapedPath() != "/.well-known/masque/udp/"+url.QueryEscape(target)+"/443/" {
			t.Error("native CONNECT-UDP method or escaped target changed")
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		writer.Header().Set("Capsule-Protocol", "?1; future=\"ignored\"; flag")
		writer.WriteHeader(http.StatusOK)
		stream := writer.(officialh3.HTTPStreamer).HTTPStream()
		accepted <- stream
		<-request.Context().Done()
	})}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(packet) }()
	t.Cleanup(func() { _ = server.Close(); _ = packet.Close(); <-done })
	conn, err := NewMASQUEConn("masque://" + packet.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := conn.EstablishWithQUICConfig(ctx, target, 443, &utls.Config{RootCAs: roots, NextProtos: []string{"h3"}}, &quic.Config{EnableDatagrams: true, MaxIdleTimeout: 3 * time.Second}); err != nil {
		t.Fatal(err)
	}
	select {
	case stream := <-accepted:
		return &masqueControlPeer{ctx, conn, stream}
	case <-ctx.Done():
		t.Fatal("independent MASQUE peer did not receive CONNECT-UDP")
		return nil
	}
}

func (peer *masqueControlPeer) assertHealthySibling(t *testing.T) {
	t.Helper()
	stream, err := peer.conn.clientConn.OpenRequestStream(peer.ctx)
	if err != nil {
		t.Fatal("control termination closed outer QUIC", err)
	}
	input := &nativehttp.Request{Method: "GET", URL: &url.URL{Scheme: "https", Host: peer.conn.quicConn.RemoteAddr().String(), Path: "/healthy"}, Header: make(nativehttp.Header)}
	if err := stream.SendRequestHeader(input); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	response, err := stream.ReadResponse()
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || string(data) != "healthy" || peer.conn.quicConn.Context().Err() != nil {
		t.Fatal("healthy sibling changed after owning CONNECT termination", string(data), err)
	}
}

func TestFathomryMASQUEControlEOFResetAndCapsules(t *testing.T) {
	for _, mode := range []string{"fin", "reset", "truncated-capsule", "missing-context", "truncated-context", "empty-udp", "capsule-datagram", "unknown-capsule"} {
		t.Run(mode, func(t *testing.T) {
			peer := newMASQUEControlPeer(t, "2001:db8::42")
			type result struct {
				data string
				err  error
			}
			returned := make(chan result, 1)
			go func() {
				buffer := make([]byte, 32)
				count, _, err := peer.conn.ReadFrom(buffer)
				returned <- result{string(buffer[:count]), err}
			}()
			switch mode {
			case "fin":
				if err := peer.stream.Close(); err != nil {
					t.Fatal(err)
				}
			case "reset":
				peer.stream.CancelWrite(official.StreamErrorCode(0x123))
			case "truncated-capsule":
				_, _ = peer.stream.Write([]byte{0, 3, 0, 'x'})
				_ = peer.stream.Close()
			case "missing-context":
				_, _ = peer.stream.Write([]byte{0, 0})
			case "truncated-context":
				_, _ = peer.stream.Write([]byte{0, 1, 0x40})
			case "empty-udp":
				_, _ = peer.stream.Write([]byte{0, 1, 0})
			case "unknown-capsule":
				if _, err := peer.stream.Write([]byte{0x27, 3, 's', 'k', 'p', 0, 4, 1, 'b', 'a', 'd'}); err != nil {
					t.Fatal(err)
				}
				fallthrough
			case "capsule-datagram":
				if _, err := peer.stream.Write([]byte{0, 3, 0, 'o', 'k'}); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case result := <-returned:
				if mode == "capsule-datagram" || mode == "unknown-capsule" || mode == "empty-udp" {
					wanted := "ok"
					if mode == "empty-udp" {
						wanted = ""
					}
					if result.err != nil || result.data != wanted {
						t.Fatal("valid capsule datagram or unknown-context discard changed", result)
					}
				} else {
					if result.err == nil || result.data != "" {
						t.Fatal("terminal CONNECT stream did not end packet reads", result)
					}
					if (mode == "truncated-capsule" || mode == "missing-context" || mode == "truncated-context") && !errors.Is(result.err, io.ErrUnexpectedEOF) {
						t.Fatal("truncated capsule became clean EOF", result.err)
					}
					peer.assertHealthySibling(t)
				}
			case <-time.After(time.Second):
				t.Fatal("CONNECT stream ownership did not reach entered packet read")
			}
		})
	}
}

type pausedWriteContext struct {
	context.Context
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (ctx *pausedWriteContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered); <-ctx.release })
	return ctx.Context.Done()
}

func TestFathomryMASQUEEnteredWriteDeadlineReplacementAndClose(t *testing.T) {
	for _, mode := range []string{"expire", "clear", "extend", "close"} {
		t.Run(mode, func(t *testing.T) {
			peer := newMASQUEControlPeer(t, "127.0.0.1")
			_ = peer.conn.SetWriteDeadline(time.Now().Add(time.Minute))
			paused := &pausedWriteContext{Context: peer.conn.writeContext, entered: make(chan struct{}), release: make(chan struct{})}
			resume := sync.OnceFunc(func() { close(paused.release) })
			defer resume()
			peer.conn.writeContext = paused
			returned := make(chan error, 1)
			go func() { _, err := peer.conn.WriteTo([]byte("sent"), nil); returned <- err }()
			select {
			case <-paused.entered:
			case <-peer.ctx.Done():
				t.Fatal("MASQUE write did not enter its captured deadline context")
			}
			closed := make(chan error, 1)
			switch mode {
			case "expire":
				_ = peer.conn.SetWriteDeadline(time.Now().Add(-time.Second))
			case "clear":
				_ = peer.conn.SetWriteDeadline(time.Time{})
			case "extend":
				_ = peer.conn.SetWriteDeadline(time.Now().Add(2 * time.Minute))
			case "close":
				go func() { closed <- peer.conn.Close() }()
				<-peer.conn.ctx.Done()
				select {
				case err := <-closed:
					t.Fatal("Close returned before entered native WriteTo left", err)
				default:
				}
			}
			resume()
			select {
			case err := <-returned:
				if mode == "expire" && !errors.Is(err, os.ErrDeadlineExceeded) || mode == "close" && err == nil || (mode == "clear" || mode == "extend") && err != nil {
					t.Fatal("entered write ignored changed deadline or source state", mode, err)
				}
			case <-time.After(time.Second):
				t.Fatal("entered write did not release after deadline/source update")
			}
			if mode == "close" {
				select {
				case err := <-closed:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("Close failed to join entered packet write")
				}
				return
			}
			if mode == "expire" {
				_ = peer.conn.SetWriteDeadline(time.Time{})
				if _, err := peer.conn.WriteTo([]byte("sent"), nil); err != nil {
					t.Fatal("cleared deadline did not restore legal writes", err)
				}
			}
			data, err := peer.stream.ReceiveDatagram(peer.ctx)
			if err != nil || string(data) != "\x00sent" {
				t.Fatal("healthy wire datagram changed after deadline update", data, err)
			}
			peer.assertHealthySibling(t)
		})
	}
}
