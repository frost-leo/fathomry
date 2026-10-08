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

package surf

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enetx/surf/profiles/chrome"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	peerh2 "golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type reviewBehaviorInput struct {
	reader *strings.Reader
	reads  atomic.Int64
	closes atomic.Int64
}

func (input *reviewBehaviorInput) Read(buffer []byte) (int, error) {
	input.reads.Add(1)
	return input.reader.Read(buffer)
}

func (input *reviewBehaviorInput) Close() error {
	input.closes.Add(1)
	return nil
}

type reviewBehaviorConn struct {
	net.Conn
	once   sync.Once
	closed chan struct{}
}

func (conn *reviewBehaviorConn) Close() error {
	err := conn.Conn.Close()
	conn.once.Do(func() { close(conn.closed) })
	return err
}

type reviewBehaviorSockets struct {
	mu    sync.Mutex
	conns []*reviewBehaviorConn
}

func (sockets *reviewBehaviorSockets) dial(ctx context.Context, network, address string) (net.Conn, error) {
	raw, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	conn := &reviewBehaviorConn{Conn: raw, closed: make(chan struct{})}
	sockets.mu.Lock()
	sockets.conns = append(sockets.conns, conn)
	sockets.mu.Unlock()
	return conn, nil
}

func (sockets *reviewBehaviorSockets) snapshot() []*reviewBehaviorConn {
	sockets.mu.Lock()
	defer sockets.mu.Unlock()
	return append([]*reviewBehaviorConn(nil), sockets.conns...)
}

func reviewBehaviorWait(t *testing.T, description string, ready func() bool) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !ready() {
		select {
		case <-deadline.C:
			t.Fatal(description)
		case <-tick.C:
		}
	}
}

func reviewBehaviorClosed(t *testing.T, closed <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal(description)
	}
}

func reviewBehaviorAlive(t *testing.T, closed <-chan struct{}, duration time.Duration, description string) {
	t.Helper()
	select {
	case <-closed:
		t.Fatal(description)
	case <-time.After(duration):
	}
}

func reviewBehaviorReleased(t *testing.T, fix *fixture, sockets *reviewBehaviorSockets) {
	t.Helper()
	if err := fix.assembly.Close(testContext(t)); err != nil {
		t.Fatal("source close", err)
	}
	status := fix.assembly.Snapshot().Sources[0]
	if !status.Released || !status.Quiescent || status.Usage != (resource.Usage{}) || fix.inbox.Usage() != (invocation.InboxUsage{}) {
		t.Fatal("source or evidence accounting remained occupied after release")
	}
	fix.client.owner.mu.Lock()
	tcp, udp, h3 := fix.client.owner.tcp, fix.client.owner.udp, fix.client.owner.h3
	fix.client.owner.mu.Unlock()
	if tcp != 0 || udp != 0 || h3 != 0 {
		t.Fatalf("native residence survived release: TCP=%d UDP=%d H3=%d", tcp, udp, h3)
	}
	if sockets != nil {
		for _, conn := range sockets.snapshot() {
			reviewBehaviorClosed(t, conn.closed, "dialed socket survived source release")
		}
	}
}

func TestAdmissionTimeoutOwnsOnlyInternalQueue(t *testing.T) {
	for _, releaseBeforeDeadline := range []bool{false, true} {
		name := "deadline-rejects-unadmitted-input"
		if releaseBeforeDeadline {
			name = "capacity-release-admits-queued-input"
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int64
			peer := httptest.NewServer(stdhttp.HandlerFunc(func(writer stdhttp.ResponseWriter, request *stdhttp.Request) {
				calls.Add(1)
				body, err := io.ReadAll(request.Body)
				if err != nil || request.Method == "POST" && string(body) != "queued input" {
					t.Error("peer request body", err)
				}
				_, _ = io.WriteString(writer, "ok")
			}))
			t.Cleanup(peer.Close)
			var sockets reviewBehaviorSockets
			fix := newFixture(t, OptionsV1{Name: "admission-behavior", Mode: HTTP1Only, MaxActive: 1, QueuedCalls: 1,
				AdmissionTimeout: 200 * time.Millisecond, Native: NativeOptionsV1{DialContext: sockets.dial}}, 2)
			stream, first, err := fix.client.Open(testContext(t), fault.Correlation{Call: "retained"}, request(t, "GET", peer.URL, ""))
			if err != nil || stream == nil {
				t.Fatal("initial admission", err)
			}
			t.Cleanup(func() { _ = stream.Close(testContext(t)) })
			if body, err := io.ReadAll(stream); err != nil || string(body) != "ok" {
				t.Fatal("initial complete stream", err)
			}
			input := &reviewBehaviorInput{reader: strings.NewReader("queued input")}
			queued := request(t, "POST", peer.URL, "")
			queued.Body, queued.GetBody, queued.ContentLength = input, nil, int64(len("queued input"))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			type completed struct {
				receipt *invocation.Receipt[Result]
				err     error
			}
			done := make(chan completed, 1)
			go func() {
				receipt, err := fix.client.Do(ctx, ctx, fault.Correlation{Call: "queued"}, queued)
				done <- completed{receipt, err}
			}()
			joined := false
			t.Cleanup(func() {
				cancel()
				if !joined {
					select {
					case <-done:
					case <-time.After(2 * time.Second):
						t.Error("queued caller did not terminate")
					}
				}
			})
			reviewBehaviorWait(t, "second call never occupied the Internal queue", func() bool {
				usage := fix.assembly.Snapshot().Sources[0].Usage
				return usage.Active == 1 && usage.Queued == 1
			})
			if fix.inbox.Usage().Outstanding != 2 || calls.Load() != 1 || input.reads.Load() != 0 || input.closes.Load() != 0 {
				t.Fatal("queue wait did not reserve evidence, or entered/closed unadmitted input")
			}
			if releaseBeforeDeadline {
				if err := stream.Close(testContext(t)); err != nil {
					t.Fatal(err)
				}
				settle(t, fix, first)
			}
			var result completed
			select {
			case result = <-done:
				joined = true
			case <-time.After(2 * time.Second):
				t.Fatal("explicit admission budget did not resolve the queued caller")
			}
			if ctx.Err() != nil {
				t.Fatal("method context, rather than the configured admission budget, expired")
			}
			if !releaseBeforeDeadline {
				usage := fix.assembly.Snapshot().Sources[0].Usage
				if result.receipt != nil || !errors.Is(result.err, context.DeadlineExceeded) ||
					usage.Active != 1 || usage.Queued != 0 || usage.QueuedBytes != 0 || fix.inbox.Usage().Outstanding != 1 ||
					calls.Load() != 1 || len(sockets.snapshot()) != 1 || input.reads.Load() != 0 || input.closes.Load() != 0 {
					t.Fatal("admission timeout lost its cause, admitted input, or retained queue/evidence capacity", result.err)
				}
				if err := stream.Close(testContext(t)); err != nil {
					t.Fatal(err)
				}
				settle(t, fix, first)
				result.receipt, result.err = fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "recovery"}, queued)
			}
			if result.err != nil || result.receipt == nil {
				t.Fatal("capacity release did not admit unchanged input", result.err)
			}
			final := settle(t, fix, result.receipt)
			if final.Err() != nil || !final.Outcome.Value.Complete() || calls.Load() != 2 || input.reads.Load() == 0 || input.closes.Load() != 1 {
				t.Fatal("admitted input or independent result did not release exactly once")
			}
			reviewBehaviorReleased(t, fix, &sockets)
		})
	}
}

func reviewBehaviorPeer(t *testing.T, protocol string, handler stdhttp.HandlerFunc) (string, OptionsV1, *peers) {
	t.Helper()
	options := OptionsV1{Name: "native-options"}
	if protocol == "h2c" {
		peer := httptest.NewServer(h2c.NewHandler(handler, &peerh2.Server{}))
		t.Cleanup(peer.Close)
		options.Mode = H2C
		return peer.URL, options, nil
	}
	peer := newPeers(t, handler, protocol == "h3")
	options.Native.TLSConfig = &tls.Config{RootCAs: peer.roots}
	switch protocol {
	case "h1", "ja-h1":
		options.Mode = HTTP1Only
	case "h2", "ja-h2":
		options.Mode = HTTP2Only
	case "h3":
		options.Mode = PreferHTTP3
	default:
		t.Fatal("unknown protocol fixture", protocol)
	}
	if strings.HasPrefix(protocol, "ja-") {
		profile := chrome.Desktop
		options.Native.Profile = &profile
	}
	return peer.tcp.URL, options, peer
}

func reviewBehaviorProtocol(protocol string) string {
	switch protocol {
	case "h1", "ja-h1":
		return "HTTP/1.1"
	case "h3":
		return "HTTP/3.0"
	default:
		return "HTTP/2.0"
	}
}

func TestIdleConnTimeoutRetainsActiveThenReleasesIdleTCP(t *testing.T) {
	const shortIdle = 80 * time.Millisecond
	for _, protocol := range []string{"h1", "h2", "ja-h1", "ja-h2", "h2c"} {
		for _, expire := range []bool{false, true} {
			name := "long-reuses-idle"
			if expire {
				name = "short-releases-idle-not-active"
			}
			t.Run(protocol+"/"+name, func(t *testing.T) {
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				url, options, _ := reviewBehaviorPeer(t, protocol, func(writer stdhttp.ResponseWriter, request *stdhttp.Request) {
					if request.URL.Path == "/hold" {
						writer.Header().Set("Content-Length", "2")
						_, _ = io.WriteString(writer, "o")
						writer.(stdhttp.Flusher).Flush()
						select {
						case <-release:
							_, _ = io.WriteString(writer, "k")
						case <-request.Context().Done():
						}
						return
					}
					_, _ = io.WriteString(writer, "ok")
				})
				t.Cleanup(unblock)
				var sockets reviewBehaviorSockets
				options.Native.DialContext = sockets.dial
				options.MaxTCPConnections = 1
				options.IdleConnTimeout = 2 * time.Second
				if expire {
					options.IdleConnTimeout = shortIdle
				}
				fix := newFixture(t, options, 1)
				stream, receipt, err := fix.client.Open(testContext(t), fault.Correlation{Call: "active"}, request(t, "GET", url+"/hold", ""))
				if err != nil || stream == nil {
					t.Fatal("active stream", err)
				}
				t.Cleanup(func() { _ = stream.Close(testContext(t)) })
				opened := sockets.snapshot()
				if len(opened) != 1 || stream.Metadata().Protocol() != reviewBehaviorProtocol(protocol) {
					t.Fatal("peer did not use the selected protocol and one native TCP socket")
				}
				reviewBehaviorAlive(t, opened[0].closed, 3*shortIdle, "idle timeout closed an active response socket")
				unblock()
				if body, err := io.ReadAll(stream); err != nil || string(body) != "ok" {
					t.Fatal("active response did not complete", err)
				}
				if err := stream.Close(testContext(t)); err != nil {
					t.Fatal(err)
				}
				if result := settle(t, fix, receipt); result.Err() != nil || !result.Outcome.Value.Complete() {
					t.Fatal("initial response evidence")
				}
				wantDials := 1
				if expire {
					reviewBehaviorClosed(t, opened[0].closed, "explicit idle timeout failed to close idle native TCP socket")
					reviewBehaviorWait(t, "idle closure failed to release the sole native TCP slot", func() bool {
						fix.client.owner.mu.Lock()
						defer fix.client.owner.mu.Unlock()
						return fix.client.owner.tcp == 0
					})
					wantDials = 2
				} else {
					reviewBehaviorAlive(t, opened[0].closed, 3*shortIdle, "long idle control closed instead of retaining the reusable socket")
				}
				receipt, err = fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "reuse"}, request(t, "GET", url+"/reuse", ""))
				if err != nil {
					t.Fatal("request after idle interval", err)
				}
				result := settle(t, fix, receipt)
				if result.Err() != nil || !result.Outcome.Value.Complete() || result.Outcome.Value.Metadata().Protocol() != reviewBehaviorProtocol(protocol) ||
					string(result.Outcome.Value.DataCopy()) != "ok" || len(sockets.snapshot()) != wantDials {
					t.Fatalf("wrong connection reuse/expiration: dials=%d want=%d", len(sockets.snapshot()), wantDials)
				}
				reviewBehaviorReleased(t, fix, &sockets)
			})
		}
	}
}

func reviewBehaviorQUICReleased(t *testing.T, peer *peers) {
	t.Helper()
	peer.mu.Lock()
	connections := slices.Clone(peer.quic)
	peer.mu.Unlock()
	for _, conn := range connections {
		reviewBehaviorClosed(t, conn.Context().Done(), "peer QUIC connection survived source release")
	}
}

func TestIdleConnTimeoutDoesNotReplaceNativeQUICPolicy(t *testing.T) {
	url, options, peer := reviewBehaviorPeer(t, "h3", func(writer stdhttp.ResponseWriter, request *stdhttp.Request) {
		_, _ = io.WriteString(writer, "ok")
	})
	options.IdleConnTimeout = 80 * time.Millisecond
	fix := newFixture(t, options, 1)
	for index := range 2 {
		receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: strconv.Itoa(index)}, request(t, "GET", url, ""))
		if err != nil {
			t.Fatal(err)
		}
		result := settle(t, fix, receipt)
		if result.Err() != nil || !result.Outcome.Value.Complete() || result.Outcome.Value.Metadata().Protocol() != "HTTP/3.0" {
			t.Fatal("QUIC applicability control did not reach HTTP/3")
		}
		peer.mu.Lock()
		count := len(peer.quic)
		var closed <-chan struct{}
		if count == 1 {
			closed = peer.quic[0].Context().Done()
		}
		peer.mu.Unlock()
		if count != 1 {
			t.Fatalf("native QUIC policy failed to reuse the same connection: %d", count)
		}
		reviewBehaviorAlive(t, closed, 240*time.Millisecond, "TCP idle option replaced the independent native QUIC policy")
	}
	reviewBehaviorReleased(t, fix, nil)
	reviewBehaviorQUICReleased(t, peer)
}

func reviewBehaviorLayeredFixture(t *testing.T, options OptionsV1, layers ...resource.Layer) *fixture {
	t.Helper()
	prepared, err := PrepareV1(options, layers...)
	if err != nil {
		t.Fatal(err)
	}
	selected := resource.WithLimits(prepared.Select(), prepared.Metadata().Limits)
	assembly, err := resource.Assemble(testContext(t), testContext(t), "option-behavior", selected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := assembly.Close(testContext(t)); err != nil {
			t.Error("source cleanup", err)
		}
	})
	inbox, err := invocation.NewInbox[Result](1, prepared.Metadata().EvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{client: client, assembly: assembly, selected: selected, inbox: inbox}
}

func TestDisableCompressionPreservesRawOrDecodesNativeResponse(t *testing.T) {
	plain := []byte("bounded gzip response from a local independent peer")
	var encoded bytes.Buffer
	compressor := gzip.NewWriter(&encoded)
	if _, err := compressor.Write(plain); err != nil {
		t.Fatal(err)
	}
	if err := compressor.Close(); err != nil {
		t.Fatal(err)
	}
	for _, protocol := range []string{"h1", "h2", "ja-h1", "ja-h2", "h2c", "h3"} {
		for _, setting := range []string{"default", "explicit-false", "true"} {
			t.Run(protocol+"/"+setting, func(t *testing.T) {
				var calls atomic.Int64
				malformed := []byte("not a gzip stream")
				url, options, peer := reviewBehaviorPeer(t, protocol, func(writer stdhttp.ResponseWriter, request *stdhttp.Request) {
					calls.Add(1)
					if request.Proto != reviewBehaviorProtocol(protocol) || request.Header.Get("Accept-Encoding") != "gzip" {
						t.Error("peer protocol or explicit request negotiation changed")
					}
					body := encoded.Bytes()
					if request.URL.Path == "/malformed" {
						body = malformed
					}
					writer.Header().Set("Content-Encoding", "gzip")
					writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
					_, _ = writer.Write(body)
				})
				var sockets reviewBehaviorSockets
				options.Native.DialContext = sockets.dial
				var layers []resource.Layer
				switch setting {
				case "true":
					options.DisableCompression = true
				case "explicit-false":
					options.DisableCompression = true
					layers = []resource.Layer{{Kind: resource.Local, Content: []byte(`{"disable_compression":false}`)}}
				}
				fix := reviewBehaviorLayeredFixture(t, options, layers...)
				for _, path := range []string{"/valid", "/malformed"} {
					input := request(t, "GET", url+path, "")
					input.Header.Set("Accept-Encoding", "gzip")
					receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: strings.TrimPrefix(path, "/")}, input)
					if receipt == nil {
						t.Fatal("compression fixture was rejected before admission", err)
					}
					result := settle(t, fix, receipt)
					value := result.Outcome.Value
					if path == "/malformed" && setting != "true" {
						if err == nil || result.Outcome.Primary == nil || value.Complete() {
							t.Fatal("enabled decoding accepted malformed gzip")
						}
					} else {
						want := plain
						if setting == "true" {
							want = encoded.Bytes()
							if path == "/malformed" {
								want = malformed
							}
						}
						if err != nil || result.Err() != nil || !value.Complete() || !bytes.Equal(value.DataCopy(), want) || value.BytesRead() != int64(len(want)) {
							t.Fatalf("native compression option changed bytes/completion: bytes=%d want=%d complete=%t error=%v", value.BytesRead(), len(want), value.Complete(), err)
						}
					}
					wantEncoding, wantLength := "gzip", int64(encoded.Len())
					if path == "/malformed" {
						wantLength = int64(len(malformed))
					} else if setting != "true" {
						wantEncoding, wantLength = "", -1
					}
					if result.Outcome.Cleanup != nil || value.Metadata().Protocol() != reviewBehaviorProtocol(protocol) || value.Metadata().StatusCode() != 200 ||
						value.Metadata().HeadersCopy().Get("Content-Encoding") != wantEncoding || value.Metadata().ContentLength() != wantLength ||
						value.RoundTrips() != 1 || !result.Final || !result.Released {
						t.Fatal("response header observation, single dispatch, cleanup or independent release changed")
					}
				}
				if calls.Load() != 2 {
					t.Fatal("compression control repeated or skipped a peer request")
				}
				reviewBehaviorReleased(t, fix, &sockets)
				if protocol == "h3" {
					reviewBehaviorQUICReleased(t, peer)
				}
			})
		}
	}
}
