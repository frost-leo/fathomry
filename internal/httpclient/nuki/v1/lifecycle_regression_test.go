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
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	nativetls "github.com/nukilabs/utls"
	"github.com/quic-go/qpack"
	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/quic-go/quicvarint"
)

func nativeWorkerStacks(symbol string) []string {
	buffer := make([]byte, 1<<20)
	buffer = buffer[:runtime.Stack(buffer, true)]
	var matches []string
	for _, stack := range strings.Split(string(buffer), "\n\n") {
		if strings.Contains(stack, symbol) {
			matches = append(matches, stack)
		}
	}
	return matches
}

func TestProviderEarlyH3ResponseJoinsBlockedUpload(t *testing.T) {
	t.Run("final-response", func(t *testing.T) { testEarlyH3Response(t, false) })
	t.Run("redirect-response", func(t *testing.T) { testEarlyH3Response(t, true) })
}

func testEarlyH3Response(t *testing.T, redirect bool) {
	t.Helper()
	exchanges := 3
	if redirect {
		exchanges *= 2
	}
	certificate := httptest.NewTLSServer(nil)
	defer certificate.Close()
	listener, err := quic.ListenAddr("127.0.0.1:0", &tls.Config{Certificates: certificate.TLS.Certificates, NextProtos: []string{"h3"}},
		&quic.Config{InitialStreamReceiveWindow: 1024, MaxStreamReceiveWindow: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx := testContext(t)
	responseReady := make(chan struct{})
	peerDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept(ctx)
		if err != nil {
			peerDone <- err
			return
		}
		defer conn.CloseWithError(0, "")
		control, err := conn.OpenUniStreamSync(ctx)
		if err != nil {
			peerDone <- err
			return
		}
		if _, err = control.Write([]byte{0, 4, 0}); err != nil {
			peerDone <- err
			return
		}
		for index := range exchanges {
			request, err := conn.AcceptStream(ctx)
			if err != nil {
				peerDone <- err
				return
			}
			reader := quicvarint.NewReader(request)
			kind, err := quicvarint.Read(reader)
			if err != nil || kind != 1 {
				peerDone <- errors.Join(err, errors.New("missing request HEADERS"))
				return
			}
			length, err := quicvarint.Read(reader)
			if err != nil || length > 64<<10 {
				peerDone <- errors.Join(err, errors.New("invalid request header length"))
				return
			}
			if _, err = io.CopyN(io.Discard, request, int64(length)); err != nil {
				peerDone <- err
				return
			}
			if !redirect || index%2 == 0 {
				select {
				case <-responseReady:
				case <-ctx.Done():
					peerDone <- ctx.Err()
					return
				}
			}
			// Complete the response while the request exceeds the peer's credit.
			response := []byte{1, 3, 0, 0, 0xd9, 0, 2, 'o', 'k'}
			if redirect && index%2 == 0 {
				var encoded bytes.Buffer
				encoder := qpack.NewEncoder(&encoded)
				err = errors.Join(encoder.WriteField(qpack.HeaderField{Name: ":status", Value: "303"}),
					encoder.WriteField(qpack.HeaderField{Name: "location", Value: "/final"}), encoder.Close())
				if err != nil {
					peerDone <- err
					return
				}
				response = quicvarint.Append(nil, 1)
				response = quicvarint.Append(response, uint64(encoded.Len()))
				response = append(response, encoded.Bytes()...)
			}
			if _, err = request.Write(response); err != nil {
				peerDone <- err
				return
			}
			if err = request.Close(); err != nil {
				peerDone <- err
				return
			}
		}
		<-conn.Context().Done()
		peerDone <- nil
	}()
	options := providerOptions()
	options.Mode, options.MaxActive, options.MaxRequestBytes = HTTP3Only, 1, 1<<20
	options.FollowRedirects = redirect
	options.Native.TLS = &nativetls.Config{InsecureSkipVerify: true}
	fixture := bindProvider(t, options)
	const sender = "github.com/nukilabs/quic-go/http3.(*ClientConn).sendRequestBody"
	baseline := len(nativeWorkerStacks(sender))
	for range 3 {
		receipt, err := fixture.client.Do(ctx, fault.Correlation{Call: "early-response"}, nativeRequest(t, "POST", "https://"+listener.Addr().String(), strings.NewReader(strings.Repeat("x", 512<<10))))
		if err != nil {
			t.Fatal(err)
		}
		// Independently observe the native upload before letting the peer reply.
		for {
			if len(nativeWorkerStacks(sender)) > baseline {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("native upload never became active")
			case <-time.After(time.Millisecond):
			}
		}
		select {
		case responseReady <- struct{}{}:
		case <-ctx.Done():
			t.Fatal("peer did not await response gate")
		}
		result := outcome(t, fixture, receipt)
		value := result.Outcome.Value
		if result.Err() != nil || !value.Complete() || string(value.DataCopy()) != "ok" || len(value.InputErrorsCopy()) == 0 {
			t.Fatal("early-response or incomplete-upload evidence changed", result.Err())
		}
		if value.Exchanges() != exchanges/3 {
			t.Fatal("redirect control did not exercise the expected exchanges")
		}
		if workers := nativeWorkerStacks(sender); len(workers) > baseline {
			t.Fatalf("upload outlived released receipt: %s", strings.Join(workers, "\n\n"))
		}
	}
	if err := fixture.assembly.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-peerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("peer did not join")
	}
}

func TestProviderCanceledProxySetupRetainsBoundedNativeCharge(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		close(entered)
		<-release
	}))
	defer peer.Close()
	defer close(release)
	options := providerOptions()
	options.Mode, options.MaxActive, options.MaxConnections, options.ProxyURL = HTTP1Only, 1, 1, peer.URL
	fixture := bindProvider(t, options)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	receipt, err := fixture.client.Do(ctx, fault.Correlation{Call: "cancel-proxy"}, nativeRequest(t, "GET", "http://127.0.0.1:1", nil))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("CONNECT never entered")
	}
	cancel()
	if result := outcome(t, fixture, receipt); !errors.Is(result.Err(), context.Canceled) {
		t.Fatal(result.Err())
	}
	for range 2 {
		receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "dial-capacity"}, nativeRequest(t, "GET", "http://127.0.0.1:1", nil))
		if err != nil {
			t.Fatal(err)
		}
		if result := outcome(t, fixture, receipt); !errors.Is(result.Err(), ErrCapacity) {
			t.Fatal("retained setup bypassed ceiling", result.Err())
		}
	}
	fixture.client.owner.mu.Lock()
	pending := fixture.client.owner.nativeDials
	fixture.client.owner.mu.Unlock()
	workers := nativeWorkerStacks("github.com/nukilabs/tlsclient/proxy.(*Dialer).DialContext")
	if pending != 1 || len(workers) != 1 {
		t.Fatalf("expected exactly one charged setup, charge=%d workers=%d", pending, len(workers))
	}
	if err := fixture.assembly.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if workers := nativeWorkerStacks("github.com/nukilabs/tlsclient/proxy.(*Dialer).DialContext"); len(workers) != 0 {
		t.Fatal("native setup survived source release")
	}
}

type transientPacketClose struct {
	net.PacketConn
	fail   atomic.Bool
	closed atomic.Bool
	cause  error
}

func (packet *transientPacketClose) Close() error {
	if packet.fail.Load() {
		return packet.cause
	}
	err := packet.PacketConn.Close()
	if err == nil || errors.Is(err, net.ErrClosed) {
		packet.closed.Store(true)
	}
	return err
}

func TestProviderRecoveredSOCKSCloseRetainsHistoryNotOwnership(t *testing.T) {
	certificate := httptest.NewTLSServer(nil)
	defer certificate.Close()
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peer := &http3.Server{TLSConfig: &tls.Config{Certificates: certificate.TLS.Certificates}, Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(writer, "ok")
	})}
	peerDone := make(chan error, 1)
	go func() { peerDone <- peer.Serve(packet) }()
	defer func() { _ = peer.Close(); _ = packet.Close(); <-peerDone }()
	address, controlClosed := socksRelay(t, packet.LocalAddr().(*net.UDPAddr))
	cause := errors.New("transient-udp-close")
	var injected atomic.Pointer[transientPacketClose]
	options := providerOptions()
	options.Mode, options.ProxyURL = HTTP3Only, address
	options.Native.TLS = &nativetls.Config{InsecureSkipVerify: true}
	options.Native.ListenPacket = func(ctx context.Context, network, _ string) (net.PacketConn, error) {
		socket, err := (&net.ListenConfig{}).ListenPacket(ctx, network, "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		owned := &transientPacketClose{PacketConn: socket, cause: cause}
		injected.Store(owned)
		return owned, nil
	}
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	limits, err := LimitsV1(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, limits)
	assembly, err := resource.Assemble(testContext(t), testContext(t), "nuki-tests", selected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if socket := injected.Load(); socket != nil {
			socket.fail.Store(false)
		}
		err := assembly.Close(testContext(t))
		if !errors.Is(err, cause) || errors.Is(err, resource.ErrIncomplete) {
			t.Error("assembly lost historical error or retained ownership", err)
		}
	})
	inbox, err := invocation.NewInbox[Result](4, 4*defaults(options).evidenceBytes())
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, info, err := resource.Bind(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &fixture{client: client, assembly: assembly, selected: selected, inbox: inbox, info: info}
	receipt, err := client.Do(testContext(t), fault.Correlation{Call: "socks-close"}, nativeRequest(t, "GET", "https://"+packet.LocalAddr().String(), nil))
	if err != nil {
		t.Fatal(err)
	}
	if result := outcome(t, fixture, receipt); result.Err() != nil || !result.Outcome.Value.Complete() {
		t.Fatal(result.Err())
	}
	socket := injected.Load()
	if socket == nil {
		t.Fatal("no physical UDP socket")
	}
	socket.fail.Store(true)
	first := client.owner.release(testContext(t))
	if first.Released || first.Quiescent || first.Continue == nil || !errors.Is(first.Err, cause) {
		t.Fatal("failed Close claimed release", first.Err)
	}
	socket.fail.Store(false)
	second := first.Continue(testContext(t))
	if !second.Released || !second.Quiescent || !errors.Is(second.Err, cause) || !socket.closed.Load() {
		t.Fatal("recovered physical release lost history or remained pending", second.Err)
	}
	client.owner.mu.Lock()
	sockets := len(client.owner.sockets)
	client.owner.mu.Unlock()
	if sockets != 0 {
		t.Fatal("physical socket charge remained")
	}
	select {
	case <-controlClosed:
	case <-testContext(t).Done():
		t.Fatal("SOCKS monitor not joined")
	}
}
