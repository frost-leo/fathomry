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
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	nativehttp "github.com/enetx/http"
	"github.com/enetx/http/httptrace"
	nativeh3 "github.com/enetx/http3"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/quic-go/qpack"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
)

type reviewH3MultipartInput struct {
	prefix              *strings.Reader
	blocked, stopped    chan struct{}
	readOnce, closeOnce sync.Once
	closed              atomic.Int64
}

func (input *reviewH3MultipartInput) Read(buffer []byte) (int, error) {
	if input.prefix.Len() > 0 {
		return input.prefix.Read(buffer)
	}
	input.readOnce.Do(func() { close(input.blocked) })
	<-input.stopped
	return 0, io.EOF
}

func (input *reviewH3MultipartInput) Close() error {
	input.closeOnce.Do(func() { input.closed.Add(1); close(input.stopped) })
	return nil
}

func reviewH3WriteFrame(writer io.Writer, kind uint64, payload []byte) error {
	frame := quicvarint.Append(nil, kind)
	frame = quicvarint.Append(frame, uint64(len(payload)))
	frame = append(frame, payload...)
	_, err := writer.Write(frame)
	return err
}

func reviewH3ResetPeer(t *testing.T, blocked <-chan struct{}) (string, *tls.Config, *atomic.Int64, <-chan error) {
	t.Helper()
	tlsPeer := newPeers(t, func(stdhttp.ResponseWriter, *stdhttp.Request) {}, false)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener, err := quic.Listen(packet, &tls.Config{Certificates: tlsPeer.tcp.TLS.Certificates, NextProtos: []string{"h3"}}, &quic.Config{})
	if err != nil {
		_ = packet.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(testContext(t))
	var pending sync.WaitGroup
	var requests atomic.Int64
	errors := make(chan error, 8)
	report := func(err error) {
		select {
		case errors <- err:
		default:
		}
	}
	pending.Go(func() {
		for {
			connection, err := listener.Accept(ctx)
			if err != nil {
				return
			}
			pending.Go(func() {
				defer connection.CloseWithError(0, "")
				stop := context.AfterFunc(ctx, func() { _ = connection.CloseWithError(0, "") })
				defer stop()
				control, err := connection.OpenUniStreamSync(ctx)
				if err != nil {
					report(err)
					return
				}
				if _, err := control.Write([]byte{0, 4, 0}); err != nil {
					report(err)
					return
				}
				for {
					stream, err := connection.AcceptStream(ctx)
					if err != nil {
						return
					}
					reader := quicvarint.NewReader(stream)
					kind, err := quicvarint.Read(reader)
					if err != nil || kind != 1 {
						report(fmt.Errorf("request HEADERS frame: kind=%d err=%v", kind, err))
						return
					}
					length, err := quicvarint.Read(reader)
					if err != nil || length > 64<<10 {
						report(fmt.Errorf("request HEADERS length=%d err=%v", length, err))
						return
					}
					if _, err := io.CopyN(io.Discard, reader, int64(length)); err != nil {
						report(err)
						return
					}
					if requests.Add(1) == 1 {
						select {
						case <-blocked:
						case <-ctx.Done():
							return
						}
						stream.CancelRead(quic.StreamErrorCode(0x10b))
						stream.CancelWrite(quic.StreamErrorCode(0x10b))
						continue
					}
					data, err := io.ReadAll(reader)
					if err != nil || !bytes.Contains(data, []byte("second-body")) {
						report(fmt.Errorf("replayed body absent: %v", err))
						return
					}
					var header bytes.Buffer
					encoder := qpack.NewEncoder(&header)
					_ = encoder.WriteField(qpack.HeaderField{Name: ":status", Value: "200"})
					_ = encoder.WriteField(qpack.HeaderField{Name: "content-length", Value: "2"})
					if err := reviewH3WriteFrame(stream, 1, header.Bytes()); err != nil {
						report(err)
						return
					}
					if err := reviewH3WriteFrame(stream, 0, []byte("ok")); err != nil {
						report(err)
						return
					}
					if err := stream.Close(); err != nil {
						report(err)
						return
					}
				}
			})
		}
	})
	t.Cleanup(func() { cancel(); _ = listener.Close(); _ = packet.Close(); pending.Wait() })
	return "https://" + packet.LocalAddr().String(), &tls.Config{RootCAs: tlsPeer.roots}, &requests, errors
}

func TestH3MultipartRejectedReplayClosesPriorInput(t *testing.T) {
	for _, clients := range []int{1, 2} {
		t.Run(fmt.Sprintf("clients-%d", clients), func(t *testing.T) {
			first := &reviewH3MultipartInput{prefix: strings.NewReader("first-prefix"), blocked: make(chan struct{}), stopped: make(chan struct{})}
			defer first.Close()
			endpoint, tlsConfig, requests, peerErrors := reviewH3ResetPeer(t, first.blocked)
			var factories atomic.Int64
			var priorOpenOnReplay atomic.Bool
			second := &multipartReader{Reader: strings.NewReader("second-body")}
			options := OptionsV1{Name: "h3-multipart-replay", Mode: PreferHTTP3, MaxActive: 1, MaxRoutes: 1,
				MaxTCPConnections: 1, MaxUDPSockets: 1, MaxHTTP3Clients: clients, Native: NativeOptionsV1{TLSConfig: tlsConfig}}
			fixture := newFixture(t, options, 1)
			body := &Multipart{Parts: []Part{{Name: "file", FileName: "input.txt", Open: func(context.Context) (io.ReadCloser, error) {
				if factories.Add(1) == 1 {
					return first, nil
				}
				priorOpenOnReplay.Store(first.closed.Load() == 0)
				return second, nil
			}}}}
			receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "h3-reset-replay"}, multipartRequest(t, endpoint), RequestOptionsV1{Multipart: body})
			if receipt == nil {
				t.Fatal("request not admitted", err)
			}
			result := settle(t, fixture, receipt)
			select {
			case peerErr := <-peerErrors:
				t.Fatal("peer failed", peerErr)
			default:
			}
			t.Logf("H3 client quota=%d peer requests=%d factories=%d prior input open at replay=%v first closes=%d second closes=%d complete=%v capacity error=%v err=%v", clients, requests.Load(), factories.Load(), priorOpenOnReplay.Load(), first.closed.Load(), second.closed.Load(), result.Outcome.Value.Complete(), errors.Is(err, resource.ErrCapacity), err)
			if err != nil || result.Err() != nil || !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "ok" || requests.Load() != 2 {
				t.Fatal("safe H3_REQUEST_REJECTED replay failed", err, result.Err())
			}
			if priorOpenOnReplay.Load() || first.closed.Load() != 1 || second.closed.Load() != 1 {
				t.Fatal("replay factory ran before prior multipart input was closed")
			}
		})
	}
}

func TestH3RejectedRetryWaitsForNativeWriter(t *testing.T) {
	first := &reviewH3MultipartInput{prefix: strings.NewReader("first-prefix"), blocked: make(chan struct{}), stopped: make(chan struct{})}
	defer first.Close()
	endpoint, tlsConfig, requests, peerErrors := reviewH3ResetPeer(t, first.blocked)
	var capacity atomic.Int64
	capacityError := errors.New("review: HTTP/3 client capacity")
	transport := &nativeh3.Transport{TLSClientConfig: tlsConfig, FathomryAcquireClient: func() (func(), error) {
		if !capacity.CompareAndSwap(0, 1) {
			return nil, capacityError
		}
		return func() { capacity.Add(-1) }, nil
	}}
	t.Cleanup(func() { _ = transport.Close() })
	writerReached, allowWriter := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(allowWriter) })
	defer unblock()
	var writes atomic.Int64
	ctx := httptrace.WithClientTrace(testContext(t), &httptrace.ClientTrace{WroteRequest: func(httptrace.WroteRequestInfo) {
		if writes.Add(1) == 1 {
			close(writerReached)
			<-allowWriter
		}
	}})
	input, err := nativehttp.NewRequestWithContext(ctx, "POST", endpoint, first)
	if err != nil {
		t.Fatal(err)
	}
	input.GetBody = func() (io.ReadCloser, error) {
		if err := first.Close(); err != nil {
			return nil, err
		}
		return io.NopCloser(strings.NewReader("second-body")), nil
	}
	type returned struct {
		response *nativehttp.Response
		err      error
	}
	returns := make(chan returned, 1)
	go func() { response, err := transport.RoundTrip(input); returns <- returned{response: response, err: err} }()
	select {
	case <-writerReached:
	case <-testContext(t).Done():
		t.Fatal("native writer did not reach the trace gate")
	}
	var completed returned
	var returnedBeforeWriter bool
	select {
	case completed = <-returns:
		returnedBeforeWriter = true
	case <-time.After(30 * time.Millisecond):
	}
	unblock()
	if !returnedBeforeWriter {
		select {
		case completed = <-returns:
		case <-testContext(t).Done():
			t.Fatal("native call did not return after writer release")
		}
	}
	select {
	case err := <-peerErrors:
		t.Fatal("peer failed", err)
	default:
	}
	t.Logf("native gated writer: peer requests=%d returned before writer=%v capacity error=%v first closes=%d", requests.Load(), returnedBeforeWriter, errors.Is(completed.err, capacityError), first.closed.Load())
	if completed.response != nil {
		defer completed.response.Body.Close()
	}
	if completed.err != nil || completed.response == nil || returnedBeforeWriter {
		t.Fatal("safe retry did not wait for prior native writer", completed.err)
	}
	data, err := io.ReadAll(completed.response.Body)
	if err != nil || string(data) != "ok" || requests.Load() != 2 {
		t.Fatal("native retry did not complete", err)
	}
}

func TestH3OneShotRejectedStopsBeforeResend(t *testing.T) {
	first := &reviewH3MultipartInput{prefix: strings.NewReader("first-prefix"), blocked: make(chan struct{}), stopped: make(chan struct{})}
	defer first.Close()
	endpoint, tlsConfig, requests, peerErrors := reviewH3ResetPeer(t, first.blocked)
	options := OptionsV1{Name: "h3-multipart-one-shot", Mode: PreferHTTP3, MaxActive: 1, MaxRoutes: 1,
		MaxTCPConnections: 1, MaxUDPSockets: 1, MaxHTTP3Clients: 1, Native: NativeOptionsV1{TLSConfig: tlsConfig}}
	fixture := newFixture(t, options, 1)
	body := &Multipart{Parts: []Part{{Name: "file", FileName: "input.txt", Input: first}}}
	receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "h3-reset-one-shot"}, multipartRequest(t, endpoint), RequestOptionsV1{Multipart: body})
	if err == nil || receipt == nil {
		t.Fatal("one-shot rejected upload must return admitted failure", err)
	}
	result := settle(t, fixture, receipt)
	select {
	case peerErr := <-peerErrors:
		t.Fatal("peer failed", peerErr)
	default:
	}
	if requests.Load() != 1 || first.closed.Load() != 1 || result.Outcome.Value.Complete() || result.Err() == nil {
		t.Fatal("unsafe one-shot retry was sent or cleanup/evidence was lost", requests.Load(), first.closed.Load(), result.Err())
	}
}
