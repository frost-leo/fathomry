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
	"context"
	"crypto/tls"
	"errors"
	"io"
	stdhttp "net/http"
	"strings"
	"sync"
	"testing"
	"time"

	http "github.com/enetx/http"
	nativeh3 "github.com/enetx/http3"
	"github.com/quic-go/quic-go"
)

var errReviewH3ClientCapacity = errors.New("review: HTTP/3 client capacity")

func reviewH3PeerCounts(peer *peers) (accepted, live int) {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	for _, conn := range peer.quic {
		accepted++
		if conn.Context().Err() == nil {
			live++
		}
	}
	return accepted, live
}

type reviewH3Quota struct {
	mu                  sync.Mutex
	active, peak, limit int
	acquired, released  int
}

func (quota *reviewH3Quota) acquire() (func(), error) {
	quota.mu.Lock()
	defer quota.mu.Unlock()
	if quota.active >= quota.limit {
		return nil, errReviewH3ClientCapacity
	}
	quota.active++
	quota.acquired++
	quota.peak = max(quota.peak, quota.active)
	return func() {
		quota.mu.Lock()
		defer quota.mu.Unlock()
		quota.active--
		quota.released++
	}, nil
}

func (quota *reviewH3Quota) used() int {
	quota.mu.Lock()
	defer quota.mu.Unlock()
	return quota.active
}

func reviewH3Eventually(t *testing.T, label string, check func() bool) {
	t.Helper()
	ctx := testContext(t)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !check() {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal(label)
		}
	}
}

func reviewH3Native(t *testing.T, peer *peers, quota *reviewH3Quota) *nativeh3.Transport {
	t.Helper()
	transport := &nativeh3.Transport{
		TLSClientConfig:        &tls.Config{RootCAs: peer.roots},
		MaxResponseHeaderBytes: 1024,
		FathomryAcquireClient:  quota.acquire,
	}
	t.Cleanup(func() {
		if err := transport.Close(); err != nil {
			t.Error("native close", err)
		}
	})
	return transport
}

func reviewH3NativeDo(t *testing.T, transport *nativeh3.Transport, uri string) (*http.Response, error) {
	t.Helper()
	input := request(t, http.MethodGet, uri, "")
	input = input.WithContext(testContext(t))
	return transport.RoundTrip(input)
}

func reviewH3Consume(t *testing.T, response *http.Response, err error) {
	t.Helper()
	if err != nil || response == nil {
		t.Fatal("normal H3 response missing", err)
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || response.Proto != "HTTP/3.0" {
		t.Fatal("normal H3 response did not complete", readErr, closeErr, response.Proto)
	}
}

func TestH3RetiredClientQuotaAndSibling(t *testing.T) {
	gate := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(gate) })
	defer unblock()
	peer := newPeers(t, func(writer stdhttp.ResponseWriter, input *stdhttp.Request) {
		switch input.URL.Path {
		case "/held":
			writer.Header().Set("Content-Length", "9")
			_, _ = io.WriteString(writer, "left")
			writer.(stdhttp.Flusher).Flush()
			select {
			case <-gate:
				_, _ = io.WriteString(writer, "right")
			case <-input.Context().Done():
			}
		case "/oversized":
			writer.Header().Set("X-Oversized", strings.Repeat("x", 16<<10))
		default:
			_, _ = io.WriteString(writer, "normal")
		}
	}, true)
	quota := &reviewH3Quota{limit: 1}
	transport := reviewH3Native(t, peer, quota)
	held, err := reviewH3NativeDo(t, transport, peer.tcp.URL+"/held")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Body.Close()
	prefix := make([]byte, 4)
	if _, err := io.ReadFull(held.Body, prefix); err != nil || string(prefix) != "left" {
		t.Fatal("held prefix", err)
	}
	transport.CloseIdleConnections()
	if _, live := reviewH3PeerCounts(peer); live != 1 || quota.used() != 1 {
		t.Fatal("live body was treated as an idle connection", live, quota.used())
	}
	if _, err := reviewH3NativeDo(t, transport, peer.tcp.URL+"/oversized"); err == nil {
		t.Fatal("oversized response admitted")
	}
	if accepted, live := reviewH3PeerCounts(peer); accepted != 1 || live != 1 || quota.used() != 1 {
		t.Fatal("retired sibling connection lost or quota released early", accepted, live, quota.used())
	}
	if _, err := reviewH3NativeDo(t, transport, peer.tcp.URL+"/normal"); !errors.Is(err, errReviewH3ClientCapacity) {
		t.Fatal("retired client did not consume the bounded slot", err)
	}
	if accepted, _ := reviewH3PeerCounts(peer); accepted != 1 {
		t.Fatal("capacity refusal reached peer", accepted)
	}
	unblock()
	tail, err := io.ReadAll(held.Body)
	if err != nil || string(tail) != "right" {
		t.Fatal("stream-local failure interrupted sibling", err)
	}
	if err := held.Body.Close(); err != nil {
		t.Fatal("held close", err)
	}
	reviewH3Eventually(t, "retired client did not release quota and close at peer", func() bool {
		_, live := reviewH3PeerCounts(peer)
		return quota.used() == 0 && live == 0
	})
	response, err := reviewH3NativeDo(t, transport, peer.tcp.URL+"/normal")
	reviewH3Consume(t, response, err)
	if accepted, live := reviewH3PeerCounts(peer); accepted != 2 || live != 1 || quota.used() != 1 {
		t.Fatal("capacity not reusable", accepted, live, quota.used())
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	reviewH3Eventually(t, "owner close did not terminate cached client", func() bool {
		_, live := reviewH3PeerCounts(peer)
		return quota.used() == 0 && live == 0
	})
}

func TestH3SerialStreamFailureReleasesClient(t *testing.T) {
	peer := newPeers(t, func(writer stdhttp.ResponseWriter, input *stdhttp.Request) {
		if input.URL.Path == "/oversized" {
			writer.Header().Set("X-Oversized", strings.Repeat("x", 16<<10))
		}
		_, _ = io.WriteString(writer, "normal")
	}, true)
	quota := &reviewH3Quota{limit: 1}
	transport := reviewH3Native(t, peer, quota)
	for index := range 8 {
		response, err := reviewH3NativeDo(t, transport, peer.tcp.URL+"/normal")
		reviewH3Consume(t, response, err)
		if _, err := reviewH3NativeDo(t, transport, peer.tcp.URL+"/oversized"); err == nil {
			t.Fatal("oversized response admitted")
		}
		reviewH3Eventually(t, "unborrowed failed client remained live", func() bool {
			_, live := reviewH3PeerCounts(peer)
			return live == 0 && quota.used() == 0
		})
		accepted, live := reviewH3PeerCounts(peer)
		t.Logf("failure %d: peer accepted=%d/live=%d quota=%d", index+1, accepted, live, quota.used())
	}
}

func TestH3CanceledLastWaiterReclaimsFailedDial(t *testing.T) {
	quota := &reviewH3Quota{limit: 1}
	dialEntered := make(chan struct{})
	dialReturn := make(chan struct{})
	dialExited := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(dialReturn) })
	transport := &nativeh3.Transport{FathomryAcquireClient: quota.acquire,
		Dial: func(ctx context.Context, _ string, _ *tls.Config, _ *quic.Config) (*quic.Conn, error) {
			close(dialEntered)
			defer close(dialExited)
			<-dialReturn
			return nil, ctx.Err()
		},
	}
	t.Cleanup(func() { unblock(); _ = transport.Close() })
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	input := request(t, http.MethodGet, "https://first.invalid/", "").WithContext(ctx)
	result := make(chan error, 1)
	go func() { _, err := transport.RoundTrip(input); result <- err }()
	select {
	case <-dialEntered:
	case <-testContext(t).Done():
		t.Fatal("dial not entered")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("wait cancellation changed", err)
		}
	case <-testContext(t).Done():
		t.Fatal("wait cancellation blocked")
	}
	if quota.used() != 1 {
		t.Fatal("pending native dial released quota too early", quota.used())
	}
	unblock()
	<-dialExited
	reviewH3Eventually(t, "last waiter canceled, native dial failed, but client slot remained stranded", func() bool { return quota.used() == 0 })
}

type reviewH3BlockedInput struct {
	readStarted chan struct{}
	allowReturn chan struct{}
	closed      chan struct{}
	startOnce   sync.Once
	closeOnce   sync.Once
}

func (input *reviewH3BlockedInput) Read([]byte) (int, error) {
	input.startOnce.Do(func() { close(input.readStarted) })
	<-input.allowReturn
	return 0, io.EOF
}

func (input *reviewH3BlockedInput) Close() error {
	input.closeOnce.Do(func() { close(input.closed) })
	return nil
}

func TestH3WriterRetainsRetiredClient(t *testing.T) {
	input := &reviewH3BlockedInput{readStarted: make(chan struct{}), allowReturn: make(chan struct{}), closed: make(chan struct{})}
	unblock := sync.OnceFunc(func() { close(input.allowReturn) })
	defer unblock()
	peer := newPeers(t, func(writer stdhttp.ResponseWriter, request *stdhttp.Request) {
		if request.URL.Path == "/oversized" {
			<-input.readStarted
			writer.Header().Set("X-Oversized", strings.Repeat("x", 16<<10))
		}
	}, true)
	quota := &reviewH3Quota{limit: 1}
	transport := reviewH3Native(t, peer, quota)
	request, err := http.NewRequestWithContext(testContext(t), http.MethodPost, peer.tcp.URL+"/oversized", input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transport.RoundTrip(request); err == nil {
		t.Fatal("oversized response admitted")
	}
	if accepted, live := reviewH3PeerCounts(peer); accepted != 1 || live != 1 || quota.used() != 1 {
		t.Fatal("request-writer returned ownership before real return", accepted, live, quota.used())
	}
	if _, err := reviewH3NativeDo(t, transport, peer.tcp.URL+"/normal"); !errors.Is(err, errReviewH3ClientCapacity) {
		t.Fatal("request writer did not retain retired slot", err)
	}
	unblock()
	select {
	case <-input.closed:
	case <-testContext(t).Done():
		t.Fatal("native input was not closed")
	}
	reviewH3Eventually(t, "writer completed but retained client was not closed", func() bool {
		_, live := reviewH3PeerCounts(peer)
		return live == 0 && quota.used() == 0
	})
}

func TestH3CloseCancelsAllPendingBeforeJoin(t *testing.T) {
	quota := &reviewH3Quota{limit: 2}
	firstEntered, secondEntered := make(chan struct{}), make(chan struct{})
	firstCanceled, secondCanceled := make(chan struct{}), make(chan struct{})
	firstReturn := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(firstReturn) })
	transport := &nativeh3.Transport{FathomryAcquireClient: quota.acquire,
		Dial: func(ctx context.Context, address string, _ *tls.Config, _ *quic.Config) (*quic.Conn, error) {
			if strings.HasPrefix(address, "first.") {
				close(firstEntered)
				<-ctx.Done()
				close(firstCanceled)
				<-firstReturn
			} else {
				close(secondEntered)
				<-ctx.Done()
				close(secondCanceled)
			}
			return nil, ctx.Err()
		},
	}
	t.Cleanup(func() { unblock(); _ = transport.Close() })
	firstResult, secondResult := make(chan error, 1), make(chan error, 1)
	go func() { _, err := reviewH3NativeDo(t, transport, "https://first.invalid/"); firstResult <- err }()
	go func() { _, err := reviewH3NativeDo(t, transport, "https://second.invalid/"); secondResult <- err }()
	for _, entered := range []chan struct{}{firstEntered, secondEntered} {
		select {
		case <-entered:
		case <-testContext(t).Done():
			t.Fatal("pending native dial did not start")
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- transport.Close() }()
	for _, canceled := range []chan struct{}{firstCanceled, secondCanceled} {
		select {
		case <-canceled:
		case <-testContext(t).Done():
			t.Fatal("blocked sibling prevented cancellation")
		}
	}
	select {
	case err := <-secondResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("second cancellation lost", err)
		}
	case <-testContext(t).Done():
		t.Fatal("independent second dial did not finish")
	}
	reviewH3Eventually(t, "unblocked pending dial was not independently released", func() bool { return quota.used() == 1 })
	select {
	case <-closed:
		t.Fatal("close returned before blocked native dial actually exited")
	default:
	}
	unblock()
	select {
	case err := <-firstResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("first cancellation lost", err)
		}
	case <-testContext(t).Done():
		t.Fatal("first dial did not finish")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal("close failed", err)
		}
	case <-testContext(t).Done():
		t.Fatal("close did not finish")
	}
	if quota.used() != 0 {
		t.Fatal("close released an incorrect number of native reservations", quota.used())
	}
}
