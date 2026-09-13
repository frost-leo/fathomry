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

package otel

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestNativeCancellationDoesNotJoinDialNegativeControl(t *testing.T) {
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	transport := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
		close(entered)
		<-release
		close(finished)
		return nil, errors.New("synthetic-dial-failure")
	}}
	defer transport.CloseIdleConnections()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, "GET", "http://127.0.0.1:1", nil)
	returned := make(chan struct{})
	go func() {
		response, _ := (&http.Client{Transport: transport}).Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		close(returned)
	}()
	<-entered
	cancel()
	<-returned
	transport.CloseIdleConnections()
	select {
	case <-finished:
		t.Fatal("native dial unexpectedly joined")
	default:
	}
	close(release)
	<-finished
}
func TestCanceledDialRetainsInvocationGuardUntilActualReturn(t *testing.T) {
	fixture := newFixture(t, OptionsV1{}, nil)
	entered, release := make(chan struct{}), make(chan struct{})
	fixture.owner.network.dialContext = func(ctx context.Context, _ string, _ string) (net.Conn, error) {
		close(entered)
		<-ctx.Done()
		<-release
		return nil, ctx.Err()
	}
	emitOne(t, fixture, "record", "message")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause := errors.New("dial-cancel-cause")
	returned := make(chan *invocation.Receipt[Result], 1)
	go func() { receipt, _ := fixture.client.Flush(ctx, fault.Correlation{Call: "flush"}); returned <- receipt }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("dial not entered")
	}
	cancel(cause)
	var receipt *invocation.Receipt[Result]
	select {
	case receipt = <-returned:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("canceled flush did not return")
	}
	result, ready := receipt.Result()
	if !ready || !result.Final || result.Released || !errors.Is(result.Err(), cause) {
		close(release)
		t.Fatal("unreturned native dial lost its retained obligation")
	}
	if err := fixture.assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
		close(release)
		t.Fatal("native dial was released before returning")
	}
	close(release)
	wait, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if result, err := receipt.WaitReleased(wait); err != nil || !result.Released {
		t.Fatal("native completion did not release guard")
	}
	if err := fixture.assembly.Close(wait); err != nil {
		t.Fatal(err)
	}
}
func TestCleanupContinuationJoinsNativeDialWithoutRetryingExport(t *testing.T) {
	fixture := newFixture(t, OptionsV1{}, nil)
	fixture.allowCloseError = true
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	fixture.owner.network.dialContext = func(ctx context.Context, _ string, _ string) (net.Conn, error) {
		calls.Add(1)
		close(entered)
		<-ctx.Done()
		<-release
		return nil, ctx.Err()
	}
	emitOne(t, fixture, "record", "message")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause := errors.New("cleanup-cancel-cause")
	returned := make(chan error, 1)
	go func() { returned <- fixture.assembly.Close(ctx) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup dial not entered")
	}
	cancel(cause)
	var err error
	select {
	case err = <-returned:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("cleanup budget was ignored")
	}
	status := fixture.assembly.Snapshot().Sources[0]
	if !errors.Is(err, resource.ErrIncomplete) || !errors.Is(err, cause) || status.Released || !status.CanContinue {
		close(release)
		t.Fatal("cleanup fabricated native release")
	}
	close(release)
	wait, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := fixture.assembly.Close(wait); !errors.Is(err, cause) {
		t.Fatal("continuation erased cleanup history")
	}
	if fixture.assembly.Snapshot().Sources[0].Pending || calls.Load() != 1 {
		t.Fatal("continuation repeated export or failed to join")
	}
}
func TestCanceledTLSHandshakeClosesActualPeerConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	entered := make(chan struct{})
	ended := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			ended <- err
			return
		}
		defer connection.Close()
		_ = connection.SetReadDeadline(time.Now().Add(3 * time.Second))
		close(entered)
		_, err = io.Copy(io.Discard, connection)
		ended <- err
	}()
	certificate := testCertificates(t, 0)
	fixture := newFixture(t, OptionsV1{LogsEndpoint: "https://" + listener.Addr().String() + "/v1/logs", TLS: &TLSV1{CA: certificate.root}}, nil)
	emitOne(t, fixture, "record", "message")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := make(chan *invocation.Receipt[Result], 1)
	go func() { receipt, _ := fixture.client.Flush(ctx, fault.Correlation{Call: "flush"}); returned <- receipt }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("TLS peer not entered")
	}
	cancel()
	receipt := <-returned
	wait, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	result, err := receipt.WaitReleased(wait)
	if err != nil || !errors.Is(result.Err(), context.Canceled) {
		t.Fatal("handshake cancellation not retained")
	}
	select {
	case err := <-ended:
		if err != nil {
			t.Fatal("peer did not observe actual connection close")
		}
	case <-wait.Done():
		t.Fatal("TLS connection leaked")
	}
	if err := fixture.assembly.Close(wait); err != nil {
		t.Fatal(err)
	}
}

func TestMalformedTLSRetainsDiagnosticWithoutConnectionHandle(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peerClosed := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		connection, err := listener.Accept()
		if err != nil {
			peerClosed <- err
			return
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(3 * time.Second))
		var hello [1]byte
		if _, err := connection.Read(hello[:]); err != nil {
			peerClosed <- err
			return
		}
		if _, err := connection.Write([]byte("NOTTL")); err != nil {
			peerClosed <- err
			return
		}
		_, err = io.Copy(io.Discard, connection)
		peerClosed <- err
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("malformed TLS peer did not terminate")
		}
	})
	certificate := testCertificates(t, 0)
	fixture := newFixture(t, OptionsV1{LogsEndpoint: "https://" + listener.Addr().String() + "/v1/logs", TLS: &TLSV1{CA: certificate.root}}, nil)
	emitOne(t, fixture, "record", "message")
	result := flushOne(t, fixture)
	var header tls.RecordHeaderError
	if !errors.Is(result.Err(), ErrExport) || !errors.As(result.Err(), &header) {
		t.Fatal("native TLS diagnostic type was not retained")
	}
	if header.Conn != nil || header.RecordHeader != [5]byte{'N', 'O', 'T', 'T', 'L'} || header.Msg == "" {
		t.Fatal("TLS diagnostic lost its fields or exposed its connection")
	}
	select {
	case err := <-peerClosed:
		if err != nil {
			t.Fatalf("TLS peer did not observe client-side closure: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("malformed TLS connection remained open")
	}
	if err := fixture.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fixture.assembly.Snapshot().Sources[0].Pending {
		t.Fatal("malformed TLS connection retained resource ownership")
	}
}

type failingCloseConnection struct {
	net.Conn
	cause error
	calls *int
}

func (connection failingCloseConnection) Close() error {
	*connection.calls++
	_ = connection.Conn.Close()
	return connection.cause
}

func TestNetworkRetainsNativeCloseFailureAndStopsFurtherAcquisition(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	network := newNetwork(defaulted(validOptions()), nil)
	cause := errors.New("private-close-canary")
	calls := 0
	connection := &ownedConnection{Conn: failingCloseConnection{left, cause, &calls}, owner: network}
	network.connections[connection] = struct{}{}
	if err := connection.Close(); !errors.Is(err, cause) {
		t.Fatal("native close failure lost")
	}
	var data [1]byte
	if _, err := right.Read(data[:]); !errors.Is(err, io.EOF) {
		t.Fatal("underlying connection still open")
	}
	done, err := network.stop(context.Background())
	if !done || !errors.Is(err, cause) || calls != 1 {
		t.Fatal("close outcome/history changed")
	}
	ctx := context.WithValue(context.Background(), networkContextKey{}, context.Background())
	if _, err := network.dialPlain(ctx, "tcp", "127.0.0.1:1"); !errors.Is(err, ErrState) {
		t.Fatal("failed network acquired another connection")
	}
}
