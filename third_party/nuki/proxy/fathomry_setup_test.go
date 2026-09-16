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
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	http "github.com/nukilabs/http"
)

type gatedDeadlineConn struct {
	net.Conn
	entered chan struct{}
	release chan struct{}
	cause   error
	once    sync.Once
}

func (conn *gatedDeadlineConn) SetDeadline(deadline time.Time) error {
	if !deadline.IsZero() && deadline.Before(time.Now()) {
		conn.once.Do(func() { close(conn.entered) })
		<-conn.release
		return conn.cause
	}
	return conn.Conn.SetDeadline(deadline)
}

func TestCONNECTCancellationJoinsDeadlineCallback(t *testing.T) {
	raw, peer := net.Pipe()
	defer raw.Close()
	defer peer.Close()
	_ = peer.SetDeadline(time.Now().Add(2 * time.Second))
	cause := errors.New("deadline-callback-failed")
	owned := &gatedDeadlineConn{Conn: raw, entered: make(chan struct{}), release: make(chan struct{}), cause: cause}
	var unblock sync.Once
	defer unblock.Do(func() { close(owned.release) })
	requestRead, responseReady := make(chan struct{}), make(chan struct{})
	peerDone := make(chan error, 1)
	go func() {
		_, err := http.ReadRequest(bufio.NewReader(peer))
		close(requestRead)
		<-responseReady
		if err == nil {
			_, err = io.WriteString(peer, "HTTP/1.1 200 Connection Established\r\n\r\n")
		}
		peerDone <- err
	}()
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	result := make(chan error, 1)
	go func() {
		dialer := &Dialer{maxHeader: 1024}
		request := &http.Request{Method: "CONNECT", URL: &url.URL{Host: "origin.invalid:80"}, Host: "origin.invalid:80", Header: make(http.Header)}
		conn, err := dialer.connectHttp1(ctx, request, owned)
		if conn != nil {
			_ = conn.Close()
		}
		result <- err
	}()
	<-requestRead
	cancelCause := errors.New("cancel-connect-setup")
	cancel(cancelCause)
	<-owned.entered
	close(responseReady)
	if err := <-peerDone; err != nil {
		t.Fatal(err)
	}
	var early bool
	var err error
	select {
	case err = <-result:
		early = true
	case <-time.After(10 * time.Millisecond):
	}
	unblock.Do(func() { close(owned.release) })
	if early {
		t.Error("CONNECT transferred ownership before cancellation callback returned")
	} else {
		err = <-result
	}
	if !errors.Is(err, cancelCause) || !errors.Is(err, cause) {
		t.Error("cancellation or deadline cause disappeared", err)
	}
}

func TestCONNECTTimeoutJoinsStreamCancellation(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	cause := errors.New("stream-cancellation-failed")
	finish := watchSetup(context.Background(), time.Millisecond, func() error {
		close(entered)
		<-release
		return cause
	})
	<-entered
	result := make(chan error, 1)
	go func() { result <- finish() }()
	var early bool
	var err error
	select {
	case err = <-result:
		early = true
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if early {
		t.Error("setup timeout callback was not joined")
	} else {
		err = <-result
	}
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, cause) {
		t.Error("timeout causes disappeared", err)
	}
}
