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
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"golang.org/x/net/dns/dnsmessage"
)

func TestResolverConfiguredRouteOwnsDNSConnections(t *testing.T) {
	dns, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		packet := make([]byte, 4096)
		for {
			count, address, err := dns.ReadFrom(packet)
			if err != nil {
				return
			}
			var query dnsmessage.Message
			if err := query.Unpack(packet[:count]); err != nil {
				continue
			}
			response := dnsmessage.Message{Header: dnsmessage.Header{ID: query.ID, Response: true, RecursionAvailable: true}, Questions: query.Questions}
			for _, question := range query.Questions {
				if question.Type == dnsmessage.TypeA {
					response.Answers = append(response.Answers, dnsmessage.Resource{Header: dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET},
						Body: &dnsmessage.AResource{A: [4]byte{127, 0, 0, 1}}})
				}
			}
			encoded, err := response.Pack()
			if err == nil {
				_, _ = dns.WriteTo(encoded, address)
			}
		}
	}()
	defer func() { _ = dns.Close(); <-done }()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "resolved") }))
	defer peer.Close()
	target, _ := url.Parse(peer.URL)
	_, port, _ := net.SplitHostPort(target.Host)
	target.Host = net.JoinHostPort("resolver.invalid.", port)
	var dials atomic.Int64
	options := providerOptions()
	options.Mode = HTTP1Only
	options.Native.Resolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, "udp", dns.LocalAddr().String())
	}}
	fix := bindProvider(t, options)
	receipt, err := fix.client.Do(testContext(t), fault.Correlation{Call: "resolver"}, nativeRequest(t, "GET", target.String(), nil))
	if err != nil {
		t.Fatal(err)
	}
	result := outcome(t, fix, receipt)
	if result.Err() != nil || !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "resolved" || dials.Load() == 0 {
		t.Fatal("explicit resolver route", result.Err(), dials.Load())
	}
	if err := fix.assembly.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	fix.client.owner.mu.Lock()
	count := len(fix.client.owner.sockets)
	fix.client.owner.mu.Unlock()
	if count != 0 {
		t.Fatal("DNS/HTTP physical socket retained", count)
	}
}

type heldResolverRead struct {
	net.Conn
	entered, allowed, closed chan struct{}
	once                     sync.Once
}

func (conn *heldResolverRead) Read([]byte) (int, error) {
	close(conn.entered)
	<-conn.closed
	<-conn.allowed
	return 0, net.ErrClosed
}
func (conn *heldResolverRead) Close() error {
	conn.once.Do(func() { close(conn.closed) })
	return conn.Conn.Close()
}

func TestResolverCloseJoinsAlreadyEnteredRead(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	raw := &heldResolverRead{Conn: local, entered: make(chan struct{}), allowed: make(chan struct{}), closed: make(chan struct{})}
	release := sync.OnceFunc(func() { close(raw.allowed) })
	defer release()
	options := providerOptions()
	options.Native.Resolver = &net.Resolver{Dial: func(context.Context, string, string) (net.Conn, error) { return raw, nil }}
	own, err := newOwner(defaults(options), options.Native)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := own.native.Resolver.Dial(context.Background(), "tcp", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	readDone := make(chan struct{})
	go func() { defer close(readDone); _, _ = conn.Read(make([]byte, 1)) }()
	<-raw.entered
	closeDone := make(chan error, 1)
	go func() { closeDone <- conn.Close() }()
	<-raw.closed
	own.mu.Lock()
	retained := len(own.sockets)
	own.mu.Unlock()
	if retained != 1 {
		t.Fatal("entered DNS read lost socket reservation")
	}
	select {
	case err := <-closeDone:
		t.Fatal("Close returned before entered read", err)
	default:
	}
	release()
	select {
	case err := <-closeDone:
		if err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not join read")
	}
	<-readDone
	result := own.release(testContext(t))
	if !result.Released || result.Err != nil {
		t.Fatal("source release", result.Err)
	}
}
