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

package endpoint

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

func TestHostAdmission(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "::1", "::ffff:127.0.0.1"} {
		if !ValidHost(host, false) || !ValidHost(host, true) {
			t.Fatal("literal rejected", host)
		}
	}
	for _, host := range []string{"localhost", "postgres", "postgres.db.svc.cluster.local", "MySQL.internal.", strings.Repeat("a", 63) + ".internal"} {
		if ValidHost(host, false) || !ValidHost(host, true) {
			t.Fatal("DNS opt-in changed", host)
		}
	}
	for _, host := range []string{"", "0.0.0.0", "::", "::ffff:0.0.0.0", "fe80::1%eth0", "[::1]", "localhost:5432", "db/path", "user@host", "db host", "db\n", "_db", "-db", "db-", "db..local", ".", strings.Repeat("a", 64), strings.Repeat("abcd.", 51), "数据库"} {
		if ValidHost(host, false) || ValidHost(host, true) {
			t.Fatal("invalid address accepted", host)
		}
	}
}
func dnsFixture(t *testing.T, address netip.Addr, drop bool) (*net.Resolver, <-chan struct{}) {
	t.Helper()
	server, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 1)
	var worker sync.WaitGroup
	worker.Add(1)
	go func() {
		defer worker.Done()
		data := make([]byte, 4096)
		for {
			count, peer, err := server.ReadFrom(data)
			if err != nil {
				return
			}
			select {
			case entered <- struct{}{}:
			default:
			}
			if drop {
				continue
			}
			var query dnsmessage.Message
			if query.Unpack(data[:count]) != nil {
				continue
			}
			response := dnsmessage.Message{Header: dnsmessage.Header{ID: query.Header.ID, Response: true, RecursionDesired: query.Header.RecursionDesired, RecursionAvailable: true}, Questions: query.Questions}
			for _, question := range query.Questions {
				if question.Type == dnsmessage.TypeA {
					response.Answers = append(response.Answers, dnsmessage.Resource{Header: dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: 1}, Body: &dnsmessage.AResource{A: address.As4()}})
				}
			}
			reply, err := response.Pack()
			if err == nil {
				_, _ = server.WriteTo(reply, peer)
			}
		}
	}()
	t.Cleanup(func() { _ = server.Close(); worker.Wait() })
	return &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "udp", server.LocalAddr().String())
	}}, entered
}
func TestResolverCandidatesAndCancellation(t *testing.T) {
	for _, valid := range []bool{true, false} {
		t.Run(map[bool]string{true: "specified", false: "unspecified"}[valid], func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			_, port, _ := net.SplitHostPort(listener.Addr().String())
			target := "127.0.0.1"
			if !valid {
				target = "0.0.0.0"
			}
			resolver, _ := dnsFixture(t, netip.MustParseAddr(target), false)
			dialer := newDialer()
			dialer.Resolver = resolver
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			connection, err := dialer.DialContext(ctx, "tcp4", "database.fixture.invalid.:"+port)
			if !valid {
				if connection != nil {
					connection.Close()
					t.Fatal("unspecified DNS target connected")
				}
				if !errors.Is(err, errAddress) {
					t.Fatal("resolved target admission was bypassed", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			accepted, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}
			accepted.Close()
			if connection.RemoteAddr().String() != listener.Addr().String() {
				t.Fatal("wrong physical peer")
			}
		})
	}
	resolver, entered := dnsFixture(t, netip.MustParseAddr("127.0.0.1"), true)
	dialer := newDialer()
	dialer.Resolver = resolver
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		connection, err := dialer.DialContext(ctx, "tcp", "database.fixture.invalid.:5432")
		if connection != nil {
			connection.Close()
		}
		finished <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("DNS was not entered")
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("DNS cancellation lost", err)
		}
	case <-time.After(time.Second):
		t.Fatal("DNS cancellation did not return")
	}
}
func FuzzValidHost(f *testing.F) {
	for _, value := range []string{"127.0.0.1", "mysql.db.svc.cluster.local", "::", "db..", "db."} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, host string) {
		if ValidHost(host, false) && !ValidHost(host, true) {
			t.Fatal("DNS changed literal admission")
		}
		if !ValidHost(host, true) {
			return
		}
		if _, err := netip.ParseAddr(host); err == nil {
			return
		}
		name := strings.TrimSuffix(host, ".")
		if len(name) == 0 || len(name) > 253 || len(host) > 254 {
			t.Fatal("hostname bound bypassed")
		}
	})
}
