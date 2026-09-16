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

package socks4

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"testing"
	"time"
)

type fathomryForward struct{ conn net.Conn }

func (forward fathomryForward) Dial(string, string) (net.Conn, error) { return forward.conn, nil }
func (forward fathomryForward) DialContext(context.Context, string, string) (net.Conn, error) {
	return forward.conn, nil
}

func TestFathomrySOCKS4PerRouteIdentityAndFragmentedReply(t *testing.T) {
	for _, identity := range []string{"first", "second", ""} {
		t.Run(identity, func(t *testing.T) {
			client, server := net.Pipe()
			defer client.Close()
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			got := make(chan string, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				reader := bufio.NewReader(server)
				var head [8]byte
				if _, err := io.ReadFull(reader, head[:]); err != nil {
					return
				}
				user, err := reader.ReadString(0)
				if err != nil {
					return
				}
				got <- user[:len(user)-1]
				for _, value := range []byte{0, accessGranted, 0, 80, 127, 0, 0, 1} {
					if _, err := server.Write([]byte{value}); err != nil {
						return
					}
				}
			}()
			address, _ := url.Parse("socks4://" + identity + "@127.0.0.1:1")
			dialer := socks4{url: address, dialer: fathomryForward{client}}
			conn, err := dialer.DialContext(ctx, "tcp", "127.0.0.1:80")
			if err != nil {
				t.Error("valid fragmented reply rejected", err)
			}
			if conn != nil {
				conn.Close()
			}
			server.Close()
			<-done
			if actual := <-got; actual != identity {
				t.Errorf("per-route USERID lost: %q", actual)
			}
		})
	}
}
func TestFathomrySOCKS4EarlyFailureClosesAcquiredConnection(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	address, _ := url.Parse("socks4://owner@127.0.0.1:1")
	dialer := socks4{url: address, dialer: fathomryForward{client}}
	conn, err := dialer.DialContext(context.Background(), "tcp", "invalid-target")
	if err == nil || conn != nil {
		t.Fatal("invalid target unexpectedly accepted")
	}
	server.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	var buffer [1]byte
	if _, err := server.Read(buffer[:]); !errors.Is(err, io.EOF) {
		t.Fatal("acquired connection survived early failure", err)
	}
}
