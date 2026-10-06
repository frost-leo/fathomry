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

package redis

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestMain(m *testing.M) {
	if os.Getenv("FATHOMRY_REDIS_LOG_TEST") == "" {
		DisableNativeLogging()
	}
	os.Exit(m.Run())
}
func testContext(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func peer(t testing.TB, respond func(net.Conn, []string) string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return servePeer(t, listener, respond)
}
func servePeer(t testing.TB, listener net.Listener, respond func(net.Conn, []string) string) string {
	t.Helper()
	var workers sync.WaitGroup
	var mu sync.Mutex
	connections := map[net.Conn]bool{}
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			connections[connection] = true
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer connection.Close()
				defer func() { mu.Lock(); delete(connections, connection); mu.Unlock() }()
				reader := bufio.NewReader(connection)
				for {
					args, err := readArgs(reader)
					if err != nil {
						return
					}
					var response string
					if strings.EqualFold(args[0], "hello") {
						response = "%2\r\n+proto\r\n:3\r\n+version\r\n+8.8.0\r\n"
					} else {
						response = respond(connection, args)
					}
					if response == "" {
						return
					}
					if _, err := io.WriteString(connection, response); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		for connection := range connections {
			_ = connection.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	return listener.Addr().String()
}
func readArgs(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 4 || line[0] != '*' {
		return nil, errors.New("invalid fixture command")
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil || count < 1 || count > 65536 {
		return nil, errors.New("invalid fixture count")
	}
	args := make([]string, count)
	for index := range args {
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if len(line) < 4 || line[0] != '$' {
			return nil, errors.New("invalid fixture argument")
		}
		size, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil || size < 0 || size > 16<<20 {
			return nil, errors.New("invalid fixture size")
		}
		data := make([]byte, size+2)
		if _, err = io.ReadFull(reader, data); err != nil {
			return nil, err
		}
		args[index] = string(data[:size])
	}
	return args, nil
}
func testSettings(address string) Settings {
	return Settings{Name: "fixture", Mode: "standalone", Addrs: []string{address}, Plaintext: true, MaxActive: 4, MaxCommands: 8,
		MaxRequestBytes: 64 << 10, MaxReplyBytes: 64 << 10, MaxReplyElements: 256, Timeout: time.Second,
		Commands:      []string{"PING", "GET", "SET", "INCR", "DEL", "SCAN", "HSET", "HGETALL", "LPUSH", "BLPOP", "XADD", "XRANGE", "PUBLISH", "TTL", "PTTL", "EVAL", "EVALSHA"},
		AllowSessions: true, AllowSubscriptions: true}
}
func openTest(t testing.TB, value Settings) (*Owner, Dependencies) {
	t.Helper()
	prepared, err := Prepare(value)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	deps := Dependencies{Runtime: runtime, Evidence: inbox}
	owner, err := prepared.Open(context.Background(), deps)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := owner.Close(ctx); err != nil {
			t.Error(err)
		}
		if !owner.ShutdownComplete() {
			t.Error("source not released")
		}
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
		status, _ := inbox.Inspect()
		ack(t, inbox, status.Outstanding)
	})
	return owner, deps
}
func ack(t testing.TB, inbox *adapters.Inbox[Result], count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for range count {
		delivery, err := inbox.NextReleased(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}
func command(t testing.TB, capability Capability, args ...string) Command {
	t.Helper()
	value, err := NewCommand(capability, args...)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
