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
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/redis/go-redis/v9"
)

func TestMain(m *testing.M) {
	DisableNativeLogging()
	os.Exit(m.Run())
}

func peer(t testing.TB, respond func([]string) string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return servePeer(t, listener, respond)
}

func servePeer(t testing.TB, listener net.Listener, respond func([]string) string) string {
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
					var reply string
					if strings.EqualFold(args[0], "hello") {
						reply = "%2\r\n+proto\r\n:3\r\n+version\r\n+8.8.0\r\n"
					} else {
						reply = respond(args)
					}
					if reply == "" {
						return
					}
					if _, err := io.WriteString(connection, reply); err != nil {
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
		size, parseErr := strconv.Atoi(strings.TrimSpace(line[1:]))
		if parseErr != nil || size < 0 || size > 16<<20 {
			return nil, errors.New("invalid fixture size")
		}
		value := make([]byte, size+2)
		if _, err = io.ReadFull(reader, value); err != nil {
			return nil, err
		}
		args[index] = string(value[:size])
	}
	return args, nil
}

func testOptions(address string) OptionsV1 {
	return OptionsV1{Name: "cache", Mode: "standalone", Addrs: []string{address}, Plaintext: true, MaxCommands: 8,
		MaxReplyBytes: 64 << 10, MaxReplyElements: 4096, Timeout: time.Second,
		Commands:      []string{"PING", "GET", "SET", "INCR", "DEL", "SCAN", "HSET", "HGETALL", "LPUSH", "BLPOP", "XADD", "XRANGE", "PUBLISH", "TTL", "PTTL", "EVAL", "EVALSHA"},
		AllowSessions: true, AllowSubscriptions: true}
}

func bindTest(t testing.TB, options OptionsV1, capacity int) (*Client, *resource.Assembly, *invocation.Inbox[Result], resource.Selection[Source]) {
	t.Helper()
	selected, err := Select(options)
	if err != nil {
		t.Fatalf("selection: %v", err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(context.Background(), context.Background(), "redis-test", selected)
	if err != nil {
		t.Fatalf("assembly: %v", err)
	}
	inbox, err := invocation.NewInbox[Result](capacity, defaults(options).evidenceReservation()*int64(capacity))
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := assembly.Close(cleanup); err != nil {
			t.Errorf("resource cleanup: %v", err)
		}
	})
	return client, assembly, inbox, selected
}

func resolved(t testing.TB, receipt *invocation.Receipt[Result], err error) invocation.Result[Result] {
	t.Helper()
	if err != nil {
		t.Fatalf("call setup: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := receipt.WaitFinal(ctx)
	if err != nil {
		t.Fatalf("completion: %v", err)
	}
	return result
}

func executeTest(t testing.TB, client *Client, id string, args ...string) invocation.Result[Result] {
	t.Helper()
	receipt, err := client.Execute(context.Background(), fault.Correlation{Call: id}, NewCommand(args...))
	return resolved(t, receipt, err)
}

func drain(t testing.TB, inbox *invocation.Inbox[Result]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for inbox.Usage().Outstanding > 0 {
		delivery, err := inbox.Next(ctx)
		if err != nil {
			t.Fatalf("inbox next: %v", err)
		}
		if _, err = delivery.Receipt().WaitReleased(ctx); err != nil {
			t.Fatalf("inbox pending: %v", err)
		}
		if err = delivery.Release(); err != nil {
			t.Fatalf("inbox release: %v", err)
		}
	}
}

func TestMaintenanceSelectionAndIdlePoolLifetime(t *testing.T) {
	var maintenanceCalls atomic.Int32
	address := peer(t, func(args []string) string {
		if strings.EqualFold(args[0], "CLIENT") && len(args) > 1 && strings.EqualFold(args[1], "MAINT_NOTIFICATIONS") {
			maintenanceCalls.Add(1)
		}
		return "+OK\r\n"
	})
	options := testOptions(address)
	options.MaintenanceMode = "enabled"
	options.MaxIdleTime = 20 * time.Millisecond
	client, assembly, inbox, _ := bindTest(t, options, 4)
	for index := 0; index < 2; index++ {
		got := executeTest(t, client, fmt.Sprintf("maintenance-%d", index), "PING")
		if got.Err() != nil {
			t.Fatal(got.Err())
		}
		drain(t, inbox)
	}
	if maintenanceCalls.Load() != 1 || client.Stats().Hits == 0 {
		t.Fatal("maintenance handshake or native connection reuse missing")
	}
	time.Sleep(40 * time.Millisecond)
	got := executeTest(t, client, "expired", "PING")
	if got.Err() != nil {
		t.Fatal(got.Err())
	}
	drain(t, inbox)
	if maintenanceCalls.Load() != 2 {
		t.Fatal("idle native connection not replaced")
	}
	if err := assembly.Close(context.Background()); err != nil || client.Stats().Sockets != 0 {
		t.Fatal("pool termination not confirmed")
	}
}

func TestSocketSaturationDoesNotVoteRingDown(t *testing.T) {
	left := peer(t, func([]string) string { return "+PONG\r\n" })
	right := peer(t, func([]string) string { return "+PONG\r\n" })
	options := testOptions(left)
	options.Mode = "ring"
	options.Addrs = nil
	options.Shards = map[string]string{"left": left, "right": right}
	options.AllowSessions = false
	options.AllowSubscriptions = false
	options.MaxActive = 1
	options.MaxConnections = 1
	if _, err := Select(options); err == nil {
		t.Fatal("insufficient persistent topology headroom accepted")
	}
	options.MaxConnections = 2
	client, _, _, _ := bindTest(t, options, 4)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, err := client.owner.transport.dial(ctx, "tcp", left)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := client.owner.transport.dial(ctx, "tcp", right)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	node, err := client.owner.native.(*sdk.Ring).GetShardClientForKey("owned")
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		if !ringHeartbeat(ctx, node, true) {
			t.Fatal("local quota incorrectly counted as downstream failure")
		}
		if ringHeartbeat(ctx, node, false) {
			t.Fatal("local quota incorrectly revived a down shard")
		}
	}
}
func TestWarmPoolLeavesDedicatedConnectionHeadroom(t *testing.T) {
	address := peer(t, func([]string) string { return "+OK\r\n" })
	options := testOptions(address)
	options.MaxActive = 1
	options.MaxConnections = 1
	if _, err := Select(options); err == nil {
		t.Fatal("configuration can permanently starve dedicated work")
	}
	options.MaxConnections = 2
	client, _, inbox, _ := bindTest(t, options, 4)
	got := executeTest(t, client, "warm", "PING")
	if got.Err() != nil {
		t.Fatal(got.Err())
	}
	drain(t, inbox)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	receipt, err := client.Dedicated(ctx, context.Background(), fault.Correlation{Call: "parent"}, "owned", func(ctx context.Context, session *Session) error {
		child, err := session.Execute(ctx, fault.Correlation{Call: "child", Parent: "parent"}, NewCommand("PING"))
		return resultError(child, err)
	})
	got = resolved(t, receipt, err)
	if got.Err() != nil {
		t.Fatal(got.Err())
	}
	drain(t, inbox)
}
