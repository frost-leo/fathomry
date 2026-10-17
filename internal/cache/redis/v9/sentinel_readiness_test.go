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
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	sdk "github.com/redis/go-redis/v9"
)

// This one-replica fixture gate is deliberately stricter than SDK eligibility.
// Observation precedes bind/open conservatively; Sentinel construction is lazy.
func waitReplica(ctx context.Context, monitor *sdk.SentinelClient, master, expected string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tick := time.NewTicker(30 * time.Millisecond)
	defer tick.Stop()
	var rows []map[string]string
	var rpcErr error
	for ctx.Err() == nil {
		rows, rpcErr = monitor.Replicas(ctx, master).Result()
		if ctx.Err() == nil && rpcErr == nil && len(rows) == 1 &&
			net.JoinHostPort(rows[0]["ip"], rows[0]["port"]) == expected &&
			rows[0]["flags"] == "slave" && rows[0]["master-link-status"] == "ok" {
			return nil
		}
		select {
		case <-ctx.Done():
		case <-tick.C:
		}
	}
	var row map[string]string
	if len(rows) > 0 {
		row = rows[0]
	}
	return fmt.Errorf("Sentinel replica readiness: %w (rows=%d expected_address=%t flags=%.64q master-link-status=%.16q rpc_error=%t)",
		ctx.Err(), len(rows), net.JoinHostPort(row["ip"], row["port"]) == expected,
		row["flags"], row["master-link-status"], rpcErr != nil)
}

func bindReplicaTest(t testing.TB, ctx context.Context, monitor *sdk.SentinelClient, replica string, options OptionsV1) (*Client, *invocation.Inbox[Result], error) {
	t.Helper()
	if err := waitReplica(ctx, monitor, options.MasterName, replica); err != nil {
		return nil, nil, err
	}
	client, _, inbox, _ := bindTest(t, options, 16)
	return client, inbox, nil
}

func sentinelBulk(value string) string { return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value) }
func sentinelArray(values ...string) string {
	result := fmt.Sprintf("*%d\r\n", len(values))
	for _, value := range values {
		result += sentinelBulk(value)
	}
	return result
}
func sentinelRows(rows ...map[string]string) string {
	result := fmt.Sprintf("*%d\r\n", len(rows))
	for _, row := range rows {
		var values []string
		for key, value := range row {
			values = append(values, key, value)
		}
		result += sentinelArray(values...)
	}
	return result
}
func sentinelRow(address, flags string) map[string]string {
	host, port, _ := net.SplitHostPort(address)
	return map[string]string{"ip": host, "port": port, "flags": flags, "master-link-status": "ok"}
}

type sentinelFixture struct {
	address, primary, replica      string
	queries, dataCalls, unexpected atomic.Int32
}

func newSentinelFixture(t *testing.T, rows func(string, int32) string) *sentinelFixture {
	t.Helper()
	fixture := &sentinelFixture{}
	data := func(label, role string) func(args []string) string {
		return func(args []string) string {
			switch strings.ToUpper(args[0]) {
			case "PING":
				return "+PONG\r\n"
			case "CLIENT":
				return "+OK\r\n"
			case "ROLE":
				fixture.dataCalls.Add(1)
				return sentinelArray(role)
			case "GET":
				fixture.dataCalls.Add(1)
				return sentinelBulk(label)
			}
			fixture.unexpected.Add(1)
			return "-ERR unexpected data command\r\n"
		}
	}
	fixture.primary = peer(t, data("primary", "master"))
	fixture.replica = peer(t, data("replica", "slave"))
	host, port, _ := net.SplitHostPort(fixture.primary)
	fixture.address = peer(t, func(args []string) string {
		switch strings.ToUpper(args[0]) {
		case "PING":
			return "+PONG\r\n"
		case "CLIENT":
			return "+OK\r\n"
		case "SENTINEL":
			if len(args) == 3 && args[2] == "fixture" {
				switch strings.ToUpper(args[1]) {
				case "REPLICAS":
					return rows(fixture.replica, fixture.queries.Add(1))
				case "GET-MASTER-ADDR-BY-NAME":
					return sentinelArray(host, port)
				case "SENTINELS":
					return "*0\r\n"
				}
			}
		case "SUBSCRIBE":
			var reply string
			for index, channel := range args[1:] {
				reply += ">3\r\n" + sentinelBulk("subscribe") + sentinelBulk(channel) + fmt.Sprintf(":%d\r\n", index+1)
			}
			return reply
		}
		fixture.unexpected.Add(1)
		return "-ERR unexpected Sentinel command\r\n"
	})
	t.Cleanup(func() {
		if fixture.unexpected.Load() != 0 {
			t.Error("unexpected fixture commands", fixture.unexpected.Load())
		}
	})
	return fixture
}

func sentinelMonitor(t *testing.T, address string) *sdk.SentinelClient {
	t.Helper()
	monitor := sdk.NewSentinelClient(&sdk.Options{Addr: address, MaxRetries: -1, DialerRetries: 1,
		DisableIdentity: true, ContextTimeoutEnabled: true})
	t.Cleanup(func() {
		if err := monitor.Close(); err != nil {
			t.Error(err)
		}
	})
	return monitor
}
func replicaSettings(fixture *sentinelFixture, mode string) OptionsV1 {
	options := testOptions(fixture.address)
	options.Mode = mode
	if mode == "universal" {
		options.UniversalMode = "sentinel"
	}
	options.MasterName = "fixture"
	options.ReadOnly = true
	options.AdminCommands = []string{"ROLE"}
	options.AllowedAddrs = []string{fixture.address, fixture.primary, fixture.replica}
	return options
}

func TestSentinelReplicaReadiness(t *testing.T) {
	for _, entry := range []struct {
		name, field, value string
	}{
		{"ready", "", ""},
		{"empty", "", ""},
		{"nil-row", "", ""},
		{"multiple", "", ""},
		{"rpc-error", "", ""},
		{"wrong-host", "ip", "192.0.2.1"},
		{"wrong-port", "port", "1"},
		{"missing-ip", "ip", ""},
		{"missing-port", "port", ""},
		{"disconnected", "flags", "slave,disconnected"},
		{"subjectively-down", "flags", "slave,s_down"},
		{"objectively-down", "flags", "slave,o_down"},
		{"missing-flags", "flags", ""},
		{"unknown-flags", "flags", "slave,unknown"},
		{"missing-link", "master-link-status", ""},
		{"broken-link", "master-link-status", "err"},
		{"unknown-link", "master-link-status", "unknown"},
	} {
		var fixture *sentinelFixture
		var monitor *sdk.SentinelClient
		t.Run(entry.name, func(t *testing.T) {
			fixture = newSentinelFixture(t, func(address string, _ int32) string {
				row := sentinelRow(address, "slave")
				row["private-field"] = "private-marker"
				switch entry.name {
				case "empty":
					return sentinelRows()
				case "nil-row":
					return sentinelRows(nil)
				case "multiple":
					return sentinelRows(row, row)
				case "rpc-error":
					return "-ERR private-marker\r\n"
				}
				if entry.field != "" {
					if entry.value == "" {
						delete(row, entry.field)
					} else {
						row[entry.field] = entry.value
					}
				}
				return sentinelRows(row)
			})
			monitor = sentinelMonitor(t, fixture.address)
			budget := 150 * time.Millisecond
			if entry.name == "ready" {
				budget = 3 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			start := time.Now()
			reader, evidence, err := bindReplicaTest(t, ctx, monitor, fixture.replica, replicaSettings(fixture, "sentinel"))
			if entry.name == "ready" {
				if err != nil {
					t.Fatal(err)
				}
				role := executeTest(t, reader, "first-role", "ROLE")
				if role.Err() != nil {
					t.Fatal(role.Err())
				}
				name, _ := role.Outcome.Value.Commands()[0].Value().Elements()[0].Text()
				drain(t, evidence)
				if name != "slave" {
					t.Fatal("first routed ROLE did not reach the replica", name)
				}
				return
			}
			if !errors.Is(err, context.DeadlineExceeded) || reader != nil || evidence != nil {
				t.Fatal("unready fixture opened a reader or lost its deadline", err)
			}
			if time.Since(start) > 2*time.Second || fixture.queries.Load() == 0 || fixture.dataCalls.Load() != 0 {
				t.Fatal("unready fixture exceeded its guard or routed a data command")
			}
			for _, field := range []string{"rows=", "expected_address=", "flags=", "master-link-status=", "rpc_error="} {
				if !strings.Contains(err.Error(), field) {
					t.Fatal("missing last-state diagnostic", field, err)
				}
			}
			if strings.Contains(err.Error(), "private-marker") {
				t.Fatal("readiness diagnostic leaked unselected fields or RPC text")
			}
		})
		if fixture == nil || monitor == nil {
			continue
		}
		if err := monitor.Ping(context.Background()).Err(); !errors.Is(err, sdk.ErrClosed) {
			t.Error("monitor was not closed", err)
		}
		for _, address := range []string{fixture.address, fixture.primary, fixture.replica} {
			conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				t.Error("fixture listener survived cleanup")
			}
		}
	}
}

func TestSentinelReplicaTransitionBeforeOpen(t *testing.T) {
	for _, mode := range []string{"sentinel", "universal"} {
		t.Run(mode, func(t *testing.T) {
			fixture := newSentinelFixture(t, func(address string, query int32) string {
				flags := "slave,disconnected"
				if query >= 3 {
					flags = "slave"
				}
				return sentinelRows(sentinelRow(address, flags))
			})
			monitor := sentinelMonitor(t, fixture.address)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			reader, evidence, err := bindReplicaTest(t, ctx, monitor, fixture.replica, replicaSettings(fixture, mode))
			if err != nil {
				t.Fatal(err)
			}
			if fixture.queries.Load() < 3 || fixture.dataCalls.Load() != 0 {
				t.Error("reader opened before healthy observation or construction routed a data command")
			}
			role := executeTest(t, reader, "first-role", "ROLE")
			if role.Err() != nil {
				t.Fatal(role.Err())
			}
			name, _ := role.Outcome.Value.Commands()[0].Value().Elements()[0].Text()
			drain(t, evidence)
			if name != "slave" {
				t.Fatal("first routed ROLE did not reach the replica", name)
			}
		})
	}
}

func TestSentinelReplicaWaitDeadline(t *testing.T) {
	release := make(chan struct{})
	fixture := newSentinelFixture(t, func(address string, _ int32) string {
		<-release
		return sentinelRows(sentinelRow(address, "slave"))
	})
	monitor := sentinelMonitor(t, fixture.address)
	// Release the blocked peer before its cleanup joins connection goroutines.
	t.Cleanup(func() { close(release) })
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	reader, evidence, err := bindReplicaTest(t, ctx, monitor, fixture.replica, replicaSettings(fixture, "sentinel"))
	if !errors.Is(err, context.DeadlineExceeded) || reader != nil || evidence != nil ||
		time.Since(start) > 2*time.Second || fixture.queries.Load() == 0 {
		t.Fatal("in-flight Sentinel RPC escaped the readiness deadline", err)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	before := fixture.queries.Load()
	reader, evidence, err = bindReplicaTest(t, canceled, monitor, fixture.replica, replicaSettings(fixture, "sentinel"))
	if !errors.Is(err, context.Canceled) || reader != nil || evidence != nil || fixture.queries.Load() != before {
		t.Fatal("canceled readiness opened a reader or sent another RPC", err)
	}
}

// The pinned SDK can drop a temporary Sentinel client when no replica is eligible.
// Own all reference-client sockets, including those not reached by Client.Close.
func sentinelReference(t testing.TB, address string) *sdk.Client {
	t.Helper()
	lifetime, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	var dials sync.WaitGroup
	var connections []net.Conn
	direct := sdk.NewFailoverClient(&sdk.FailoverOptions{MasterName: "fixture", SentinelAddrs: []string{address},
		ReplicaOnly: true, MaxRetries: -1, DialerRetries: 1, DisableIdentity: true, ContextTimeoutEnabled: true,
		Dialer: func(ctx context.Context, network, address string) (net.Conn, error) {
			mu.Lock()
			if lifetime.Err() != nil {
				mu.Unlock()
				return nil, net.ErrClosed
			}
			dials.Add(1)
			mu.Unlock()
			defer dials.Done()
			ctx, stop := context.WithCancel(ctx)
			stopCancel := context.AfterFunc(lifetime, stop)
			defer stopCancel()
			defer stop()
			connection, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if connection != nil {
				mu.Lock()
				connections = append(connections, connection)
				mu.Unlock()
			}
			return connection, err
		}})
	t.Cleanup(func() {
		mu.Lock()
		cancel()
		mu.Unlock()
		if err := direct.Close(); err != nil {
			t.Error(err)
		}
		dials.Wait()
		for _, connection := range connections {
			_ = connection.Close()
		}
	})
	return direct
}

func TestSentinelReferenceCleanup(t *testing.T) {
	var connection net.Conn
	t.Run("unpooled-dial", func(t *testing.T) {
		fixture := newSentinelFixture(t, func(string, int32) string { return sentinelRows() })
		direct := sentinelReference(t, fixture.address)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var err error
		connection, err = direct.Options().Dialer(ctx, "tcp", "")
		if err != nil {
			t.Fatal(err)
		}
	})
	if connection == nil {
		t.Fatal("reference dial did not return a connection")
	}
	if _, err := connection.Write([]byte("*1\r\n$4\r\nPING\r\n")); !errors.Is(err, net.ErrClosed) {
		t.Fatal("unpooled reference socket survived fixture cleanup", err)
	}
}

func TestSentinelReplicaFallback(t *testing.T) {
	for _, mode := range []string{"sentinel", "universal"} {
		for _, flags := range []string{"none", "slave,disconnected", "slave,s_down", "slave,o_down", "slave"} {
			t.Run(mode+"/"+flags, func(t *testing.T) {
				var healthy atomic.Bool
				fixture := newSentinelFixture(t, func(address string, _ int32) string {
					if healthy.Load() {
						return sentinelRows(sentinelRow(address, "slave"))
					}
					if flags == "none" {
						return sentinelRows()
					}
					return sentinelRows(sentinelRow(address, flags))
				})
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				direct := sentinelReference(t, fixture.address)
				reader, _, evidence, _ := bindTest(t, replicaSettings(fixture, mode), 16)
				if fixture.queries.Load() != 0 || fixture.dataCalls.Load() != 0 {
					t.Fatal("Sentinel construction unexpectedly routed a command")
				}
				want := "primary"
				if flags == "slave" {
					want = "replica"
				}
				for read := range 2 {
					value, err := direct.Get(ctx, "route").Result()
					if err != nil || value != want {
						t.Fatal("direct SDK route changed", value, err)
					}
					result := executeTest(t, reader, fmt.Sprintf("route-%d", read), "GET", "route")
					if result.Err() != nil {
						t.Fatal(result.Err())
					}
					value, _ = result.Outcome.Value.Commands()[0].Value().Text()
					drain(t, evidence)
					if value != want {
						t.Fatal("wrapper route changed", value)
					}
					healthy.Store(true)
					t.Logf("flags=%q read=%d SDK/wrapper=%s", flags, read+1, want)
				}
			})
		}
	}
}
