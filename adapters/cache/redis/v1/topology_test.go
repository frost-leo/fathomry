//go:build redis_service

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
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	sdk "github.com/redis/go-redis/v9"
)

type testNode struct {
	address string
	process *exec.Cmd
	done    chan error
	once    sync.Once
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return port
}
func startServer(t *testing.T, extra string, sentinel bool) *testNode {
	t.Helper()
	binary := os.Getenv("FATHOMRY_REDIS_TEST_SERVER")
	if !filepath.IsAbs(binary) {
		t.Fatal("an explicitly authorized absolute Redis test executable is required")
	}
	directory := t.TempDir()
	port := freePort(t)
	logPath := filepath.Join(directory, "server.log")
	text := fmt.Sprintf("bind 127.0.0.1\nport %d\nprotected-mode yes\nsave \"\"\nappendonly no\ndir %s\nlogfile %s\n", port, directory, logPath) + extra
	path := filepath.Join(directory, "redis.conf")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{path}
	if sentinel {
		args = append(args, "--sentinel")
	}
	process := exec.Command(binary, args...)
	node := &testNode{address: fmt.Sprintf("127.0.0.1:%d", port), process: process, done: make(chan error, 1)}
	if err := process.Start(); err != nil {
		t.Fatal("owned test server failed to start")
	}
	go func() { node.done <- process.Wait() }()
	t.Cleanup(func() {
		node.stop(t)
		if !t.Failed() {
			return
		}
		log, err := os.Open(logPath)
		if err != nil {
			t.Log("owned loopback Redis server log unavailable")
			return
		}
		defer log.Close()
		content, err := io.ReadAll(io.LimitReader(log, 64<<10))
		if err != nil {
			t.Log("owned loopback Redis server log could not be read")
			return
		}
		t.Logf("Owned loopback Redis server log (first 64 KiB):\n%s", content)
	})
	probe := sdk.NewClient(&sdk.Options{Addr: node.address, MaxRetries: -1, DialerRetries: 1, DialTimeout: 100 * time.Millisecond, ReadTimeout: 100 * time.Millisecond, DisableIdentity: true})
	defer probe.Close()
	until(t, 5*time.Second, func() bool {
		err := probe.Ping(context.Background()).Err()
		return err == nil || sdk.HasErrorPrefix(err, "NOAUTH")
	})
	return node
}
func (node *testNode) stop(t *testing.T) {
	t.Helper()
	node.once.Do(func() {
		_ = node.process.Process.Signal(os.Interrupt)
		select {
		case <-node.done:
		case <-time.After(5 * time.Second):
			_ = node.process.Process.Kill()
			<-node.done
			t.Error("owned test server required forced termination")
		}
	})
}
func topologySettings(address string) Settings {
	value := testSettings(address)
	value.Timeout = 2 * time.Second
	value.MaxConnections = 24
	return value
}
func TestPublicRedisTopologies(t *testing.T) {
	ctx := serviceContext(t)
	prefix := namespace(t)
	t.Run("standalone-universal-ring-node-local-scan", func(t *testing.T) {
		first, second := startServer(t, "", false), startServer(t, "", false)
		left, right := serviceOracle(t, topologySettings(first.address)), serviceOracle(t, topologySettings(second.address))
		for _, mode := range []string{"standalone", "universal", "ring"} {
			options := topologySettings(first.address)
			options.Mode = mode
			if mode == "universal" {
				options.UniversalMode = "standalone"
			}
			if mode == "ring" {
				options.Addrs = nil
				options.Shards = map[string]string{"first": first.address, "second": second.address}
				options.MaintenanceMode = "auto"
			}
			owner, deps := openTest(t, options)
			for index := range 20 {
				target := fmt.Sprintf("%s%s:%d", prefix, mode, index)
				_, err := owner.Client().Cache().Execute(ctx, command(t, Cache, "SET", target, "written"))
				serviceOK(t, err)
				ack(t, deps.Evidence, 1)
			}
			if mode == "ring" {
				count, err := right.DBSize(ctx).Result()
				serviceOK(t, err)
				if count == 0 {
					t.Fatal("Ring failed to route to second independent shard")
				}
				if _, err := owner.Client().Cache().Execute(ctx, command(t, Cache, "SCAN", "0")); !errors.Is(err, ErrAuthority) {
					t.Fatal("topology scan implied full coverage")
				}
				ack(t, deps.Evidence, 1)
				_, err = owner.Client().Cache().Dedicated(ctx, context.Background(), prefix+"ring:1", func(ctx context.Context, session *Session) error {
					page, err := session.Execute(ctx, command(t, Cache, "SCAN", "0", "MATCH", prefix+"*", "COUNT", "5"))
					if err != nil {
						return err
					}
					if len(page.Replies()[0].Value().Elements()) != 2 {
						t.Error("node-local scan shape")
					}
					ack(t, deps.Evidence, 1)
					return nil
				})
				serviceOK(t, err)
				ack(t, deps.Evidence, 1)
				second.stop(t)
				until(t, 6*time.Second, func() bool {
					result, err := owner.Client().Cache().Execute(ctx, command(t, Cache, "SET", prefix+"ring-after-loss", "written"))
					if result.Kind() != NoResult {
						ack(t, deps.Evidence, 1)
					}
					return err == nil
				})
				serviceOK(t, left.Get(ctx, prefix+"ring-after-loss").Err())
			}
		}
	})
	t.Run("cluster-universal-cross-slot-and-authority", func(t *testing.T) {
		var nodes []*testNode
		var peers []*sdk.Client
		var addrs []string
		var buses []int
		for range 3 {
			bus := freePort(t)
			node := startServer(t, fmt.Sprintf("cluster-enabled yes\ncluster-config-file nodes.conf\ncluster-node-timeout 1000\ncluster-port %d\n", bus), false)
			nodes = append(nodes, node)
			buses = append(buses, bus)
			addrs = append(addrs, node.address)
			peers = append(peers, serviceOracle(t, topologySettings(node.address)))
		}
		for index := 1; index < len(nodes); index++ {
			host, port, _ := net.SplitHostPort(nodes[index].address)
			serviceOK(t, peers[0].Do(ctx, "CLUSTER", "MEET", host, port, buses[index]).Err())
		}
		for index, node := range peers {
			serviceOK(t, node.Do(ctx, "CLUSTER", "ADDSLOTSRANGE", index*16384/3, (index+1)*16384/3-1).Err())
		}
		for _, node := range peers {
			until(t, 10*time.Second, func() bool {
				info, err := node.ClusterInfo(ctx).Result()
				return err == nil && strings.Contains(info, "cluster_state:ok")
			})
		}
		for _, mode := range []string{"cluster", "universal"} {
			options := topologySettings(addrs[0])
			options.Mode = mode
			options.AllowedAddrs = addrs
			options.MaxRedirects = 2
			options.ExperimentalAutoPipeline = true
			if mode == "universal" {
				options.UniversalMode = "cluster"
			}
			owner, deps := openTest(t, options)
			first, second, third := "{a}:"+prefix+mode, "{b}:"+prefix+mode, "{c}:"+prefix+mode
			result, err := owner.Client().Cache().Pipeline(ctx, command(t, Cache, "SET", first, "a"), command(t, Cache, "SET", second, "b"), command(t, Cache, "SET", third, "c"))
			serviceOK(t, err)
			if len(result.Replies()) != 3 {
				t.Fatal("cross-slot pipeline incomplete")
			}
			ack(t, deps.Evidence, 1)
			tx, err := owner.Client().Cache().Transaction(ctx, context.Background(), first, command(t, Cache, "SET", first, "updated"), command(t, Cache, "GET", first))
			serviceOK(t, err)
			if len(tx.Execution().Replies()) != 2 {
				t.Fatal("transaction child lost")
			}
			ack(t, deps.Evidence, 2)
			_, err = owner.Client().Cache().Transaction(ctx, context.Background(), first, command(t, Cache, "SET", first, "bad"), command(t, Cache, "SET", second, "bad"))
			if err == nil {
				t.Fatal("cross-slot transaction silently split")
			}
			ack(t, deps.Evidence, 2)
			check, err := owner.Client().Cache().Execute(ctx, command(t, Cache, "GET", first))
			serviceOK(t, err)
			if value, _ := check.Replies()[0].Value().Text(); value != "updated" {
				t.Fatal("refused transaction mutated key")
			}
			ack(t, deps.Evidence, 1)
			if _, err := owner.Client().Cache().Execute(ctx, command(t, Cache, "SCAN", "0")); !errors.Is(err, ErrAuthority) {
				t.Fatal("cluster SCAN overclaimed")
			}
			ack(t, deps.Evidence, 1)
			_, err = owner.Client().Cache().Automatic(ctx, command(t, Cache, "GET", first))
			serviceOK(t, err)
			ack(t, deps.Evidence, 1)
			// Reassign an empty test-owned slot, then witness real MOVED routing.
			moving := "{" + mode + "-moving}:" + prefix
			_, err = owner.Client().Cache().Execute(ctx, command(t, Cache, "GET", moving))
			if !IsNull(err) {
				t.Fatal("warm-up absence")
			}
			ack(t, deps.Evidence, 1)
			slot, err := peers[0].ClusterKeySlot(ctx, moving).Result()
			serviceOK(t, err)
			spans, err := peers[0].ClusterSlots(ctx).Result()
			serviceOK(t, err)
			previous := ""
			for _, span := range spans {
				if int(slot) >= span.Start && int(slot) <= span.End {
					previous = span.Nodes[0].Addr
				}
			}
			destination := 0
			if addrs[destination] == previous {
				destination = 1
			}
			id, err := peers[destination].ClusterMyID(ctx).Result()
			serviceOK(t, err)
			for _, node := range peers {
				serviceOK(t, node.Do(ctx, "CLUSTER", "SETSLOT", slot, "NODE", id).Err())
			}
			_, err = owner.Client().Cache().Execute(ctx, command(t, Cache, "SET", moving, "redirected"))
			serviceOK(t, err)
			ack(t, deps.Evidence, 1)
			actual, err := peers[destination].Get(ctx, moving).Result()
			serviceOK(t, err)
			if actual != "redirected" {
				t.Fatal("redirect did not reach authorized node")
			}
		}
		refused := topologySettings(addrs[0])
		refused.Mode = "cluster"
		refused.AllowedAddrs = []string{addrs[0]}
		refused.MaxRedirects = 2
		owner, deps := openTest(t, refused)
		// {a} hashes outside the seed's first slot range. Discovery must not grow
		// authority, even though native topology metadata contains the other node.
		_, err := owner.Client().Cache().Execute(ctx, command(t, Cache, "SET", "{a}:"+prefix+"forbidden", "never"))
		if !errors.Is(err, ErrAuthority) {
			t.Fatal("discovered endpoint expanded authority", err)
		}
		ack(t, deps.Evidence, 1)
	})
	t.Run("sentinel-universal-replica-staleness-and-failover", func(t *testing.T) {
		primary := startServer(t, "", false)
		host, port, _ := net.SplitHostPort(primary.address)
		replica := startServer(t, fmt.Sprintf("replicaof %s %s\n", host, port), false)
		writer, reader := serviceOracle(t, topologySettings(primary.address)), serviceOracle(t, topologySettings(replica.address))
		until(t, 10*time.Second, func() bool {
			info, err := reader.Info(ctx, "replication").Result()
			return err == nil && strings.Contains(info, "master_link_status:up")
		})
		sentinel := startServer(t, fmt.Sprintf("sentinel monitor gh108 %s %s 1\nsentinel down-after-milliseconds gh108 500\nsentinel failover-timeout gh108 3000\nsentinel parallel-syncs gh108 1\n", host, port), true)
		monitor := sdk.NewSentinelClient(&sdk.Options{Addr: sentinel.address, MaxRetries: -1, DialerRetries: 1, DisableIdentity: true})
		defer monitor.Close()
		until(t, 10*time.Second, func() bool {
			replicas, err := monitor.Replicas(ctx, "gh108").Result()
			return err == nil && len(replicas) == 1
		})
		options := topologySettings(sentinel.address)
		options.Mode = "sentinel"
		options.MasterName = "gh108"
		options.AllowedAddrs = []string{sentinel.address, primary.address, replica.address}
		for _, mode := range []string{"sentinel", "universal"} {
			reading := options
			reading.Mode = mode
			reading.ReadOnly = true
			reading.AdminCommands = []string{"ROLE"}
			if mode == "universal" {
				reading.UniversalMode = "sentinel"
			}
			owner, deps := openTest(t, reading)
			role, err := owner.Client().Cache().Execute(ctx, command(t, Cache, "ROLE"))
			serviceOK(t, err)
			name, _ := role.Replies()[0].Value().Elements()[0].Text()
			if name != "slave" {
				t.Fatal("replica profile used primary")
			}
			ack(t, deps.Evidence, 1)
			target := prefix + mode + "-stale"
			serviceOK(t, writer.Set(ctx, target, "before", 0).Err())
			serviceOK(t, writer.Wait(ctx, 1, time.Second).Err())
			serviceOK(t, reader.Do(ctx, "CLIENT", "PAUSE", "1000", "WRITE").Err())
			serviceOK(t, writer.Set(ctx, target, "after", 0).Err())
			stale, err := owner.Client().Cache().Execute(ctx, command(t, Cache, "GET", target))
			serviceOK(t, err)
			text, _ := stale.Replies()[0].Value().Text()
			if text != "before" {
				t.Fatal("controlled replica lag was not witnessed")
			}
			ack(t, deps.Evidence, 1)
			until(t, 4*time.Second, func() bool { value, err := reader.Get(ctx, target).Result(); return err == nil && value == "after" })
		}
		owner, deps := openTest(t, options)
		_, err := owner.Client().Cache().Execute(ctx, command(t, Cache, "SET", prefix+"promote", "before"))
		serviceOK(t, err)
		ack(t, deps.Evidence, 1)
		serviceOK(t, writer.Wait(ctx, 1, time.Second).Err())
		until(t, 10*time.Second, func() bool {
			info := sdk.NewStringCmd(ctx, "INFO", "sentinel")
			if err := monitor.Process(ctx, info); err != nil || !strings.Contains(info.Val(), "sentinel_tilt:0\r\n") {
				return false
			}
			master, err := monitor.Master(ctx, "gh108").Result()
			if err != nil || master["flags"] != "master" || net.JoinHostPort(master["ip"], master["port"]) != primary.address {
				return false
			}
			replicas, err := monitor.Replicas(ctx, "gh108").Result()
			return err == nil && len(replicas) == 1 && replicas[0]["flags"] == "slave" &&
				replicas[0]["master-link-status"] == "ok" && net.JoinHostPort(replicas[0]["ip"], replicas[0]["port"]) == replica.address
		})
		serviceOK(t, monitor.Failover(ctx, "gh108").Err())
		until(t, 15*time.Second, func() bool {
			info := sdk.NewStringCmd(ctx, "INFO", "sentinel")
			serviceOK(t, monitor.Process(ctx, info))
			if strings.Contains(info.Val(), "sentinel_tilt:1\r\n") {
				t.Fatal("owned Sentinel entered TILT during failover; topology acceptance is unqualified")
			}
			address, err := monitor.GetMasterAddrByName(ctx, "gh108").Result()
			if err == nil && len(address) == 2 && net.JoinHostPort(address[0], address[1]) == replica.address {
				return true
			}
			master, err := monitor.Master(ctx, "gh108").Result()
			if err == nil && net.JoinHostPort(master["ip"], master["port"]) == primary.address && !strings.Contains(master["flags"], "failover_in_progress") {
				t.Fatal("owned Sentinel ended manual failover without the expected primary; topology acceptance is unqualified")
			}
			return false
		})
		until(t, 10*time.Second, func() bool {
			result, err := owner.Client().Cache().Execute(ctx, command(t, Cache, "SET", prefix+"promote", "after"))
			if result.Kind() != NoResult {
				ack(t, deps.Evidence, 1)
			}
			actual, check := reader.Get(ctx, prefix+"promote").Result()
			return err == nil && check == nil && actual == "after"
		})
		options.Mode = "universal"
		options.UniversalMode = "sentinel"
		options.ExperimentalAutoPipeline = true
		universal, evidence := openTest(t, options)
		_, err = universal.Client().Cache().Automatic(ctx, command(t, Cache, "GET", prefix+"promote"))
		serviceOK(t, err)
		ack(t, evidence.Evidence, 1)
	})
	t.Run("composition-password-refresh-and-functions", func(t *testing.T) {
		node := startServer(t, "requirepass gh108-test-only-password\n", false)
		options := topologySettings(node.address)
		options.Username = "default"
		options.AdminCommands = []string{"FUNCTION|LOAD", "FUNCTION|DELETE"}
		options.Commands = append(options.Commands, "FCALL")
		password, err := NewPassword("incorrect-test-password")
		serviceOK(t, err)
		prepared, err := PrepareWithPassword(options, password)
		serviceOK(t, err)
		policy, err := prepared.Policy()
		serviceOK(t, err)
		runtime, err := adapters.New(ctx, policy.Runtime)
		serviceOK(t, err)
		defer runtime.Close(context.Background())
		inbox, err := adapters.NewInbox[Result](policy.Evidence)
		serviceOK(t, err)
		owner, err := prepared.Open(ctx, Dependencies{Runtime: runtime, Evidence: inbox})
		serviceOK(t, err)
		defer owner.Close(context.Background())
		_, err = owner.Client().Cache().Execute(ctx, command(t, Cache, "PING"))
		if err == nil {
			t.Fatal("wrong credentials accepted")
		}
		ack(t, inbox, 1)
		serviceOK(t, password.Replace("gh108-test-only-password"))
		_, err = owner.Client().Cache().Execute(ctx, command(t, Cache, "PING"))
		serviceOK(t, err)
		ack(t, inbox, 1)
		library := "gh108library"
		_, err = owner.Client().Cache().Execute(ctx, command(t, Cache, "FUNCTION", "LOAD", "#!lua name="+library+"\nredis.register_function('gh108value', function(keys,args) return args[1] end)"))
		serviceOK(t, err)
		ack(t, inbox, 1)
		output, err := owner.Client().Cache().Execute(ctx, command(t, Cache, "FCALL", "gh108value", "0", "exact"))
		serviceOK(t, err)
		ack(t, inbox, 1)
		if value, _ := output.Replies()[0].Value().Text(); value != "exact" {
			t.Fatal("function output lost")
		}
		_, err = owner.Client().Cache().Execute(ctx, command(t, Cache, "FUNCTION", "DELETE", library))
		serviceOK(t, err)
		ack(t, inbox, 1)
		serviceOK(t, owner.Close(ctx))
		ack(t, inbox, 1)
	})
}
