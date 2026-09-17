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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
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

type localNode struct {
	address  string
	command  *exec.Cmd
	done     chan error
	stopOnce sync.Once
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
func startNode(t *testing.T, extra string, sentinel bool) *localNode {
	t.Helper()
	binary := os.Getenv("FATHOMRY_REDIS_TEST_SERVER")
	if binary == "" || !filepath.IsAbs(binary) {
		t.Fatal("FATHOMRY_REDIS_TEST_SERVER must name an explicitly authorized absolute redis-server executable")
	}
	directory := t.TempDir()
	port := freePort(t)
	config := fmt.Sprintf("bind 127.0.0.1\nport %d\nprotected-mode yes\nsave \"\"\nappendonly no\ndir %s\nlogfile %s\n", port, directory, filepath.Join(directory, "redis.log")) + extra
	path := filepath.Join(directory, "redis.conf")
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{path}
	if sentinel {
		args = append(args, "--sentinel")
	}
	command := exec.Command(binary, args...)
	node := &localNode{address: net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), command: command, done: make(chan error, 1)}
	if err := command.Start(); err != nil {
		t.Fatal("test-owned Redis process did not start")
	}
	go func() { node.done <- command.Wait() }()
	t.Cleanup(func() { node.stop(t) })
	native := sdk.NewClient(&sdk.Options{Addr: node.address, MaxRetries: -1, DialerRetries: 1, DialTimeout: 100 * time.Millisecond, ReadTimeout: 100 * time.Millisecond, DisableIdentity: true})
	defer native.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := native.Ping(context.Background()).Err(); err == nil || sdk.HasErrorPrefix(err, "NOAUTH") {
			return node
		}
		select {
		case err := <-node.done:
			node.done <- err
			t.Fatal("test-owned Redis process exited during startup")
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("test-owned Redis startup deadline exceeded")
	return nil
}
func (node *localNode) stop(t *testing.T) {
	t.Helper()
	node.stopOnce.Do(func() {
		_ = node.command.Process.Signal(os.Interrupt)
		select {
		case <-node.done:
		case <-time.After(5 * time.Second):
			_ = node.command.Process.Kill()
			<-node.done
			t.Error("test-owned Redis required forced termination")
		}
	})
}
func topologyOptions(address string) OptionsV1 {
	value := testOptions(address)
	value.MaxReplyBytes = 1 << 20
	value.MaxReplyElements = 32768
	value.Timeout = 2 * time.Second
	return value
}
func eventually(t *testing.T, limit time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("independent topology oracle did not converge within its test budget")
}
func TestRedisTopologies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	t.Run("universal-and-ring", func(t *testing.T) {
		first, second := startNode(t, "", false), startNode(t, "", false)
		options := topologyOptions(first.address)
		options.Mode = "universal"
		options.UniversalMode = "standalone"
		client, _, inbox, _ := bindTest(t, options, 16)
		got := executeTest(t, client, "universal", "SET", "gh62:universal", "value")
		if got.Err() != nil {
			t.Fatal(got.Err())
		}
		drain(t, inbox)
		options.Mode = "ring"
		options.UniversalMode = ""
		options.Addrs = nil
		options.Shards = map[string]string{"first": first.address, "second": second.address}
		ring, _, evidence, _ := bindTest(t, options, 16)
		for index := 0; index < 20; index++ {
			got := executeTest(t, ring, fmt.Sprintf("ring-%d", index), "SET", fmt.Sprintf("gh62:ring:%d", index), "value")
			if got.Err() != nil {
				t.Fatal(got.Err())
			}
			drain(t, evidence)
		}
		left, right := oracle(t, topologyOptions(first.address)), oracle(t, topologyOptions(second.address))
		leftCount, lerr := left.DBSize(ctx).Result()
		rightCount, rerr := right.DBSize(ctx).Result()
		mustNative(t, lerr)
		mustNative(t, rerr)
		if leftCount < 2 || rightCount < 1 || leftCount+rightCount != 21 {
			t.Fatal("Ring did not distribute native writes")
		}
		second.stop(t)
		eventually(t, 5*time.Second, func() bool { return ring.owner.native.(*sdk.Ring).Len() == 1 })
		got = executeTest(t, ring, "ring-after-loss", "SET", "gh62:after-loss", "value")
		if got.Err() != nil {
			t.Fatal(got.Err())
		}
		drain(t, evidence)
	})
	t.Run("cluster-and-universal-selection", func(t *testing.T) {
		var nodes []*localNode
		var buses []int
		var natives []*sdk.Client
		var addresses []string
		for index := 0; index < 3; index++ {
			bus := freePort(t)
			node := startNode(t, fmt.Sprintf("cluster-enabled yes\ncluster-config-file nodes.conf\ncluster-node-timeout 1000\ncluster-port %d\n", bus), false)
			nodes = append(nodes, node)
			buses = append(buses, bus)
			addresses = append(addresses, node.address)
			natives = append(natives, oracle(t, topologyOptions(node.address)))
		}
		for index := 1; index < len(nodes); index++ {
			host, port, _ := net.SplitHostPort(nodes[index].address)
			mustNative(t, natives[0].Do(ctx, "CLUSTER", "MEET", host, port, buses[index]).Err())
		}
		for index, native := range natives {
			start, end := index*16384/3, (index+1)*16384/3-1
			mustNative(t, native.Do(ctx, "CLUSTER", "ADDSLOTSRANGE", start, end).Err())
		}
		for _, native := range natives {
			eventually(t, 10*time.Second, func() bool {
				info, err := native.ClusterInfo(ctx).Result()
				return err == nil && strings.Contains(info, "cluster_state:ok")
			})
		}
		for _, mode := range []string{"cluster", "universal"} {
			options := topologyOptions(addresses[0])
			options.Mode = mode
			if mode == "universal" {
				options.UniversalMode = "cluster"
			}
			options.AllowedAddrs = addresses
			options.MaxRedirects = 2
			options.ExperimentalAutoPipeline = true
			options.AdminCommands = []string{"DBSIZE"}
			client, _, inbox, _ := bindTest(t, options, 32)
			receipt, err := client.Pipeline(ctx, fault.Correlation{Call: mode + "-multi-slot"},
				NewCommand("SET", "{a}:gh62", "a"), NewCommand("SET", "{b}:gh62", "b"), NewCommand("SET", "{c}:gh62", "c"))
			got := resolved(t, receipt, err)
			if got.Err() != nil || len(got.Outcome.Value.Commands()) != 3 {
				t.Fatal("independent multi-slot pipeline failed")
			}
			drain(t, inbox)
			moving := "{" + mode + "-moving}:gh62"
			before := executeTest(t, client, mode+"-route-before", "GET", moving)
			if !errors.Is(before.Err(), sdk.Nil) {
				t.Fatal("route warmup did not observe absence")
			}
			drain(t, inbox)
			slot, slotErr := natives[0].ClusterKeySlot(ctx, moving).Result()
			mustNative(t, slotErr)
			slots, slotsErr := natives[0].ClusterSlots(ctx).Result()
			mustNative(t, slotsErr)
			var previous string
			for _, span := range slots {
				if int(slot) >= span.Start && int(slot) <= span.End {
					previous = span.Nodes[0].Addr
				}
			}
			destination := 0
			if addresses[destination] == previous {
				destination = 1
			}
			identifier, idErr := natives[destination].ClusterMyID(ctx).Result()
			mustNative(t, idErr)
			for _, node := range natives {
				mustNative(t, node.Do(ctx, "CLUSTER", "SETSLOT", slot, "NODE", identifier).Err())
			}
			moved := executeTest(t, client, mode+"-moved", "SET", moving, "redirected")
			if moved.Err() != nil {
				t.Fatal("bounded MOVED redirect failed")
			}
			drain(t, inbox)
			readback, readErr := natives[destination].Get(ctx, moving).Result()
			mustNative(t, readErr)
			if readback != "redirected" {
				t.Fatal("redirect effect did not reach the selected node")
			}
			receipt, err = client.Dedicated(ctx, context.Background(), fault.Correlation{Call: mode + "-tx"}, "{a}:gh62", func(ctx context.Context, session *Session) error {
				child, err := session.Transaction(ctx, fault.Correlation{Call: mode + "-exec", Parent: mode + "-tx"}, NewCommand("SET", "{a}:gh62", "updated"), NewCommand("GET", "{a}:gh62"))
				return resultError(child, err)
			})
			got = resolved(t, receipt, err)
			if got.Err() != nil {
				t.Fatal("same-slot transaction failed")
			}
			drain(t, inbox)
			receipt, err = client.Dedicated(ctx, context.Background(), fault.Correlation{Call: mode + "-cross"}, "{a}:gh62", func(ctx context.Context, session *Session) error {
				child, err := session.Transaction(ctx, fault.Correlation{Call: mode + "-cross-exec", Parent: mode + "-cross"}, NewCommand("SET", "{a}:gh62", "bad"), NewCommand("SET", "{b}:gh62", "bad"))
				return resultError(child, err)
			})
			got = resolved(t, receipt, err)
			if got.Err() == nil {
				t.Fatal("cross-slot transaction silently split")
			}
			drain(t, inbox)
			check := executeTest(t, client, mode+"-check", "GET", "{a}:gh62")
			value, _ := check.Outcome.Value.Commands()[0].Value().Text()
			if check.Err() != nil || value != "updated" {
				t.Fatal("refused transaction changed primary key")
			}
			drain(t, inbox)
			automatic, err := client.Submit(ctx, fault.Correlation{Call: mode + "-automatic"}, NewCommand("GET", "{a}:gh62"))
			got = resolved(t, automatic, err)
			if got.Err() != nil {
				t.Fatal("cluster automatic failed")
			}
			drain(t, inbox)
			cluster := client.owner.native.(*sdk.ClusterClient)
			var calls atomic.Int32
			var reject atomic.Bool
			var choose sync.Once
			mustNative(t, cluster.ForEachMaster(ctx, func(_ context.Context, node *sdk.Client) error {
				hook := fanoutFailureHook{calls: &calls}
				choose.Do(func() { hook.reject = &reject })
				node.AddHook(hook)
				return nil
			}))
			nodeLocal := executeTest(t, client, mode+"-node-local", "DBSIZE")
			if nodeLocal.Err() != nil || calls.Load() != 1 {
				t.Fatal("default native DBSIZE policy is not node-local")
			}
			drain(t, inbox)
			// Dynamic core policies are a test-only fan-out oracle; the Provider
			// retains the SDK default, which routes DBSIZE to one node.
			resolver := sdk.NewDefaultCommandPolicyResolver()
			resolver.SetFallbackResolver(cluster.NewDynamicResolver())
			cluster.SetCommandInfoResolver(resolver)
			calls.Store(0)
			aggregate := executeTest(t, client, mode+"-aggregate-success", "DBSIZE")
			var expected int64
			for _, node := range natives {
				count, err := node.DBSize(ctx).Result()
				mustNative(t, err)
				expected += count
			}
			replies := aggregate.Outcome.Value.Commands()
			if aggregate.Err() != nil || len(replies) != 1 || calls.Load() != int32(len(nodes)) {
				t.Fatal("native aggregate did not query all nodes successfully")
			}
			total, ok := replies[0].Value().Double()
			if !ok || total != float64(expected) {
				t.Fatal("native aggregate did not combine all independently observed node counts")
			}
			drain(t, inbox)
			reject.Store(true)
			calls.Store(0)
			aggregate = executeTest(t, client, mode+"-aggregate-cause", "DBSIZE")
			replies = aggregate.Outcome.Value.Commands()
			if !errors.Is(aggregate.Err(), sdk.ErrClosed) || calls.Load() < int32(len(nodes)) ||
				len(replies) != 1 || replies[0].State() != Unknown || !errors.Is(replies[0].Err(), sdk.ErrClosed) {
				t.Fatal("native aggregate replaced the original node cause")
			}
			drain(t, inbox)
		}
	})
	t.Run("sentinel-failover", func(t *testing.T) {
		master := startNode(t, "", false)
		host, port, _ := net.SplitHostPort(master.address)
		replica := startNode(t, fmt.Sprintf("replicaof %s %s\n", host, port), false)
		primary, secondary := oracle(t, topologyOptions(master.address)), oracle(t, topologyOptions(replica.address))
		eventually(t, 10*time.Second, func() bool {
			info, err := secondary.Info(ctx, "replication").Result()
			return err == nil && strings.Contains(info, "master_link_status:up")
		})
		sentinel := startNode(t, fmt.Sprintf("sentinel monitor gh62 %s %s 1\nsentinel down-after-milliseconds gh62 500\nsentinel failover-timeout gh62 3000\nsentinel parallel-syncs gh62 1\n", host, port), true)
		monitor := sdk.NewSentinelClient(&sdk.Options{Addr: sentinel.address, MaxRetries: -1, DialerRetries: 1, DisableIdentity: true})
		defer monitor.Close()
		eventually(t, 10*time.Second, func() bool {
			replicas, err := monitor.Replicas(ctx, "gh62").Result()
			return err == nil && len(replicas) == 1
		})
		options := topologyOptions(sentinel.address)
		options.Mode = "sentinel"
		options.MasterName = "gh62"
		options.AllowedAddrs = []string{sentinel.address, master.address, replica.address}
		readOptions := options
		readOptions.ReadOnly = true
		readOptions.AdminCommands = []string{"ROLE"}
		reader, _, readEvidence, _ := bindTest(t, readOptions, 16)
		role := executeTest(t, reader, "replica-role", "ROLE")
		if role.Err() != nil {
			t.Fatal(role.Err())
		}
		roleName, _ := role.Outcome.Value.Commands()[0].Value().Elements()[0].Text()
		if roleName != "slave" {
			t.Fatal("replica profile did not select a replica")
		}
		drain(t, readEvidence)
		mustNative(t, primary.Set(ctx, "gh62:staleness", "before", 0).Err())
		mustNative(t, primary.Wait(ctx, 1, time.Second).Err())
		mustNative(t, secondary.Do(ctx, "CLIENT", "PAUSE", "2000", "WRITE").Err())
		mustNative(t, primary.Set(ctx, "gh62:staleness", "after", 0).Err())
		stale := executeTest(t, reader, "replica-stale", "GET", "gh62:staleness")
		staleValue, _ := stale.Outcome.Value.Commands()[0].Value().Text()
		if stale.Err() != nil || staleValue != "before" {
			t.Fatal("controlled replica lag was not observed")
		}
		drain(t, readEvidence)
		eventually(t, 5*time.Second, func() bool {
			result, err := secondary.Get(ctx, "gh62:staleness").Result()
			return err == nil && result == "after"
		})
		client, _, inbox, _ := bindTest(t, options, 16)
		got := executeTest(t, client, "before-failover", "SET", "gh62:failover", "before")
		if got.Err() != nil {
			t.Fatal(got.Err())
		}
		drain(t, inbox)
		mustNative(t, primary.Wait(ctx, 1, time.Second).Err())
		mustNative(t, monitor.Failover(ctx, "gh62").Err())
		eventually(t, 15*time.Second, func() bool {
			address, err := monitor.GetMasterAddrByName(ctx, "gh62").Result()
			return err == nil && len(address) == 2 && net.JoinHostPort(address[0], address[1]) == replica.address
		})
		eventually(t, 10*time.Second, func() bool {
			receipt, err := client.Execute(ctx, fault.Correlation{Call: "after-failover"}, NewCommand("SET", "gh62:failover", "after"))
			if err != nil {
				return false
			}
			got := resolved(t, receipt, nil)
			drain(t, inbox)
			actual, readErr := secondary.Get(ctx, "gh62:failover").Result()
			return got.Err() == nil && readErr == nil && actual == "after"
		})
		actual, err := secondary.Get(ctx, "gh62:failover").Result()
		mustNative(t, err)
		if actual != "after" {
			t.Fatal("write did not reach promoted primary")
		}
		options.Mode = "universal"
		options.UniversalMode = "sentinel"
		universal, _, evidence, _ := bindTest(t, options, 8)
		got = executeTest(t, universal, "universal-sentinel", "GET", "gh62:failover")
		actual, _ = got.Outcome.Value.Commands()[0].Value().Text()
		if got.Err() != nil || actual != "after" {
			t.Fatal("universal sentinel selection failed")
		}
		drain(t, evidence)
	})
	t.Run("functions-auth-refresh", func(t *testing.T) {
		node := startNode(t, "requirepass gh62-test-only-password\n", false)
		options := topologyOptions(node.address)
		options.Username = "default"
		options.Password = "gh62-test-only-password"
		options.Commands = append(options.Commands, "FCALL")
		options.AdminCommands = []string{"FUNCTION|LOAD", "FUNCTION|DELETE"}
		client, _, inbox, _ := bindTest(t, options, 16)
		got := executeTest(t, client, "function-load", "FUNCTION", "LOAD", "#!lua name=gh62\nredis.register_function('gh62_echo', function(keys, args) return args[1] end)")
		if got.Err() != nil {
			t.Fatal(got.Err())
		}
		drain(t, inbox)
		got = executeTest(t, client, "function-call", "FCALL", "gh62_echo", "0", "binary\x00")
		text, _ := got.Outcome.Value.Commands()[0].Value().Text()
		if got.Err() != nil || text != "binary\x00" {
			t.Fatal("native function failed")
		}
		drain(t, inbox)
		got = executeTest(t, client, "function-delete", "FUNCTION", "DELETE", "gh62")
		if got.Err() != nil {
			t.Fatal(got.Err())
		}
		drain(t, inbox)
		credentials, err := NewPassword(options.Password)
		if err != nil {
			t.Fatal(err)
		}
		options.Password = ""
		selected, err := SelectWithPassword(options, credentials)
		if err != nil {
			t.Fatal(err)
		}
		selected = resource.WithLimits(selected, LimitsV1(options))
		assembly, err := resource.Assemble(ctx, context.Background(), "rotation", selected)
		if err != nil {
			t.Fatal(err)
		}
		defer assembly.Close(context.Background())
		evidence, _ := invocation.NewInbox[Result](16, 16*defaults(options).evidenceReservation())
		rotating, err := Bind(assembly, selected, evidence, nil)
		if err != nil {
			t.Fatal(err)
		}
		call := func(id string) invocation.Result[Result] {
			receipt, err := rotating.Dedicated(ctx, context.Background(), fault.Correlation{Call: id}, "key", func(ctx context.Context, session *Session) error {
				child, err := session.Execute(ctx, fault.Correlation{Call: id + "-ping", Parent: id}, NewCommand("PING"))
				return resultError(child, err)
			})
			got := resolved(t, receipt, err)
			drain(t, evidence)
			return got
		}
		if got := call("initial-password"); got.Err() != nil {
			t.Fatal(got.Err())
		}
		mustNative(t, credentials.Replace("wrong-test-only-password"))
		if got := call("rejected-password"); got.Err() == nil {
			t.Fatal("credential replacement was not consulted on new connection")
		}
		mustNative(t, credentials.Replace("gh62-test-only-password"))
		if got := call("refreshed-password"); got.Err() != nil {
			t.Fatal(got.Err())
		}
	})
}

func TestRedisLostAcknowledgement(t *testing.T) {
	node := startNode(t, "", false)
	native := oracle(t, topologyOptions(node.address))
	address := peer(t, func(args []string) string {
		backend, err := net.DialTimeout("tcp", node.address, time.Second)
		if err != nil {
			return ""
		}
		defer backend.Close()
		_ = backend.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err = fmt.Fprintf(backend, "*%d\r\n", len(args)); err != nil {
			return ""
		}
		for _, arg := range args {
			if _, err = fmt.Fprintf(backend, "$%d\r\n%s\r\n", len(arg), arg); err != nil {
				return ""
			}
		}
		reply, err := readFrame(bufio.NewReader(backend), 1<<20, 32768)
		if err != nil {
			return ""
		}
		if strings.EqualFold(args[0], "INCR") {
			return ""
		}
		return string(reply)
	})
	options := topologyOptions(address)
	client, _, inbox, _ := bindTest(t, options, 4)
	got := executeTest(t, client, "lost-real-ack", "INCR", "gh62:lost")
	if !errors.Is(got.Err(), io.EOF) || got.Outcome.Value.Commands()[0].State() != Unknown {
		t.Fatal("real response loss was not preserved as unknown")
	}
	effect, err := native.Get(context.Background(), "gh62:lost").Int()
	mustNative(t, err)
	if effect != 1 {
		t.Fatal("lost acknowledgement retried or lost the actual server effect")
	}
	drain(t, inbox)
}

type fanoutFailureHook struct {
	batchEvidenceHook
	calls  *atomic.Int32
	reject *atomic.Bool
}

func (hook fanoutFailureHook) ProcessHook(next sdk.ProcessHook) sdk.ProcessHook {
	return func(ctx context.Context, cmd sdk.Cmder) error {
		if cmd.Name() == "dbsize" {
			hook.calls.Add(1)
			if hook.reject != nil && hook.reject.Load() {
				return sdk.ErrClosed
			}
		}
		return next(ctx, cmd)
	}
}
func TestRedisRingCompoundCommands(t *testing.T) {
	first, second := startNode(t, "", false), startNode(t, "", false)
	options := topologyOptions(first.address)
	options.Mode = "ring"
	options.Addrs = nil
	options.Shards = map[string]string{"first": first.address, "second": second.address}
	options.Commands = append(options.Commands, "ZADD", "ZUNION", "XGROUP", "XREAD", "XREADGROUP")
	client, _, inbox, _ := bindTest(t, options, 16)
	ring := client.owner.native.(*sdk.Ring)
	countNode, err := ring.GetShardClientForKey("1")
	mustNative(t, err)
	key := ""
	for index := 0; index < 100; index++ {
		candidate := fmt.Sprintf("{owned-%d}:sorted", index)
		node, err := ring.GetShardClientForKey(candidate)
		mustNative(t, err)
		if node != countNode {
			key = candidate
			break
		}
	}
	if key == "" {
		t.Fatal("fixture did not distinguish routing keys")
	}
	result := executeTest(t, client, "seed", "ZADD", key, "1", "member")
	if result.Err() != nil {
		t.Fatal(result.Err())
	}
	drain(t, inbox)
	result = executeTest(t, client, "union", "ZUNION", "1", key)
	if result.Err() != nil || len(result.Outcome.Value.Commands()[0].Value().Elements()) != 1 {
		t.Fatal("non-leading key routed to wrong shard")
	}
	drain(t, inbox)
	stream := strings.Replace(key, ":sorted", ":stream", 1)
	for index, args := range [][]string{
		{"XADD", stream, "*", "field", "value"},
		{"XGROUP", "CREATE", stream, "STREAMS", "0"},
		{"XREAD", "COUNT", "1", "STREAMS", stream, "0"},
		{"XREADGROUP", "COUNT", "1", "GROUP", "STREAMS", "CLAIM", "STREAMS", stream, ">"},
	} {
		result = executeTest(t, client, fmt.Sprintf("stream-%d", index), args...)
		if result.Err() != nil {
			t.Fatal(result.Err())
		}
		drain(t, inbox)
	}
}
