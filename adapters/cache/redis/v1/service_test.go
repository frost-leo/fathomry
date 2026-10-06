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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	sdk "github.com/redis/go-redis/v9"
	"go.yaml.in/yaml/v3"
)

func serviceSettings(t *testing.T) Settings {
	t.Helper()
	if os.Getenv("FATHOMRY_REDIS_SERVICE_APPROVED") != "gh-108" {
		t.Fatal("isolated service authorization must be explicit")
	}
	path := os.Getenv("FATHOMRY_REDIS_SERVICE_CONFIG")
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 128<<10 {
		t.Fatal("private service configuration must be a bounded mode-0600 regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("private service configuration unavailable")
	}
	var config struct {
		Redis struct {
			Host, Protocol, Username, Password string
			Port, Database                     int
		} `yaml:"redis"`
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		t.Fatal("invalid private service configuration")
	}
	value := config.Redis
	switch strings.ToLower(value.Protocol) {
	case "redis", "redis://", "tcp", "resp", "resp2", "resp3":
	default:
		t.Fatal("service fixture requires explicit plaintext transport; TLS is separately qualified")
	}
	settings := testSettings(net.JoinHostPort(value.Host, strconv.Itoa(value.Port)))
	settings.Username, settings.Password, settings.DB = value.Username, value.Password, value.Database
	settings.Timeout = 3 * time.Second
	settings.MaxReplyBytes = 1 << 20
	settings.MaxReplyElements = 32768
	settings.MaxCommands = 16
	return settings
}
func serviceOracle(t *testing.T, value Settings) *sdk.Client {
	t.Helper()
	client := sdk.NewClient(&sdk.Options{Addr: value.Addrs[0], Username: value.Username, Password: value.Password, DB: value.DB, Protocol: 3,
		MaxRetries: -1, DialerRetries: 1, DialTimeout: time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, DisableIdentity: true})
	t.Cleanup(func() { _ = client.Close() })
	return client
}
func serviceOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal("service check failed; sensitive native diagnostic suppressed")
	}
}
func namespace(t *testing.T) string {
	t.Helper()
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal("fixture identity unavailable")
	}
	return "fathomry:gh108:" + hex.EncodeToString(random[:]) + ":"
}
func serviceContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func TestPublicRedisService(t *testing.T) {
	settings := serviceSettings(t)
	oracle := serviceOracle(t, settings)
	ctx := serviceContext(t)
	serviceOK(t, oracle.Ping(ctx).Err())
	cluster, err := oracle.Info(ctx, "cluster").Result()
	serviceOK(t, err)
	if !strings.Contains(cluster, "cluster_enabled:0") {
		t.Fatal("standalone service profile not confirmed")
	}
	info, err := oracle.Info(ctx, "server").Result()
	serviceOK(t, err)
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "redis_version:") {
			t.Log(strings.TrimSpace(line))
		}
	}
	prefix := namespace(t)
	t.Log("Owned fixture prefix:", prefix)
	var keys []string
	key := func(suffix string) string { value := prefix + suffix; keys = append(keys, value); return value }
	t.Cleanup(func() {
		clean, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if len(keys) == 0 {
			return
		}
		serviceOK(t, oracle.Del(clean, keys...).Err())
		count, err := oracle.Exists(clean, keys...).Result()
		serviceOK(t, err)
		if count != 0 {
			t.Error("precise fixture cleanup not confirmed")
		} else {
			t.Log("Exact owned-key cleanup independently confirmed")
		}
	})
	settings.Commands = []string{"PING", "SET", "GET", "DEL", "INCR", "TTL", "PTTL", "PEXPIRE", "HSET", "HGETALL", "HSCAN", "LPUSH", "BLPOP", "SADD", "SSCAN", "ZADD", "ZSCAN", "SETBIT", "GETBIT", "GEOADD", "GEOPOS", "PFADD", "PFCOUNT", "SCAN", "EVAL", "EVALSHA", "XADD", "XGROUP", "XREADGROUP", "XPENDING", "XAUTOCLAIM", "XACK", "XNACK", "XCFGSET", "XRANGE", "PUBLISH", "SPUBLISH"}
	owner, deps := openTest(t, settings)
	run := func(t *testing.T, capability Capability, args ...string) Result {
		t.Helper()
		view := owner.Client().Cache()
		if capability == Messaging {
			view = owner.Client().Messaging()
		}
		result, err := view.Execute(ctx, command(t, capability, args...))
		serviceOK(t, err)
		ack(t, deps.Evidence, 1)
		return result
	}
	t.Run("values-units-data-families-and-pages", func(t *testing.T) {
		binary, empty := key("binary"), key("empty")
		run(t, Cache, "SET", binary, "a\x00\xffb", "PX", "30000")
		run(t, Cache, "SET", empty, "")
		if data, _ := run(t, Cache, "GET", binary).Replies()[0].Value().Text(); data != "a\x00\xffb" {
			t.Fatal("binary changed")
		}
		if data, ok := run(t, Cache, "GET", empty).Replies()[0].Value().Text(); !ok || data != "" {
			t.Fatal("empty changed")
		}
		if ttl, _ := run(t, Cache, "PTTL", binary).Replies()[0].Value().Integer(); ttl <= 0 || ttl > 30000 {
			t.Fatal("TTL units changed")
		}
		missing, err := owner.Client().Cache().Execute(ctx, command(t, Cache, "GET", key("missing")))
		if !IsNull(err) || !missing.Replies()[0].HasValue() || missing.Replies()[0].Value().Kind() != Null {
			t.Fatal("decoded null lost")
		}
		ack(t, deps.Evidence, 1)
		for _, test := range []struct {
			suffix      string
			write, read []string
		}{
			{"hash", []string{"HSET", "field", "value"}, []string{"HGETALL"}},
			{"set", []string{"SADD", "member"}, []string{"SSCAN", "0", "COUNT", "5"}},
			{"sorted", []string{"ZADD", "1", "member"}, []string{"ZSCAN", "0", "COUNT", "5"}},
			{"bits", []string{"SETBIT", "3", "1"}, []string{"GETBIT", "3"}},
			{"geo", []string{"GEOADD", "13.361389", "38.115556", "point"}, []string{"GEOPOS", "point"}},
			{"hll", []string{"PFADD", "one", "two"}, []string{"PFCOUNT"}},
		} {
			target := key(test.suffix)
			write := append([]string{test.write[0], target}, test.write[1:]...)
			run(t, Cache, write...)
			read := append([]string{test.read[0], target}, test.read[1:]...)
			if !run(t, Cache, read...).Replies()[0].HasValue() {
				t.Fatal("data family unavailable")
			}
		}
		list := key("list")
		run(t, Cache, "HSCAN", prefix+"hash", "0", "COUNT", "5")
		run(t, Messaging, "LPUSH", list, "job")
		if value := run(t, Messaging, "BLPOP", list, "1").Replies()[0].Value(); value.Kind() != Array {
			t.Fatal("list queue shape changed")
		}
		page := run(t, Cache, "SCAN", "0", "MATCH", prefix+"*", "COUNT", "5")
		if len(page.Replies()[0].Value().Elements()) != 2 {
			t.Fatal("SCAN page shape changed")
		}
	})
	t.Run("resp2-shape", func(t *testing.T) {
		options := settings
		options.Protocol = 2
		second, evidence := openTest(t, options)
		result, err := second.Client().Cache().Execute(ctx, command(t, Cache, "HGETALL", prefix+"hash"))
		serviceOK(t, err)
		if result.Replies()[0].Value().Kind() != Array {
			t.Fatal("RESP2 shape lost")
		}
		ack(t, evidence.Evidence, 1)
	})
	t.Run("partial-batch-exec-script-and-watch", func(t *testing.T) {
		target := key("partial")
		run(t, Cache, "SET", target, "not-integer")
		result, err := owner.Client().Cache().Pipeline(ctx, command(t, Cache, "INCR", target), command(t, Messaging, "XADD", target, "*", "f", "v"), command(t, Cache, "GET", target))
		if !errors.Is(err, ErrCommand) || !errors.Is(err, ErrMessagingCommand) || len(result.Replies()) != 3 || !result.Replies()[2].HasValue() {
			t.Fatal("partial batch owners lost")
		}
		ack(t, deps.Evidence, 1)
		tx, err := owner.Client().Cache().Transaction(ctx, context.Background(), target, command(t, Cache, "SET", target, "kept"), command(t, Messaging, "XADD", target, "*", "f", "v"))
		if !errors.Is(err, ErrMessagingCommand) || tx.Lifecycle().Kind() != Lifecycle || len(tx.Execution().Replies()) != 2 {
			t.Fatal("EXEC error/evidence lost")
		}
		ack(t, deps.Evidence, 2)
		actual, err := oracle.Get(ctx, target).Result()
		serviceOK(t, err)
		if actual != "kept" {
			t.Fatal("EXEC partial effect missing")
		}
		script := key("script")
		_, err = owner.Client().Cache().Execute(ctx, command(t, Cache, "EVAL", "redis.call('SET',KEYS[1],ARGV[1]); return redis.error_reply('fixture failure')", "1", script, "effect"))
		if err == nil {
			t.Fatal("script error missing")
		}
		ack(t, deps.Evidence, 1)
		actual, err = oracle.Get(ctx, script).Result()
		serviceOK(t, err)
		if actual != "effect" {
			t.Fatal("script mutation lost")
		}
		_, err = owner.Client().Cache().Execute(ctx, command(t, Cache, "EVALSHA", strings.Repeat("0", 40), "1", script))
		var server sdk.Error
		if !errors.As(err, &server) || !sdk.HasErrorPrefix(err, "NOSCRIPT") {
			t.Fatal("NOSCRIPT lost")
		}
		ack(t, deps.Evidence, 1)
		watched := key("watch")
		run(t, Cache, "SET", watched, "0")
		callbacks := 0
		_, err = owner.Client().Cache().Watch(ctx, context.Background(), []string{watched}, func(ctx context.Context, session *Session) error {
			callbacks++
			serviceOK(t, oracle.Incr(ctx, watched).Err())
			_, err := session.Transaction(ctx, command(t, Cache, "SET", watched, "never"))
			ack(t, deps.Evidence, 1)
			return err
		})
		if !IsWatchConflict(err) || callbacks != 1 {
			t.Fatal("WATCH conflict replayed or lost")
		}
		ack(t, deps.Evidence, 1)
	})
	t.Run("streams-groups-claim-ack", func(t *testing.T) {
		stream := key("stream")
		entry, _ := run(t, Messaging, "XADD", stream, "*", "field", "value").Replies()[0].Value().Text()
		run(t, Messaging, "XGROUP", "CREATE", stream, "owned-group", "0")
		read := run(t, Messaging, "XREADGROUP", "GROUP", "owned-group", "first", "COUNT", "1", "STREAMS", stream, ">")
		if !read.Replies()[0].HasValue() {
			t.Fatal("stream data absent")
		}
		pending := run(t, Messaging, "XPENDING", stream, "owned-group").Replies()[0].Value().Elements()
		if count, _ := pending[0].Integer(); count != 1 {
			t.Fatal("pending entry absent")
		}
		run(t, Messaging, "XAUTOCLAIM", stream, "owned-group", "second", "0", "0-0", "COUNT", "1")
		if count, _ := run(t, Messaging, "XACK", stream, "owned-group", entry).Replies()[0].Value().Integer(); count != 1 {
			t.Fatal("native ack absent")
		}
		run(t, Messaging, "XCFGSET", stream, "IDMP-DURATION", "60", "IDMP-MAXSIZE", "100")
		entry, _ = run(t, Messaging, "XADD", stream, "*", "field", "redelivery").Replies()[0].Value().Text()
		run(t, Messaging, "XREADGROUP", "GROUP", "owned-group", "first", "COUNT", "1", "STREAMS", stream, ">")
		if count, _ := run(t, Messaging, "XNACK", stream, "owned-group", "SILENT", "IDS", "1", entry).Replies()[0].Value().Integer(); count != 1 {
			t.Fatal("native negative acknowledgement absent")
		}
		run(t, Messaging, "XREADGROUP", "GROUP", "owned-group", "replacement", "COUNT", "1", "CLAIM", "1", "STREAMS", stream, ">")
		run(t, Messaging, "XACK", stream, "owned-group", entry)
	})
	for _, mode := range []string{"channel", "pattern", "shard"} {
		t.Run("pubsub-"+mode, func(t *testing.T) {
			channel := prefix + "channel-" + mode
			target := channel
			if mode == "pattern" {
				target = channel + "*"
			}
			publish := func(payload string) (int64, error) {
				if mode == "shard" {
					return oracle.SPublish(ctx, channel, payload).Result()
				}
				return oracle.Publish(ctx, channel, payload).Result()
			}
			_, err := owner.Client().Messaging().Subscribe(ctx, context.Background(), SubscriptionOptions{Mode: mode, Channels: []string{target}}, func(ctx context.Context, subscription *Subscription) error {
				confirmation, err := subscription.Receive(ctx)
				if err != nil {
					return err
				}
				kind, _ := confirmation.Replies()[0].Value().Elements()[0].Text()
				if !strings.HasSuffix(kind, "subscribe") {
					t.Error("confirmation mistaken for payload")
				}
				ack(t, deps.Evidence, 1)
				count, err := publish("\x00payload")
				serviceOK(t, err)
				if count != 1 {
					t.Error("independent subscriber count mismatch")
				}
				message, err := subscription.Receive(ctx)
				if err != nil {
					return err
				}
				data, _ := message.Replies()[0].Value().Elements()[3].Text()
				if data != "\x00payload" {
					t.Error("message data changed")
				}
				ack(t, deps.Evidence, 1)
				return nil
			})
			serviceOK(t, err)
			ack(t, deps.Evidence, 1)
			until(t, 2*time.Second, func() bool { count, err := publish("after-close"); return err == nil && count == 0 })
		})
	}
	for _, mode := range []string{"watch", "dedicated", "subscription"} {
		t.Run("callback-goexit-"+mode, func(t *testing.T) {
			current, evidence := openTest(t, settings)
			cleanup, stop := context.WithCancel(context.Background())
			stop()
			target := prefix + "goexit-" + mode
			finished := make(chan struct{})
			var captured *operation
			returned := false
			go func() {
				defer close(finished)
				if mode == "subscription" {
					_, _ = current.Client().Messaging().Subscribe(ctx, cleanup, SubscriptionOptions{Mode: "channel", Channels: []string{target}}, func(work context.Context, subscription *Subscription) error {
						captured = subscription.state.op
						_, err := subscription.Receive(work)
						ack(t, evidence.Evidence, 1)
						if err != nil {
							t.Error("real subscription confirmation failed")
							return err
						}
						runtime.Goexit()
						return nil
					})
				} else {
					capture := func(_ context.Context, session *Session) error {
						captured = session.state.op
						runtime.Goexit()
						return nil
					}
					if mode == "watch" {
						_, _ = current.Client().Cache().Watch(ctx, cleanup, []string{target}, capture)
					} else {
						_, _ = current.Client().Cache().Dedicated(ctx, cleanup, target, capture)
					}
				}
				returned = true
			}()
			select {
			case <-finished:
			case <-ctx.Done():
				t.Fatal("real callback termination did not complete")
			}
			if returned || captured == nil {
				t.Fatal("real callback termination path was not exercised")
			}
			delivery, err := evidence.Evidence.NextReleased(ctx)
			serviceOK(t, err)
			receipt, err := delivery.Receipt()
			serviceOK(t, err)
			snapshot, _ := receipt.Snapshot()
			value, present := snapshot.ValueCopy()
			if !present || value.Kind() != Lifecycle || snapshot.Primary() == nil || captured.family.inbox.Usage().Outstanding != 0 || current.PendingTransfers() != 0 {
				t.Fatal("real callback lifecycle evidence was not transferred")
			}
			if mode != "subscription" && !errors.Is(snapshot.Cleanup(), context.Canceled) {
				t.Fatal("real callback cleanup evidence was lost")
			}
			serviceOK(t, delivery.Ack())
			serviceOK(t, current.Close(ctx))
			if !current.ShutdownComplete() {
				t.Fatal("real callback source shutdown unconfirmed")
			}
			if mode == "subscription" {
				until(t, 2*time.Second, func() bool {
					count, err := oracle.Publish(ctx, target, "after-close").Result()
					return err == nil && count == 0
				})
			}
		})
	}
	t.Run("blocking-unknown-after-timeout", func(t *testing.T) {
		wait, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
		defer cancel()
		result, err := owner.Client().Messaging().Execute(wait, command(t, Messaging, "BLPOP", key("blocking"), "0"))
		if err == nil || result.Replies()[0].State() != Unknown || result.Replies()[0].HasValue() {
			t.Fatal("timeout invented absence")
		}
		ack(t, deps.Evidence, 1)
	})
	t.Run("real-cache-hit-invalidation-and-auto-width", func(t *testing.T) {
		if settings.DB != 0 {
			t.Fatal("CSC service profile requires explicitly configured DB0")
		}
		options := settings
		options.ExperimentalCache = true
		options.ExperimentalAutoPipeline = true
		options.CacheMaxStaleness = time.Minute
		options.MaxActive = 8
		cached, evidence := openTest(t, options)
		target := key("cached")
		serviceOK(t, oracle.Set(ctx, target, "before", time.Minute).Err())
		get := command(t, Cache, "GET", target)
		for range 2 {
			result, err := cached.Client().Cache().Execute(ctx, get)
			serviceOK(t, err)
			if result.Replies()[0].State() != CacheOrReply {
				t.Fatal("freshness overstated")
			}
			ack(t, evidence.Evidence, 1)
		}
		stats, err := cached.Client().Stats(ctx)
		serviceOK(t, err)
		if stats.CacheHits == 0 {
			t.Fatal("no genuine CSC hit")
		}
		serviceOK(t, oracle.Set(ctx, target, "after", time.Minute).Err())
		until(t, 2*time.Second, func() bool {
			result, err := cached.Client().Cache().Execute(ctx, get)
			if err != nil {
				return false
			}
			text, _ := result.Replies()[0].Value().Text()
			ack(t, evidence.Evidence, 1)
			return text == "after"
		})
		var receipts []*adapters.Receipt[Result]
		var mutex sync.Mutex
		var workers sync.WaitGroup
		barrier := make(chan struct{})
		targets := make([]string, 8)
		for index := range targets {
			targets[index] = key(fmt.Sprintf("auto-%d", index))
		}
		for _, target := range targets {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-barrier
				receipt, err := cached.Client().Cache().Submit(ctx, command(t, Cache, "SET", target, "written"))
				if err != nil {
					t.Error("deferred admission failed")
					return
				}
				mutex.Lock()
				receipts = append(receipts, receipt)
				mutex.Unlock()
			}()
		}
		close(barrier)
		workers.Wait()
		for _, receipt := range receipts {
			result, err := receipt.WaitReleased(ctx)
			serviceOK(t, err)
			serviceOK(t, result.Err())
			ack(t, evidence.Evidence, 1)
		}
		if len(receipts) != 8 {
			t.Fatal("not all automatic calls accepted")
		}
		for _, target := range targets {
			actual, err := oracle.Get(ctx, target).Result()
			serviceOK(t, err)
			if actual != "written" {
				t.Fatal("automatic write missing")
			}
		}
		stats, err = cached.Client().Stats(ctx)
		serviceOK(t, err)
		if stats.AutomaticMaxWidth < 2 {
			t.Fatal("automatic pipeline did not demonstrate a multi-command batch")
		}
		t.Logf("Observed automatic pipeline maximum width: %d", stats.AutomaticMaxWidth)
	})
}
func until(t *testing.T, duration time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("bounded independent observation did not converge")
}
