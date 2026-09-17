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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	sdk "github.com/redis/go-redis/v9"
	"go.yaml.in/yaml/v3"
)

func serviceOptions(t *testing.T) OptionsV1 {
	t.Helper()
	path := os.Getenv("FATHOMRY_REDIS_SERVICE_CONFIG")
	if path == "" {
		t.Fatal("FATHOMRY_REDIS_SERVICE_CONFIG is required for this explicitly selected service test")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 128<<10 {
		t.Fatal("private service configuration must be a regular, bounded mode-0600 file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("cannot read private service configuration")
	}
	var config struct {
		Redis struct {
			Host     string `yaml:"host"`
			Port     int    `yaml:"port"`
			Protocol string `yaml:"protocol"`
			Username string `yaml:"username"`
			Password string `yaml:"password"`
			Database int    `yaml:"database"`
		} `yaml:"redis"`
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		t.Fatal("invalid private service configuration")
	}
	value := config.Redis
	options := testOptions(net.JoinHostPort(value.Host, strconv.Itoa(value.Port)))
	options.Username, options.Password, options.DB = value.Username, value.Password, value.Database
	switch strings.ToLower(value.Protocol) {
	case "redis", "redis://", "tcp", "resp", "resp2", "resp3":
	default:
		t.Fatal("this fixture requires an explicitly plaintext Redis transport; TLS uses separate local transport tests")
	}
	options.Timeout = 3 * time.Second
	options.MaxReplyBytes = 1 << 20
	options.MaxReplyElements = 32768
	return options
}
func oracle(t *testing.T, options OptionsV1) *sdk.Client {
	t.Helper()
	native := sdk.NewClient(&sdk.Options{Addr: options.Addrs[0], Username: options.Username, Password: options.Password, DB: options.DB,
		Protocol: 3, MaxRetries: -1, DialerRetries: 1, DialTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second,
		DisableIdentity: true})
	t.Cleanup(func() { _ = native.Close() })
	return native
}
func fixtureNamespace(t *testing.T) string {
	t.Helper()
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal("random fixture identity unavailable")
	}
	return "fathomry:gh62:" + hex.EncodeToString(random[:]) + ":"
}
func mustNative(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal("independent Redis oracle failed; private native diagnostic suppressed")
	}
}
func TestRedisService(t *testing.T) {
	options := serviceOptions(t)
	native := oracle(t, options)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	mustNative(t, native.Ping(ctx).Err())
	info, err := native.Info(ctx, "server").Result()
	mustNative(t, err)
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "redis_version:") {
			t.Log(strings.TrimSpace(line))
		}
	}
	prefix := fixtureNamespace(t)
	var keys []string
	key := func(suffix string) string { value := prefix + suffix; keys = append(keys, value); return value }
	t.Cleanup(func() {
		clean, end := context.WithTimeout(context.Background(), 10*time.Second)
		defer end()
		if len(keys) > 0 {
			mustNative(t, native.Del(clean, keys...).Err())
			exists, err := native.Exists(clean, keys...).Result()
			mustNative(t, err)
			if exists != 0 {
				t.Error("test-owned key cleanup not confirmed")
			}
		}
	})
	options.Commands = []string{"PING", "SET", "GET", "INCR", "DEL", "TTL", "PTTL", "EXPIRE", "PEXPIRE", "HSET", "HGETALL",
		"LPUSH", "BLPOP", "SADD", "SMEMBERS", "ZADD", "ZRANGE", "SETBIT", "GETBIT", "GEOADD", "GEOPOS", "PFADD", "PFCOUNT",
		"SCAN", "XADD", "XGROUP", "XREADGROUP", "XACK", "XPENDING", "XAUTOCLAIM", "XRANGE", "PUBLISH", "EVAL", "EVALSHA"}
	client, _, inbox, _ := bindTest(t, options, 64)
	t.Run("resp2", func(t *testing.T) {
		resp2 := options
		resp2.Protocol = 2
		second, _, evidence, _ := bindTest(t, resp2, 4)
		hash := key("resp2-hash")
		mustNative(t, native.HSet(ctx, hash, "field", "value").Err())
		got := executeTest(t, second, "resp2", "HGETALL", hash)
		if got.Err() != nil || got.Outcome.Value.Commands()[0].Value().Kind() != Array {
			t.Fatal("RESP2 shape changed")
		}
		drain(t, evidence)
	})
	run := func(id string, args ...string) Result {
		t.Helper()
		got := executeTest(t, client, id, args...)
		if got.Err() != nil {
			t.Fatalf("controlled command %s failed: %v", id, got.Err())
		}
		drain(t, inbox)
		return got.Outcome.Value
	}
	t.Run("binary-missing-empty-ttl", func(t *testing.T) {
		binaryKey, emptyKey := key("binary"), key("empty")
		run("set-binary", "SET", binaryKey, "\x00\xffvalue", "PX", "30000")
		run("set-empty", "SET", emptyKey, "")
		binary, _ := run("get-binary", "GET", binaryKey).Commands()[0].Value().Text()
		if binary != "\x00\xffvalue" {
			t.Fatal("binary changed")
		}
		empty, ok := run("get-empty", "GET", emptyKey).Commands()[0].Value().Text()
		if !ok || empty != "" {
			t.Fatal("empty lost")
		}
		missing := executeTest(t, client, "missing", "GET", key("missing"))
		if !errors.Is(missing.Err(), sdk.Nil) {
			t.Fatal("redis.Nil lost")
		}
		drain(t, inbox)
		ttl, _ := run("ttl", "PTTL", binaryKey).Commands()[0].Value().Integer()
		if ttl <= 0 || ttl > 30000 {
			t.Fatal("millisecond TTL changed")
		}
		ttl, _ = run("persistent", "TTL", emptyKey).Commands()[0].Value().Integer()
		if ttl != -1 {
			t.Fatal("persistent sentinel lost")
		}
		ttl, _ = run("absent", "TTL", key("absent")).Commands()[0].Value().Integer()
		if ttl != -2 {
			t.Fatal("absent sentinel lost")
		}
	})
	t.Run("native-data-families", func(t *testing.T) {
		hash, list, set, sorted, bitmap, geo, hll := key("hash"), key("list"), key("set"), key("sorted"), key("bitmap"), key("geo"), key("hll")
		run("hash-set", "HSET", hash, "field", "value")
		if len(run("hash-get", "HGETALL", hash).Commands()[0].Value().Pairs()) != 1 {
			t.Fatal("RESP3 hash map lost")
		}
		run("list-push", "LPUSH", list, "value")
		if len(run("list-pop", "BLPOP", list, "0.1").Commands()[0].Value().Elements()) != 2 {
			t.Fatal("blocking list reply")
		}
		run("set-add", "SADD", set, "one", "two")
		if len(run("set-members", "SMEMBERS", set).Commands()[0].Value().Elements()) != 2 {
			t.Fatal("set reply")
		}
		run("sorted-add", "ZADD", sorted, "1.5", "member")
		if len(run("sorted-range", "ZRANGE", sorted, "0", "-1", "WITHSCORES").Commands()[0].Value().Elements()) != 1 {
			t.Fatal("sorted reply")
		}
		run("bit-set", "SETBIT", bitmap, "3", "1")
		bit, _ := run("bit-get", "GETBIT", bitmap, "3").Commands()[0].Value().Integer()
		if bit != 1 {
			t.Fatal("bitmap changed")
		}
		run("geo-add", "GEOADD", geo, "13.361389", "38.115556", "Palermo")
		if len(run("geo-pos", "GEOPOS", geo, "Palermo").Commands()[0].Value().Elements()) != 1 {
			t.Fatal("geo reply")
		}
		run("pf-add", "PFADD", hll, "member")
		count, _ := run("pf-count", "PFCOUNT", hll).Commands()[0].Value().Integer()
		if count != 1 {
			t.Fatal("HLL changed")
		}
	})
	t.Run("pipeline-partial-effects", func(t *testing.T) {
		wrong, counter := key("wrong-type"), key("counter")
		receipt, err := client.Pipeline(ctx, fault.Correlation{Call: "partial"},
			NewCommand("SET", wrong, "string"), NewCommand("HSET", wrong, "field", "value"), NewCommand("INCR", counter))
		got := resolved(t, receipt, err)
		replies := got.Outcome.Value.Commands()
		var server sdk.Error
		if len(replies) != 3 || replies[0].Err() != nil || !errors.As(replies[1].Err(), &server) || replies[2].Err() != nil {
			t.Fatal("partial replies lost")
		}
		value, err := native.Get(ctx, counter).Int()
		mustNative(t, err)
		if value != 1 {
			t.Fatal("independent partial effect")
		}
		drain(t, inbox)
	})
	t.Run("watch-conflict-and-exec-runtime-errors", func(t *testing.T) {
		watched := key("watched")
		mustNative(t, native.Set(ctx, watched, "1", time.Minute).Err())
		receipt, err := client.Watch(ctx, context.Background(), fault.Correlation{Call: "watch"}, []string{watched}, func(ctx context.Context, session *Session) error {
			mustNative(t, native.Incr(ctx, watched).Err())
			child, err := session.Transaction(ctx, fault.Correlation{Call: "watch-exec", Parent: "watch"}, NewCommand("SET", watched, "no"))
			result := resolved(t, child, err)
			if !errors.Is(result.Err(), sdk.TxFailedErr) || result.Outcome.Value.Commands()[0].State() != TransactionAborted {
				t.Error("watch conflict meaning lost")
			}
			return result.Err()
		})
		got := resolved(t, receipt, err)
		if !errors.Is(got.Err(), sdk.TxFailedErr) {
			t.Fatal("root conflict cause lost")
		}
		drain(t, inbox)
		value, err := native.Get(ctx, watched).Result()
		mustNative(t, err)
		if value != "2" {
			t.Fatal("conflicted EXEC mutated key")
		}
		effect := key("transaction-effect")
		receipt, err = client.Dedicated(ctx, context.Background(), fault.Correlation{Call: "tx"}, watched, func(ctx context.Context, session *Session) error {
			child, err := session.Transaction(ctx, fault.Correlation{Call: "tx-exec", Parent: "tx"},
				NewCommand("HSET", watched, "field", "wrong"), NewCommand("SET", effect, "committed"))
			got := resolved(t, child, err)
			if len(got.Outcome.Value.Commands()) != 2 {
				t.Error("EXEC results missing")
			}
			return got.Err()
		})
		got = resolved(t, receipt, err)
		if got.Err() == nil {
			t.Fatal("runtime transaction error hidden")
		}
		drain(t, inbox)
		value, err = native.Get(ctx, effect).Result()
		mustNative(t, err)
		if value != "committed" {
			t.Fatal("Redis runtime error falsely rolled back other command")
		}
	})
	t.Run("lua-and-scan", func(t *testing.T) {
		scripted := key("scripted")
		value, _ := run("eval", "EVAL", "return redis.call('INCR',KEYS[1])", "1", scripted).Commands()[0].Value().Integer()
		if value != 1 {
			t.Fatal("script reply")
		}
		missing := executeTest(t, client, "no-script", "EVALSHA", strings.Repeat("0", 40), "0")
		if !errors.Is(missing.Err(), sdk.ErrNoScript) {
			t.Fatal("NOSCRIPT identity lost")
		}
		drain(t, inbox)
		cursor := "0"
		found := false
		for pages := 0; pages < 100; pages++ {
			reply := run(fmt.Sprintf("scan-%d", pages), "SCAN", cursor, "MATCH", prefix+"*", "COUNT", "10").Commands()[0].Value().Elements()
			if len(reply) != 2 {
				t.Fatal("scan cursor reply")
			}
			cursor, _ = reply[0].Text()
			for _, item := range reply[1].Elements() {
				text, _ := item.Text()
				if text == scripted {
					found = true
				}
			}
			if cursor == "0" {
				break
			}
		}
		if !found {
			t.Fatal("owned key not observed by scan")
		}
	})
	t.Run("streams", func(t *testing.T) {
		stream := key("stream")
		entry, _ := run("xadd", "XADD", stream, "*", "field", "value").Commands()[0].Value().Text()
		run("group", "XGROUP", "CREATE", stream, "group", "0")
		result := run("xread", "XREADGROUP", "GROUP", "group", "consumer", "COUNT", "1", "STREAMS", stream, ">").Commands()[0].Value()
		if result.Kind() != Map && result.Kind() != Array {
			t.Fatal("stream data lost")
		}
		pending := run("pending", "XPENDING", stream, "group").Commands()[0].Value().Elements()
		count, _ := pending[0].Integer()
		if count != 1 {
			t.Fatal("pending entry missing")
		}
		run("claim", "XAUTOCLAIM", stream, "group", "replacement", "0", "0-0", "COUNT", "1")
		ack, _ := run("ack", "XACK", stream, "group", entry).Commands()[0].Value().Integer()
		if ack != 1 {
			t.Fatal("stream ack changed")
		}
	})
	for _, mode := range []string{"channel", "pattern", "shard"} {
		t.Run("pubsub-"+mode, func(t *testing.T) {
			socketsBefore := client.Stats().Sockets
			channel := prefix + "channel"
			pattern := channel
			if mode == "pattern" {
				pattern = prefix + "*"
			}
			publish := func(payload string) *sdk.IntCmd {
				if mode == "shard" {
					return native.SPublish(ctx, channel, payload)
				}
				return native.Publish(ctx, channel, payload)
			}
			receipt, err := client.Subscribe(ctx, context.Background(), fault.Correlation{Call: "subscription"},
				SubscriptionOptions{Mode: mode, Channels: []string{pattern}}, func(ctx context.Context, subscription *Subscription) error {
					first, err := subscription.Receive(ctx, fault.Correlation{Call: "sub-confirm", Parent: "subscription"})
					got := resolved(t, first, err)
					if got.Err() != nil {
						return got.Err()
					}
					kind, _ := got.Outcome.Value.Commands()[0].Value().Elements()[0].Text()
					if kind != "subscribe" && kind != "psubscribe" && kind != "ssubscribe" {
						t.Error("subscription not confirmed")
					}
					subscribers, err := publish("\x00payload").Result()
					mustNative(t, err)
					if subscribers != 1 {
						t.Error("independent publisher did not observe exactly one owned subscription")
					}
					second, err := subscription.Receive(ctx, fault.Correlation{Call: "message", Parent: "subscription"})
					got = resolved(t, second, err)
					if got.Err() != nil {
						return got.Err()
					}
					message := got.Outcome.Value.Commands()[0].Value().Elements()
					payload, _ := message[3].Text()
					if payload != "\x00payload" {
						t.Error("binary pubsub payload changed")
					}
					return nil
				})
			got := resolved(t, receipt, err)
			if got.Err() != nil {
				t.Fatal(got.Err())
			}
			drain(t, inbox)
			if client.Stats().Sockets != socketsBefore {
				t.Fatal("subscription retained its local socket after release")
			}
			// Socket close is local completion, not a Redis unsubscribe acknowledgement.
			deadline := time.Now().Add(2 * time.Second)
			for {
				subscribers, err := publish("after-close").Result()
				mustNative(t, err)
				if time.Now().After(deadline) {
					t.Fatal("subscription remained on the server after the cleanup observation deadline")
				}
				if subscribers == 0 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
	t.Run("blocking-deadline", func(t *testing.T) {
		work, end := context.WithTimeout(ctx, 80*time.Millisecond)
		defer end()
		receipt, err := client.Execute(work, fault.Correlation{Call: "blocking"}, NewCommand("BLPOP", key("blocked"), "0"))
		got := resolved(t, receipt, err)
		if got.Err() == nil || got.Outcome.Value.Commands()[0].State() != Unknown {
			t.Fatal("blocking timeout fabricated no-effect")
		}
		drain(t, inbox)
	})
	t.Run("experimental-cache-and-automatic", func(t *testing.T) {
		if options.DB != 0 {
			t.Fatal("CSC acceptance fixture must explicitly provide DB 0")
		}
		options.ExperimentalCache = true
		options.ExperimentalAutoPipeline = true
		options.CacheMaxStaleness = time.Minute
		cached, _, evidence, _ := bindTest(t, options, 64)
		target := key("cached")
		mustNative(t, native.Set(ctx, target, "before", time.Minute).Err())
		for index := 0; index < 2; index++ {
			got := executeTest(t, cached, fmt.Sprintf("cache-%d", index), "GET", target)
			if got.Err() != nil || got.Outcome.Value.Commands()[0].State() != CacheOrReply {
				t.Fatal("cache read missing")
			}
			drain(t, evidence)
		}
		mustNative(t, native.Set(ctx, target, "after", time.Minute).Err())
		if stats := cached.Stats(); stats.CacheHits == 0 || stats.CacheEntries > options.CacheEntries && options.CacheEntries != 0 {
			t.Fatal("experimental cache did not actually serve a hit within its entry bound")
		}
		deadline := time.Now().Add(2 * time.Second)
		observed := false
		for time.Now().Before(deadline) {
			got := executeTest(t, cached, "invalidation", "GET", target)
			if got.Err() != nil {
				t.Fatal("cached read failed")
			}
			text, _ := got.Outcome.Value.Commands()[0].Value().Text()
			drain(t, evidence)
			if text == "after" {
				observed = true
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if !observed || time.Now().After(deadline) {
			t.Fatal("invalidation did not arrive before the staleness backstop")
		}
		receipts := make([]*invocation.Receipt[Result], 4)
		written := make([]string, len(receipts))
		for index := range receipts {
			var err error
			written[index] = key(fmt.Sprintf("auto-%d", index))
			receipts[index], err = cached.Submit(ctx, fault.Correlation{Call: fmt.Sprintf("auto-%d", index)}, NewCommand("SET", written[index], "value"))
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, receipt := range receipts {
			if got := resolved(t, receipt, nil); got.Err() != nil {
				t.Fatal(got.Err())
			}
		}
		for _, target := range written {
			value, err := native.Get(ctx, target).Result()
			mustNative(t, err)
			if value != "value" {
				t.Fatal("independent automatic-write readback disagrees")
			}
		}
		drain(t, evidence)
	})
	drain(t, inbox)
}
