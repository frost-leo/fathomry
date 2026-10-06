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
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	sdk "github.com/redis/go-redis/v9"
)

func TestSessionMixedTransactionRootAndChildCustody(t *testing.T) {
	var unwatch atomic.Int32
	address := peer(t, func(_ net.Conn, args []string) string {
		switch strings.ToUpper(args[0]) {
		case "MULTI", "WATCH":
			return "+OK\r\n"
		case "UNWATCH":
			unwatch.Add(1)
			return "+OK\r\n"
		case "EXEC":
			return "*2\r\n+OK\r\n-WRONGTYPE messaging-private-canary\r\n"
		default:
			return "+QUEUED\r\n"
		}
	})
	owner, deps := openTest(t, testSettings(address))
	var escaped *Session
	lifecycle, err := owner.Client().Cache().Watch(testContext(t), context.Background(), []string{"owned"}, func(ctx context.Context, session *Session) error {
		copy := *session
		escaped = &copy
		session.state.op.family.gate.Lock()
		_, gateErr := copy.Execute(ctx, command(t, Cache, "GET", "owned"))
		session.state.op.family.gate.Unlock()
		if !errors.Is(gateErr, ErrState) {
			t.Error("copy minted gate")
		}
		output, err := session.Transaction(ctx, command(t, Cache, "SET", "owned", "value"), command(t, Messaging, "XADD", "owned", "*", "f", "v"))
		if !errors.Is(err, ErrMessagingCommand) || len(output.Replies()) != 2 || output.Replies()[0].State() != Replied {
			t.Error("mixed transaction error lost", err)
		}
		if output.Attribution().Parent == 0 || output.Kind() != Commands {
			t.Error("child attribution missing")
		}
		ack(t, deps.Evidence, 1)
		return err
	})
	if !errors.Is(err, ErrMessagingCommand) || lifecycle.Kind() != Lifecycle || len(lifecycle.Replies()) != 0 || unwatch.Load() != 1 {
		t.Fatal("root disguised as execution", err)
	}
	if _, err := escaped.Execute(testContext(t), command(t, Cache, "GET", "owned")); !errors.Is(err, ErrState) {
		t.Fatal("callback escape")
	}
	ack(t, deps.Evidence, 1)
	transaction, err := owner.Client().Cache().Transaction(testContext(t), context.Background(), "owned",
		command(t, Cache, "SET", "owned", "value"), command(t, Messaging, "XADD", "owned", "*", "f", "v"))
	if !errors.Is(err, ErrMessagingCommand) || transaction.Lifecycle().Kind() != Lifecycle || len(transaction.Execution().Replies()) != 2 {
		t.Fatal("transaction convenience lost child")
	}
	ack(t, deps.Evidence, 2)
}

func TestPubSubReconnectIsNotDurableDelivery(t *testing.T) {
	sockets := make(chan net.Conn, 8)
	address := peer(t, func(connection net.Conn, args []string) string {
		sockets <- connection
		return "*3\r\n$9\r\nsubscribe\r\n$5\r\nowned\r\n:1\r\n"
	})
	owner, deps := openTest(t, testSettings(address))
	_, err := owner.Client().Messaging().Subscribe(testContext(t), context.Background(), SubscriptionOptions{Mode: "channel", Channels: []string{"owned"}}, func(ctx context.Context, subscription *Subscription) error {
		initial, err := subscription.Receive(ctx)
		if err != nil {
			return err
		}
		ack(t, deps.Evidence, 1)
		first, _ := initial.Replies()[0].Value().Elements()[0].Text()
		if first != "subscribe" {
			t.Error("initial confirmation absent")
		}
		_ = (<-sockets).Close()
		failed, err := subscription.Receive(ctx)
		if err == nil || failed.Replies()[0].HasValue() {
			t.Error("disconnect fabricated message")
		}
		ack(t, deps.Evidence, 1)
		repeated, err := subscription.Receive(ctx)
		if err != nil {
			return err
		}
		ack(t, deps.Evidence, 1)
		kind, _ := repeated.Replies()[0].Value().Elements()[0].Text()
		if kind != "subscribe" {
			t.Error("reconnect confirmation fabricated application delivery")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ack(t, deps.Evidence, 1)
}

func TestWatchConflictCallbackErrorAndPanic(t *testing.T) {
	address := peer(t, func(_ net.Conn, args []string) string {
		if strings.EqualFold(args[0], "exec") {
			return "*-1\r\n"
		}
		if strings.EqualFold(args[0], "set") {
			return "+QUEUED\r\n"
		}
		return "+OK\r\n"
	})
	owner, deps := openTest(t, testSettings(address))
	_, err := owner.Client().Cache().Watch(testContext(t), context.Background(), []string{"owned"}, func(ctx context.Context, session *Session) error {
		output, err := session.Transaction(ctx, command(t, Cache, "SET", "owned", "value"))
		if !IsWatchConflict(err) || !errors.Is(err, sdk.TxFailedErr) || output.Replies()[0].State() != TransactionAborted || output.Replies()[0].HasValue() {
			t.Error("conflict changed", err)
		}
		ack(t, deps.Evidence, 1)
		return err
	})
	if !IsWatchConflict(err) {
		t.Fatal("root conflict lost")
	}
	ack(t, deps.Evidence, 1)
	forwarded, _ := failure.New(adapters.Definitions()[0], failure.Location{Operation: "synthetic"})
	_, err = owner.Client().Messaging().Dedicated(testContext(t), context.Background(), "owned", func(context.Context, *Session) error { return forwarded })
	if !errors.Is(err, forwarded) {
		t.Fatal("public callback error lost")
	}
	var occurrence failure.Occurrence
	if !errors.As(err, &occurrence) || occurrence.Failure().Diagnostic().Definition.Code != forwarded.Diagnostic().Definition.Code {
		t.Fatal("public callback owner relabeled")
	}
	ack(t, deps.Evidence, 1)
	_, err = owner.Client().Messaging().Dedicated(testContext(t), context.Background(), "owned", func(context.Context, *Session) error { panic("private-panic") })
	if !errors.Is(err, ErrMessagingState) {
		t.Fatal("panic lost lifecycle owner", err)
	}
	ack(t, deps.Evidence, 1)
	cleanup, cancel := context.WithCancel(context.Background())
	cancel()
	value, err := owner.Client().Cache().Watch(testContext(t), cleanup, []string{"owned"}, func(context.Context, *Session) error { return nil })
	if !errors.Is(err, context.Canceled) || !errors.Is(value.Cleanup(), ErrCleanup) {
		t.Fatal("cleanup join lost", err)
	}
	ack(t, deps.Evidence, 1)
}

func TestNonCooperativeCallbackRetainsActualOwner(t *testing.T) {
	address := peer(t, func(_ net.Conn, _ []string) string { return "+OK\r\n" })
	owner, deps := openTest(t, testSettings(address))
	entered, unblock := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(unblock) })
	defer release()
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		_, _ = owner.Client().Cache().Dedicated(testContext(t), context.Background(), "owned", func(context.Context, *Session) error { close(entered); <-unblock; return nil })
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := owner.Close(ctx); !errors.Is(err, context.DeadlineExceeded) || owner.ShutdownComplete() {
		t.Fatal("cancellation forged quiescence", err)
	}
	stats, _ := deps.Runtime.Inspect()
	if stats.Active < 2 {
		t.Fatal("source/callback charge vanished")
	}
	release()
	<-returned
	if err := owner.Close(testContext(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("cleanup continuation failed", err)
	}
}

func TestDeferredCompletionAfterStoppedWaitingAndAliasCapacity(t *testing.T) {
	entered, unblock := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(unblock) })
	defer release()
	var once sync.Once
	address := peer(t, func(_ net.Conn, _ []string) string { once.Do(func() { close(entered) }); <-unblock; return "+OK\r\n" })
	value := testSettings(address)
	value.ExperimentalAutoPipeline = true
	value.MaxActive = 1
	owner, deps := openTest(t, value)
	receipt, err := owner.Client().Cache().Submit(testContext(t), command(t, Cache, "SET", "owned", "value"))
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	wait, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := receipt.Wait(wait); !errors.Is(err, context.Canceled) {
		t.Fatal("wait did not stop")
	}
	if stats, _ := deps.Runtime.Inspect(); stats.Active != 2 {
		t.Fatal("accepted work released before native completion")
	}
	_, err = owner.Client().Messaging().Execute(testContext(t), command(t, Messaging, "PUBLISH", "channel", "data"))
	if !errors.Is(err, adapters.ErrLimit) {
		t.Fatal("view multiplied capacity", err)
	}
	release()
	snapshot, err := receipt.WaitReleased(testContext(t))
	if err != nil || snapshot.Err() != nil {
		t.Fatal("late evidence missing", err)
	}
	output, _ := snapshot.ValueCopy()
	if output.Replies()[0].State() != Replied {
		t.Fatal("late reply lost")
	}
	ack(t, deps.Evidence, 1)
}

func TestSubscriptionEventsCustodyAndEscapes(t *testing.T) {
	for _, mode := range []string{"channel", "pattern", "shard", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			address := peer(t, func(_ net.Conn, args []string) string {
				if mode == "malformed" {
					return "*0\r\n"
				}
				return "*3\r\n$" + strconv.Itoa(len(args[0])) + "\r\n" + strings.ToLower(args[0]) + "\r\n$5\r\nowned\r\n:1\r\n" +
					"*3\r\n$7\r\nmessage\r\n$5\r\nowned\r\n$0\r\n\r\n"
			})
			owner, deps := openTest(t, testSettings(address))
			selected := mode
			if mode == "malformed" {
				selected = "channel"
			}
			var escaped *Subscription
			root, err := owner.Client().Messaging().Subscribe(testContext(t), context.Background(), SubscriptionOptions{Mode: selected, Channels: []string{"owned"}}, func(ctx context.Context, subscription *Subscription) error {
				copy := *subscription
				escaped = &copy
				first, err := subscription.Receive(ctx)
				if mode == "malformed" {
					ack(t, deps.Evidence, 1)
					return err
				}
				if err != nil {
					return err
				}
				if first.Kind() != SubscriptionEvent || first.Attribution().Parent == 0 || first.Replies()[0].Capability() != Messaging {
					t.Error("subscription child lost")
				}
				ack(t, deps.Evidence, 1)
				second, err := subscription.Receive(ctx)
				if err != nil {
					return err
				}
				values := second.Replies()[0].Value().Elements()
				if len(values) != 4 {
					t.Fatal("message structure lost")
				}
				if data, ok := values[3].Text(); !ok || data != "" {
					t.Error("empty payload lost")
				}
				ack(t, deps.Evidence, 1)
				return nil
			})
			if mode == "malformed" {
				if !errors.Is(err, ErrMessagingProtocol) {
					t.Fatal("malformed receive lost", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if root.Kind() != Lifecycle {
				t.Fatal("confirmation confused with lifecycle")
			}
			if _, err := escaped.Receive(testContext(t)); !errors.Is(err, ErrMessagingState) {
				t.Fatal("subscription escape")
			}
			ack(t, deps.Evidence, 1)
		})
	}
}
