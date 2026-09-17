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

func TestAutomaticWaitingAndBorrowedOwnership(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var once sync.Once
	address := peer(t, func([]string) string { once.Do(func() { close(entered) }); <-release; return "+OK\r\n" })
	options := testOptions(address)
	options.ExperimentalAutoPipeline = true
	options.MaxActive = 1
	_, assembly, inbox, selected := bindTest(t, options, 8)
	borrowed := resource.Borrow("alias", assembly, selected)
	alias, err := resource.Assemble(context.Background(), context.Background(), "alias-scope", borrowed)
	if err != nil {
		t.Fatal(err)
	}
	defer alias.Close(context.Background())
	facade, err := Bind(alias, borrowed, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := facade.Submit(context.Background(), fault.Correlation{Call: "accepted"}, NewCommand("SET", "owned", "value"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("no native entry")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := receipt.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("waiting cancellation lost")
	}
	if err := assembly.Close(context.Background()); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("owner released active alias")
	}
	_, err = facade.Execute(context.Background(), fault.Correlation{Call: "overload"}, NewCommand("GET", "owned"))
	if !errors.Is(err, resource.ErrCapacity) {
		t.Fatal("alias multiplied allowance")
	}
	unblock()
	got := resolved(t, receipt, nil)
	if got.Err() != nil || got.Source.Scope != "redis-test" {
		t.Fatal("late result/source lost")
	}
	drain(t, inbox)
	if err := alias.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type batchWidthHook struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
	width   atomic.Int32
}

func (hook *batchWidthHook) block()                                  { hook.once.Do(func() { close(hook.entered); <-hook.release }) }
func (hook *batchWidthHook) DialHook(next sdk.DialHook) sdk.DialHook { return next }
func (hook *batchWidthHook) ProcessHook(next sdk.ProcessHook) sdk.ProcessHook {
	return func(ctx context.Context, cmd sdk.Cmder) error {
		if cmd.Name() == "set" {
			hook.block()
		}
		return next(ctx, cmd)
	}
}
func (hook *batchWidthHook) ProcessPipelineHook(next sdk.ProcessPipelineHook) sdk.ProcessPipelineHook {
	return func(ctx context.Context, commands []sdk.Cmder) error {
		count := int32(0)
		for _, cmd := range commands {
			if cmd.Name() == "set" {
				count++
			}
		}
		for current := hook.width.Load(); count > current; current = hook.width.Load() {
			if hook.width.CompareAndSwap(current, count) {
				break
			}
		}
		if count > 0 {
			hook.block()
		}
		return next(ctx, commands)
	}
}
func TestAutomaticBatchWidthAndInitializationFailure(t *testing.T) {
	for _, rejected := range []bool{false, true} {
		t.Run(fmt.Sprintf("initialization-rejected-%t", rejected), func(t *testing.T) {
			var writes atomic.Int32
			address := peer(t, func(args []string) string {
				if args[0] == "select" && rejected {
					return "-ERR DB index out of range\r\n"
				}
				if strings.EqualFold(args[0], "set") {
					writes.Add(1)
				}
				return "+OK\r\n"
			})
			options := testOptions(address)
			options.ExperimentalAutoPipeline = true
			if rejected {
				options.DB = 65535
			}
			client, _, inbox, _ := bindTest(t, options, 8)
			hook := &batchWidthHook{entered: make(chan struct{}), release: make(chan struct{})}
			unblock := sync.OnceFunc(func() { close(hook.release) })
			defer unblock()
			client.owner.native.AddHook(hook)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			receipts := make([]*invocation.Receipt[Result], 4)
			var err error
			receipts[0], err = client.Submit(ctx, fault.Correlation{Call: "first"}, NewCommand("SET", "owned:0", "value"))
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-hook.entered:
			case <-ctx.Done():
				t.Fatal("native dispatch not reached")
			}
			for index := 1; index < len(receipts); index++ {
				receipts[index], err = client.Submit(ctx, fault.Correlation{Call: fmt.Sprintf("batch-%d", index)}, NewCommand("SET", fmt.Sprintf("owned:%d", index), "value"))
				if err != nil {
					t.Fatal(err)
				}
			}
			unblock()
			for _, receipt := range receipts {
				got := resolved(t, receipt, nil)
				if rejected {
					var original sdk.Error
					if !errors.As(got.Err(), &original) {
						t.Fatal("native initialization cause lost")
					}
					reply := got.Outcome.Value.Commands()[0]
					if reply.State() == Replied || reply.Err() == nil {
						t.Fatal("failed batch invented a command reply")
					}
				} else if got.Err() != nil {
					t.Fatal(got.Err())
				}
			}
			if hook.width.Load() < 2 {
				t.Fatal("fixture did not execute a multi-command native batch")
			}
			expected := int32(4)
			if rejected {
				expected = 0
			}
			if writes.Load() != expected {
				t.Fatal("independent write-count oracle disagrees")
			}
			drain(t, inbox)
		})
	}
}
