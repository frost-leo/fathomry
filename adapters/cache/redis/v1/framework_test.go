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
	"reflect"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func cloneSettings(value Settings) Settings {
	value.Addrs = append([]string(nil), value.Addrs...)
	value.AllowedAddrs = append([]string(nil), value.AllowedAddrs...)
	value.Commands = append([]string(nil), value.Commands...)
	value.AdminCommands = append([]string(nil), value.AdminCommands...)
	if value.Shards != nil {
		shards := make(map[string]string, len(value.Shards))
		for name, address := range value.Shards {
			shards[name] = address
		}
		value.Shards = shards
	}
	if value.MaxIdleTime != nil {
		idle := *value.MaxIdleTime
		value.MaxIdleTime = &idle
	}
	return value
}

func TestRetryRetainsRedisEvidence(t *testing.T) {
	address := peer(t, func(_ net.Conn, _ []string) string { return "+OK\r\n" })
	owner, deps := openTest(t, testSettings(address))
	original, err := owner.Client().Cache().Execute(testContext(t), command(t, Cache, "SET", "owned", "effect"))
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := deps.Evidence.NextReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Retry(); err != nil {
		t.Fatal(err)
	}
	delivery, err = deps.Evidence.NextReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	recovered, _ := snapshot.ValueCopy()
	if recovered.Attribution().Sequence != original.Attribution().Sequence {
		t.Fatal("evidence retry became a new operation")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
}

func TestFrameworkFixedFollowRetainsCallbackGenerationAndLargerBudget(t *testing.T) {
	first := testSettings(peer(t, func(_ net.Conn, _ []string) string { return "+first\r\n" }))
	first.Name = "first"
	second := testSettings(peer(t, func(_ net.Conn, _ []string) string { return "+second\r\n" }))
	second.Name = "second"
	initial, _ := Prepare(first)
	replacement, _ := Prepare(second)
	// Fixed and Follow each build their own source, with one retained old Follow.
	policy, err := Compose(initial, initial, replacement, replacement)
	if err != nil {
		t.Fatal(err)
	}
	compositionContext, stopComposition := context.WithCancel(context.Background())
	defer stopComposition()
	runtime, err := adapters.New(compositionContext, policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	resourceScope, err := resource.New(compositionContext, resource.Options{})
	if err != nil {
		stopComposition()
		err = errors.Join(err, runtime.Close(context.Background()))
		t.Fatal(err)
	}
	closeComposition := func(cleanup context.Context) error {
		stopComposition()
		return errors.Join(runtime.Close(cleanup), resourceScope.Close(cleanup))
	}
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	deps := Dependencies{Runtime: runtime, Evidence: inbox}
	var mutex sync.Mutex
	var owners []*Owner
	t.Cleanup(func() {
		if err := closeComposition(testContext(t)); err != nil {
			t.Error(err)
		}
		mutex.Lock()
		defer mutex.Unlock()
		for _, owner := range owners {
			if !owner.ShutdownComplete() {
				t.Error("retained native source abandoned")
			}
		}
		status, _ := inbox.Inspect()
		ack(t, inbox, status.Outstanding)
	})
	refusal := errors.New("synthetic candidate refusal")
	bind := func(name string, mode resource.Policy) resource.Ref[Handle] {
		ref, err := resource.Bind(resourceScope, resource.Binding[Settings, Handle]{
			Name: name, Policy: mode, Clone: cloneSettings, Equal: func(left, right Settings) bool { return reflect.DeepEqual(left, right) },
			Select: func(view settings.View) (Settings, error) {
				snapshot, err := settings.As[Settings](view)
				if err != nil {
					return Settings{}, err
				}
				return snapshot.ValueCopy()
			},
			Build: func(ctx context.Context, value Settings) (*resource.Instance[Handle], error) {
				owner, err := Open(ctx, value, deps)
				if owner == nil {
					return nil, err
				}
				mutex.Lock()
				owners = append(owners, owner)
				mutex.Unlock()
				instance := &resource.Instance[Handle]{Value: owner.Handle(), Release: owner.Release}
				if value.Name == "refused" {
					return instance, errors.Join(err, refusal)
				}
				return instance, err
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	fixedRef, followRef := bind("fixed", resource.Fixed), bind("follow", resource.Follow)
	apply := func(value Settings) error {
		snapshot, err := settings.New(value, cloneSettings)
		if err != nil {
			return err
		}
		update, err := resourceScope.Apply(testContext(t), snapshot.View())
		if err != nil {
			return err
		}
		return update.Wait(testContext(t))
	}
	if err := apply(first); err != nil {
		t.Fatal(err)
	}
	fixed, err := Using(testContext(t), fixedRef, policy.Budget, deps)
	if err != nil {
		t.Fatal(err)
	}
	follow, err := Using(testContext(t), followRef, policy.Budget, deps)
	if err != nil {
		t.Fatal(err)
	}
	old, _ := followRef.Inspect()
	entered, unblock := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(unblock) })
	defer release()
	ended := make(chan error, 1)
	go func() {
		_, err := follow.Cache().Dedicated(testContext(t), context.Background(), "owned", func(ctx context.Context, session *Session) error {
			close(entered)
			<-unblock
			result, err := session.Execute(ctx, command(t, Cache, "GET", "owned"))
			if err != nil {
				return err
			}
			text, _ := result.Replies()[0].Value().Text()
			if text != "first" || result.Source().Name != "first" || result.Attribution().Source.Generation != old.Generation {
				return errors.New("callback retargeted")
			}
			ack(t, inbox, 1)
			return nil
		})
		ended <- err
	}()
	<-entered
	if err := apply(second); err != nil {
		t.Fatal(err)
	}
	current, _ := followRef.Inspect()
	if current.Generation == old.Generation || current.Retiring != 1 {
		t.Fatal("old generation not retained")
	}
	for _, test := range []struct {
		client   *Client
		expected string
	}{{follow, "second"}, {fixed, "first"}} {
		result, err := test.client.Cache().Execute(testContext(t), command(t, Cache, "GET", "owned"))
		if err != nil {
			t.Fatal(err)
		}
		text, _ := result.Replies()[0].Value().Text()
		if text != test.expected || result.Source().Name != test.expected {
			t.Fatal("wrong borrowed generation")
		}
		ack(t, inbox, 1)
	}
	release()
	if err := <-ended; err != nil {
		t.Fatal(err)
	}
	ack(t, inbox, 1)
	rejected := second
	rejected.Name = "refused"
	if err := apply(rejected); !errors.Is(err, refusal) {
		t.Fatal("partial replacement error lost", err)
	}
	status, _ := followRef.Inspect()
	if status.Generation != current.Generation {
		t.Fatal("failed candidate replaced last good")
	}
	larger := second
	larger.MaxReplyBytes = 1 << 20
	// Capacity in the old composition may reject construction outright. Either
	// path must reject BEFORE a larger native call can be silently undercharged.
	if err := apply(larger); err == nil {
		_, err = follow.Cache().Execute(testContext(t), command(t, Cache, "GET", "owned"))
		if !errors.Is(err, ErrLimit) {
			t.Fatal("larger adopted source undercharged", err)
		}
	} else if !errors.Is(err, adapters.ErrLimit) {
		t.Fatal("unexpected replacement failure", err)
	}
}
