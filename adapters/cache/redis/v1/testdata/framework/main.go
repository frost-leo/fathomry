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

package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/frost-leo/fathomry/adapters/cache/redis/v1"
	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"io"
	"os"
	"time"

	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	"maps"
	"reflect"
	"slices"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "redis framework consumer failed")
		os.Exit(1)
	}
	fmt.Println("redis framework consumer passed")
}
func run() error {
	redis.DisableNativeLogging()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	input, err := io.ReadAll(io.LimitReader(os.Stdin, 1<<20))
	if err != nil {
		return err
	}
	loaded, err := configsource.Prepare(ctx, configsource.Schema[redis.Settings]{Version: 1, Validate: func(_ context.Context, value redis.Settings) error { return redis.Validate(value) }},
		[]configsource.Layer{{Kind: configsource.Local, Encoding: configsource.JSON, Content: input}})
	if err != nil {
		return err
	}
	value, err := loaded.ValueCopy()
	if err != nil {
		return err
	}
	prepared, err := redis.Prepare(value)
	if err != nil {
		return err
	}
	policy, err := redis.Compose(prepared, prepared, prepared, prepared)
	if err != nil {
		return err
	}
	runtime, err := framework.New(ctx, framework.Options{Operations: policy.Runtime})
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[redis.Result](policy.Evidence)
	if err != nil {
		return err
	}
	receiver, err := framework.StartReceiver(ctx, inbox, framework.ReceiverOptions{}, func(_ context.Context, snapshot adapters.Snapshot[redis.Result]) error {
		if !snapshot.Info().Released {
			return errors.New("unreleased evidence")
		}
		return nil
	})
	if err != nil {
		return err
	}
	defer receiver.Close(context.Background())
	deps := redis.Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	clone := func(value redis.Settings) redis.Settings {
		value.Addrs = slices.Clone(value.Addrs)
		value.AllowedAddrs = slices.Clone(value.AllowedAddrs)
		value.Commands = slices.Clone(value.Commands)
		value.AdminCommands = slices.Clone(value.AdminCommands)
		value.Shards = maps.Clone(value.Shards)
		if value.MaxIdleTime != nil {
			idle := *value.MaxIdleTime
			value.MaxIdleTime = &idle
		}
		return value
	}
	bind := func(name string, mode resource.Policy) (resource.Ref[redis.Handle], error) {
		return resource.Bind(runtime.Resources(), resource.Binding[redis.Settings, redis.Handle]{
			Name: name, Policy: mode, Clone: clone, Equal: func(a, b redis.Settings) bool { return reflect.DeepEqual(a, b) },
			Select: func(view settings.View) (redis.Settings, error) {
				snapshot, err := settings.As[redis.Settings](view)
				if err != nil {
					return redis.Settings{}, err
				}
				return snapshot.ValueCopy()
			},
			Build: func(ctx context.Context, value redis.Settings) (*resource.Instance[redis.Handle], error) {
				owner, err := redis.Open(ctx, value, deps)
				if owner == nil {
					return nil, err
				}
				return &resource.Instance[redis.Handle]{Value: owner.Handle(), Release: owner.Release}, err
			},
		})
	}
	fixedRef, err := bind("fixed", resource.Fixed)
	if err != nil {
		return err
	}
	followRef, err := bind("follow", resource.Follow)
	if err != nil {
		return err
	}
	apply := func(value redis.Settings) error {
		snapshot, err := settings.New(value, clone)
		if err != nil {
			return err
		}
		update, err := runtime.Resources().Apply(ctx, snapshot.View())
		if err != nil {
			return err
		}
		return update.Wait(ctx)
	}
	value.Name = "first"
	if err := apply(value); err != nil {
		return err
	}
	fixed, err := redis.Using(ctx, fixedRef, policy.Budget, deps)
	if err != nil {
		return err
	}
	follow, err := redis.Using(ctx, followRef, policy.Budget, deps)
	if err != nil {
		return err
	}
	value.Name = "second"
	if err := apply(value); err != nil {
		return err
	}
	ping, err := redis.NewCommand(redis.Cache, "PING")
	if err != nil {
		return err
	}
	for _, test := range []struct {
		client *redis.Client
		name   string
	}{{fixed, "first"}, {follow, "second"}} {
		result, err := test.client.Cache().Execute(ctx, ping)
		if err != nil {
			return err
		}
		if result.Source().Name != test.name || result.Attribution().Source.Generation == 0 {
			return errors.New("generation attribution")
		}
	}
	if err := runtime.Close(ctx); err != nil {
		return err
	}
	if err := receiver.Finish(ctx); err != nil {
		return err
	}
	status, err := receiver.Status()
	if err != nil {
		return err
	}
	if status.Delivered < 5 || status.Failures != 0 {
		return errors.New("receiver incomplete")
	}
	return nil
}
