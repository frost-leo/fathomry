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
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "redis public consumer failed")
		os.Exit(1)
	}
	fmt.Println("redis public consumer passed")
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
	settings, err := loaded.ValueCopy()
	if err != nil {
		return err
	}
	prepared, err := redis.Prepare(settings)
	if err != nil {
		return err
	}
	policy, err := prepared.Policy()
	if err != nil {
		return err
	}
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[redis.Result](policy.Evidence)
	if err != nil {
		return err
	}
	owner, err := prepared.Open(ctx, redis.Dependencies{Runtime: runtime, Evidence: inbox})
	if owner != nil {
		defer owner.Close(context.Background())
	}
	if err != nil {
		return err
	}
	cache, messaging := owner.Client().Cache(), owner.Client().Messaging()
	get, err := cache.Command("GET", "owned")
	if err != nil {
		return err
	}
	stream, err := messaging.Command("XADD", "owned", "*", "f", "v")
	if err != nil {
		return err
	}
	output, err := cache.Pipeline(ctx, get, stream)
	if !redis.IsNull(err) || !errors.Is(err, redis.ErrMessagingCommand) || len(output.Replies()) != 2 ||
		output.Replies()[0].Value().Kind() != redis.Null || output.Replies()[1].Capability() != redis.Messaging {
		return errors.New("mixed classification")
	}
	lifecycle, err := messaging.Dedicated(ctx, context.Background(), "owned", func(ctx context.Context, session *redis.Session) error {
		_, err := session.Execute(ctx, get)
		return err
	})
	if !redis.IsNull(err) || lifecycle.Kind() != redis.Lifecycle {
		return errors.New("callback custody")
	}
	if _, err := owner.Client().Stats(ctx); err != nil {
		return err
	}
	if err := owner.Close(ctx); err != nil {
		return err
	}
	if !owner.ShutdownComplete() {
		return errors.New("source retained")
	}
	status, err := inbox.Inspect()
	if err != nil {
		return err
	}
	for range status.Outstanding {
		delivery, err := inbox.NextReleased(ctx)
		if err != nil {
			return err
		}
		if err := delivery.Ack(); err != nil {
			return err
		}
	}
	return runtime.Close(ctx)
}
