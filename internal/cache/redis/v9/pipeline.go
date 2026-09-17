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

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	sdk "github.com/redis/go-redis/v9"
)

type batchEvidenceHook struct{}

func (batchEvidenceHook) DialHook(next sdk.DialHook) sdk.DialHook          { return next }
func (batchEvidenceHook) ProcessHook(next sdk.ProcessHook) sdk.ProcessHook { return next }
func (batchEvidenceHook) ProcessPipelineHook(next sdk.ProcessPipelineHook) sdk.ProcessPipelineHook {
	return func(ctx context.Context, commands []sdk.Cmder) error {
		err := next(ctx, commands)
		if err != nil {
			for _, command := range commands {
				if cmd, ok := command.(*sdk.Cmd); ok {
					if _, untouched := cmd.Val().(int32); untouched && cmd.Err() == nil {
						cmd.SetErr(err)
					}
				}
			}
		}
		return err
	}
}

// Pipeline preserves input order and every native reply/error. It is not atomic;
// independent Cluster commands can route to different slots/nodes.
func (client *Client) Pipeline(ctx context.Context, id fault.Correlation, commands ...Command) (*invocation.Receipt[Result], error) {
	return client.execute(ctx, id, commands, true, nil, nil, false)
}

// Submit uses the opt-in native deferred autopipeliner. Its one owned waiter
// retains admission and evidence until actual native completion. Canceling a
// Receipt.Wait does not cancel accepted work. At most MaxActive waiters exist.
func (client *Client) Submit(ctx context.Context, id fault.Correlation, command Command) (*invocation.Receipt[Result], error) {
	if client == nil || client.owner == nil || client.owner.automatic == nil {
		return nil, failure(ErrUnsupported, "automatic")
	}
	if err := client.owner.settings.check([]Command{command}, false); err != nil {
		return nil, err
	}
	call, err := client.begin(ctx, id, "automatic", invocation.Async, nil)
	if err != nil {
		return nil, err
	}
	work, cancel, err := client.work(ctx)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return call.Receipt(), nil
	}
	if work.Err() != nil {
		call.Complete(invocation.Outcome[Result]{Primary: nativeFailure(work, work.Err())})
		cancel()
		return call.Receipt(), nil
	}
	cmd := command.native(work)
	_, _ = call.Attempt()
	future := client.owner.automatic.Submit(work, cmd)
	go func() {
		defer cancel()
		err := future.Wait()
		result, bounds := collect(work, []*sdk.Cmd{cmd}, true, client.owner.settings.ExperimentalCache, client.owner.settings, err)
		call.Complete(invocation.Outcome[Result]{Present: true, Value: result, Primary: nativeFailure(work, errors.Join(err, bounds))})
	}()
	return call.Receipt(), nil
}

// Automatic is the blocking face of the same bounded native batching engine.
func (client *Client) Automatic(ctx context.Context, id fault.Correlation, command Command) (*invocation.Receipt[Result], error) {
	receipt, err := client.Submit(ctx, id, command)
	if err != nil {
		return receipt, err
	}
	_, err = receipt.WaitReleased(ctx)
	return receipt, err
}
