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

package kafka

import (
	"context"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/broker/franz/v1"
)

// ConsumerProgress counts returned records and physical scan progress, not Items.
type ConsumerProgress struct {
	private
	Next     Position
	End      int64
	Received uint64
	Complete bool
}

func (value Result) ConsumerProgress() ConsumerProgress {
	item := value.native.ConsumerProgress()
	return ConsumerProgress{Next: position(item.Next), End: item.End, Received: item.Received, Complete: item.Complete}
}

// Consumer retains a direct physical cursor and its acquired source generation.
// It is not a group member. Close never commits and needs no new admission slot.
type Consumer struct {
	private
	operation *operation
	native    *native.Consumer
}

// Consume retains the declared direct interval until Close or lifetime cancellation.
// A final page remains open for explicit processing and optional Commit.
func (client *Client) Consume(lifetime context.Context, interval Range) (*Consumer, error) {
	var consumer *Consumer
	receipt, err := client.dispatch(lifetime, "consume", func(op *operation, bound *native.Client) {
		cursor, err := bound.Consume(op.lifetime, op.correlation(), interval.native())
		if cursor == nil {
			op.finish(nil, err)
			return
		}
		consumer = &Consumer{operation: op, native: cursor}
		if keepErr := op.keep(cursor.Receipt(), err, func() { cursor.Close() }); keepErr != nil {
			cursor.Close()
			op.finish(nil, keepErr)
		}
	})
	if consumer == nil {
		_, err = result(receipt, err)
	}
	return consumer, err
}
func (consumer *Consumer) Receipt() *adapters.Receipt[Result] {
	if consumer == nil || consumer.operation == nil {
		return nil
	}
	return consumer.operation.call.Receipt()
}

// Next performs one bounded synchronous page; concurrent child calls refuse.
func (consumer *Consumer) Next(ctx context.Context) (Result, error) {
	if consumer == nil || consumer.native == nil {
		return Result{}, fail(ErrInput, "next")
	}
	return consumer.operation.child(ctx, "consume-page", consumer.native.Next)
}

// Commit explicitly declares the read prefix eligible for the standalone
// checkpoint. It does not establish business processing or group ownership.
func (consumer *Consumer) Commit(ctx context.Context) (Result, error) {
	if consumer == nil || consumer.native == nil {
		return Result{}, fail(ErrInput, "commit")
	}
	return consumer.operation.child(ctx, "consume-commit", consumer.native.Commit)
}

// Close stops new pages and joins the existing root without committing.
func (consumer *Consumer) Close(ctx context.Context) (Result, error) {
	if consumer == nil || consumer.native == nil || ctx == nil {
		return Result{}, fail(ErrInput, "close")
	}
	consumer.native.Close()
	return waitResult(ctx, consumer.Receipt())
}
