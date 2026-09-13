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

package franz

import (
	"context"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TransactionState reports native Kafka transaction completion only. Aborted
// includes a no-op abort before any remote partition was added; neither it nor
// local client shutdown rolls back external systems or settles business results.
type TransactionState uint8

const (
	TransactionUnobserved TransactionState = iota
	TransactionNotStarted
	TransactionCommitted
	TransactionAborted
	TransactionUnknown
)

// ProduceTransaction atomically commits this bounded Kafka-only batch. It never
// commits consumer offsets, arbitrary external effects or a whole Run. It uses a
// separate producer, so ordinary Produce cannot accidentally join this transaction.
// Any uncertain end fences further transactions on this source until reassembly;
// this method never silently retries an uncertain commit as another transaction.
func (client *Client) ProduceTransaction(ctx context.Context, correlation fault.Correlation, messages []Message) (*invocation.Receipt[Result], error) {
	return client.produce(ctx, correlation, messages, true)
}
func (client *Client) transact(ctx context.Context, call *invocation.Call[Result], records []*kgo.Record, data *resultData) (primary error, cleanupError error) {
	owner := client.owner
	data.transaction = TransactionNotStarted
	select {
	case owner.txGate <- struct{}{}:
		defer func() { <-owner.txGate }()
	case <-ctx.Done():
		return failure(ErrTransaction, "waiting", ctx.Err(), context.Cause(ctx)), nil
	}
	if owner.poisoned.Load() {
		return failure(ErrState, "transaction-fenced"), nil
	}
	if err := owner.transaction.failure(); err != nil {
		return err, nil
	}
	if err := owner.checkIdentityWith(ctx, owner.transaction.Client); err != nil {
		return err, nil
	}
	if ctx.Err() != nil {
		return failure(ErrTransaction, "begin", ctx.Err(), context.Cause(ctx)), nil
	}
	stopWatch := owner.transaction.watch(ctx, owner.settings.CleanupTimeout)
	defer func() {
		if fenced := stopWatch(); fenced != nil {
			owner.poisoned.Store(true)
			primary = failure(ErrTransaction, "producer-fenced", primary, fenced)
		}
	}()
	data.transaction = TransactionUnknown
	if err := owner.transaction.BeginTransaction(); err != nil {
		return nativeFailure(ErrTransaction, "begin", ctx, err), nil
	}
	if fenced := owner.transaction.failure(); fenced != nil {
		return fenced, nil
	}
	primary = client.deliver(ctx, call, owner.transaction, records, data)
	if primary != nil {
		// All callbacks have completed; no buffered records can enter later work.
		cleanup, cancel := context.WithTimeout(context.Background(), owner.settings.CleanupTimeout)
		defer cancel()
		err := owner.transaction.EndTransaction(nativeContext{cleanup}, kgo.TryAbort)
		if err != nil {
			owner.poisoned.Store(true)
			return primary, nativeFailure(ErrCleanup, "abort", cleanup, err)
		}
		data.transaction = TransactionAborted
		return primary, nil
	}
	if err := owner.transaction.EndTransaction(nativeContext{ctx}, kgo.TryCommit); err != nil {
		owner.poisoned.Store(true)
		return nativeFailure(ErrTransaction, "commit", ctx, err), nil
	}
	data.transaction = TransactionCommitted
	return nil, nil
}
