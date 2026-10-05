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
	"sync"

	"github.com/frost-leo/fathomry/adapters/v1"
)

// TransactionIDs is a caller-owned exclusivity registry for all sources targeting
// a cluster. Its zero value is usable; do not copy it. It cannot fence another
// process: composition must separately own the remote TransactionalID exclusively.
type TransactionIDs struct {
	private
	mu     sync.Mutex
	owners map[transactionKey]bool
}
type transactionKey struct{ cluster, id string }

func (registry *TransactionIDs) acquire(cluster, id string) (func(), error) {
	if id == "" {
		return func() {}, nil
	}
	if registry == nil {
		return nil, fail(ErrInput, "transaction-identity")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	key := transactionKey{cluster, id}
	if registry.owners[key] {
		return nil, fail(ErrState, "transaction-identity")
	}
	if registry.owners == nil {
		registry.owners = make(map[transactionKey]bool)
	}
	registry.owners[key] = true
	return sync.OnceFunc(func() { registry.mu.Lock(); delete(registry.owners, key); registry.mu.Unlock() }), nil
}

// TransactionState is separate from per-record ACKs and external business effects.
type TransactionState uint8

const (
	TransactionUnobserved TransactionState = iota
	TransactionNotStarted
	TransactionCommitted
	TransactionAborted
	TransactionUnknown
)

func (value Result) Transaction() TransactionState {
	return TransactionState(value.native.Transaction())
}

// ProduceTransaction selects the separate serialized Kafka-only producer.
// Unknown completion permanently fences it; no new identity is manufactured.
func (client *Client) ProduceTransaction(ctx context.Context, messages []Message) (*adapters.Receipt[Result], error) {
	return client.produce(ctx, messages, true)
}
