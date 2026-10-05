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
	"errors"
	"testing"
)

func TestTransactionalIDExclusivity(t *testing.T) {
	peer := publicPeer(t)
	options := peerSettings(peer)
	options.TransactionalID = "gh105-exclusive"
	deps, inbox := testDependencies(t, options, options)
	first := testOpen(t, options, deps)
	if second, err := Open(testContext(t), options, deps); second != nil || !errors.Is(err, ErrState) {
		t.Fatal("overlapping transactional identity accepted", err)
	}
	ack(t, inbox)
	receipt, err := first.Client().ProduceTransaction(testContext(t), []Message{{Topic: "records", Value: []byte("transaction")}})
	if value := await(t, receipt, err); value.Transaction() != TransactionCommitted {
		t.Fatal("transaction outcome collapsed")
	}
	ack(t, inbox)
	if err := first.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	second := testOpen(t, options, deps)
	if second == nil {
		t.Fatal("identity was not released")
	}
}
