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
	"testing"
)

func TestForwardedRedisAggregateKeepsItsOriginalOwners(t *testing.T) {
	address := peer(t, func(_ net.Conn, _ []string) string { return "+OK\r\n" })
	owner, deps := openTest(t, testSettings(address))
	_, original := owner.Client().Cache().Dedicated(testContext(t), context.Background(), "owned", func(context.Context, *Session) error {
		return errors.Join(errors.New("first failure"), errors.New("second failure"))
	})
	ack(t, deps.Evidence, 1)
	if !errors.Is(original, ErrCommand) || errors.Is(original, ErrMessagingCommand) {
		t.Fatal("initial classification control failed")
	}
	_, forwarded := owner.Client().Messaging().Dedicated(testContext(t), context.Background(), "owned", func(context.Context, *Session) error {
		return original
	})
	ack(t, deps.Evidence, 1)
	if errors.Is(forwarded, ErrMessagingCommand) {
		t.Fatal("re-forwarding a classified Cache aggregate reclassified its retained private causes as Messaging")
	}
	if !errors.Is(forwarded, original) || !errors.Is(forwarded, ErrCommand) {
		t.Fatal("original classification/history lost")
	}
}
