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

package minio

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/objectstore/v1"
)

func TestDelegationGrantsAndSensitiveExtraction(t *testing.T) {
	peer := newPeer(t)
	options := peer.options
	options.PresignGET = false
	test := newFixture(t, options)
	ctx := testContext(t)
	before := peer.requests()
	receipt, err := test.client.Presign(ctx, SignRequest{Method: GET, Address: Address{Key: "owned/key"}, Expiry: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil || !errors.Is(snapshot.Err(), ErrAuthority) {
		t.Fatal("grant ignored")
	}
	delivery, err := test.inbox.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	result := test.result(test.client.Presign(ctx, SignRequest{Method: PUT, Address: Address{Key: "owned/key"}, IfAbsent: true, Expiry: time.Minute}))
	signed, ok := result.Delegation()
	if !ok || !strings.Contains(signed.URL(), "X-Amz-Signature=") || result.Transfer().Effect != objectstore.NotSubmitted {
		t.Fatal("issuance facts")
	}
	headers := signed.HeadersCopy()
	headers.Set("If-None-Match", "changed")
	if signed.HeadersCopy().Get("If-None-Match") != "*" {
		t.Fatal("signed headers aliased")
	}
	if _, err := json.Marshal(signed); !errors.Is(err, ErrSerialization) {
		t.Fatal("URL serialized")
	}
	if peer.requests() != before {
		t.Fatal("issuance used service I/O")
	}
}
