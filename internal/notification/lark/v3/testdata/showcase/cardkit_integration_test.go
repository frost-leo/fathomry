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
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	lark "github.com/frost-leo/fathomry/internal/notification/lark/v3"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/google/uuid"
)

// TestLiveCardKit creates one synthetic entity without sending a message. It
// explicitly leaves entity retention to Feishu: CardKit has no delete endpoint.
func TestLiveCardKit(t *testing.T) {
	path := os.Getenv("FATHOMRY_FEISHU_CONFIG")
	if path == "" || os.Getenv("FATHOMRY_FEISHU_CARDKIT") != "1" {
		t.Skip("requires explicit CardKit fixture authorization")
	}
	options, err := readSettings(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	selected, err := lark.Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, lark.LimitsV1(options))
	assembly, err := resource.Assemble(ctx, ctx, "cardkit-test", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(context.Background())
	inbox, _ := invocation.NewInbox[lark.Result](1, lark.EvidenceBytesV1(options))
	client, err := lark.Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	session := &session{client, inbox, &output}
	defer func() { t.Log("\n" + output.String()) }()
	await := func(receipt *invocation.Receipt[lark.Result], err error) lark.Result {
		t.Helper()
		result, err := session.await(ctx, receipt, err)
		if err != nil {
			t.Log(liveFailureHint(err))
			t.Fatal(err)
		}
		return result
	}
	card, _, _, _, err := report("")
	if err != nil {
		t.Fatal(err)
	}
	entity := await(client.CreateCard(ctx, callID(), card))
	revision := func(seq int) lark.Revision { return lark.Revision{Sequence: seq, UUID: uuid.NewString()} }
	jsonInput := func(value string) lark.JSON {
		t.Helper()
		data, err := lark.NewJSON([]byte(value))
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	await(client.PatchElement(ctx, callID(), entity.CardID(), "intro", revision(1), jsonInput(`{"content":"**CardKit patch verified.** Synthetic data only."}`)))
	await(client.InsertElements(ctx, callID(), entity.CardID(), revision(2), "append", "", jsonInput(`[{"tag":"markdown","element_id":"extra","content":"Temporary native element."}]`)))
	await(client.UpdateElement(ctx, callID(), entity.CardID(), "extra", revision(3), jsonInput(`{"tag":"markdown","element_id":"extra","content":"Replaced native element."}`)))
	await(client.DeleteElement(ctx, callID(), entity.CardID(), "extra", revision(4)))
	actions, _ := json.Marshal([]any{map[string]any{"action": "partial_update_element", "params": map[string]any{"element_id": "intro", "partial_element": map[string]string{"content": "Batch-updated native card."}}}})
	await(client.BatchUpdateCard(ctx, callID(), entity.CardID(), revision(5), jsonInput(string(actions))))
	await(client.CardSettings(ctx, callID(), entity.CardID(), revision(6), jsonInput(`{"config":{"streaming_mode":true}}`)))
	await(client.ElementContent(ctx, callID(), entity.CardID(), "intro", revision(7), "Finite streaming update; no WebSocket bot runtime."))
	await(client.CardSettings(ctx, callID(), entity.CardID(), revision(8), jsonInput(`{"config":{"streaming_mode":false}}`)))
	await(client.UpdateCard(ctx, callID(), entity.CardID(), revision(9), card))
	receipt, err := client.UpdateCard(ctx, callID(), entity.CardID(), revision(1), card)
	rejected, err := session.await(ctx, receipt, err)
	if err == nil || rejected.Effect() != lark.Rejected {
		t.Fatal("stale sequence was not explicitly rejected")
	}
}
