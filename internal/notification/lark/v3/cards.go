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

package lark

import (
	"context"
	"encoding/json"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	larkcard "github.com/larksuite/oapi-sdk-go/v3/service/cardkit/v1"
)

// Revision is caller-owned CardKit sequencing: Sequence must be 1..2147483647,
// strictly increasing across ALL updates to the same entity. UUID is optional
// native idempotency data. This Provider owns no distributed sequence allocator.
type Revision struct {
	private
	Sequence int
	UUID     string
}

func (value Revision) valid() bool {
	return value.Sequence > 0 && value.Sequence <= 2147483647 && (value.UUID == "" || identifier(value.UUID) && len(value.UUID) <= 50)
}
func cardPath(id string) (string, error) {
	if !identifier(id) {
		return "", failure(ErrInput, "card-id")
	}
	return "/open-apis/cardkit/v1/cards/" + id, nil
}
func entity(content Content) (*larkcard.Card, error) {
	if content.kind != "interactive" {
		return nil, failure(ErrInput, "card")
	}
	var shape struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	_ = json.Unmarshal(content.json.Bytes(), &shape)
	kind, data := "card_json", content.json.value
	if shape.Type == "card" {
		return nil, failure(ErrUnsupported, "card-reference")
	}
	if shape.Type == "template" {
		kind, data = "template", string(shape.Data)
	}
	return &larkcard.Card{Type: &kind, Data: &data}, nil
}

// CreateCard creates a JSON 2.0 or published-template entity. The returned card ID
// is not yet a sent message; sending and updates have separate effects.
func (client *Client) CreateCard(ctx context.Context, id fault.Correlation, content Content) (*invocation.Receipt[Result], error) {
	card, err := entity(content)
	if err != nil {
		return nil, err
	}
	return client.execute(ctx, id, requestSpec{operation: "create-card", method: "POST", path: "/open-apis/cardkit/v1/cards", ackKey: "card_id",
		body: &larkcard.CreateCardReqBody{Type: card.Type, Data: card.Data}, bodyBytes: len(content.json.value)})
}

// UpdateCard replaces a JSON 2.0 entity with an explicitly sequenced update.
func (client *Client) UpdateCard(ctx context.Context, id fault.Correlation, cardID string, revision Revision, content Content) (*invocation.Receipt[Result], error) {
	path, err := cardPath(cardID)
	if err != nil {
		return nil, err
	}
	card, err := entity(content)
	if err != nil {
		return nil, err
	}
	if !revision.valid() || deref(card.Type) != "card_json" {
		return nil, failure(ErrInput, "update-card")
	}
	return client.execute(ctx, id, requestSpec{operation: "update-card", method: "PUT", path: path, target: cardID, uuid: revision.UUID,
		body: &larkcard.UpdateCardReqBody{Card: card, Sequence: pointer(revision.Sequence), Uuid: optional(revision.UUID)}, bodyBytes: len(content.json.value)})
}

// CardSettings updates native config/card_link fields, including streaming-mode
// settings. No WebSocket stream, callbacks or background worker is started.
func (client *Client) CardSettings(ctx context.Context, id fault.Correlation, cardID string, revision Revision, settings JSON) (*invocation.Receipt[Result], error) {
	path, err := cardPath(cardID)
	if err != nil {
		return nil, err
	}
	if !revision.valid() || !object(settings) {
		return nil, failure(ErrInput, "card-settings")
	}
	return client.execute(ctx, id, requestSpec{operation: "card-settings", method: "PATCH", path: path + "/settings", target: cardID, uuid: revision.UUID,
		body: &larkcard.SettingsCardReqBody{Settings: pointer(settings.value), Sequence: pointer(revision.Sequence), Uuid: optional(revision.UUID)}, bodyBytes: len(settings.value)})
}

// BatchUpdateCard submits one bounded native actions array. It does not infer
// atomicity or independently retry actions after an ambiguous response.
func (client *Client) BatchUpdateCard(ctx context.Context, id fault.Correlation, cardID string, revision Revision, actions JSON) (*invocation.Receipt[Result], error) {
	path, err := cardPath(cardID)
	if err != nil {
		return nil, err
	}
	if !revision.valid() || len(actions.value) == 0 || actions.value[0] != '[' {
		return nil, failure(ErrInput, "card-actions")
	}
	return client.execute(ctx, id, requestSpec{operation: "batch-update-card", method: "POST", path: path + "/batch_update", target: cardID, uuid: revision.UUID,
		body: &larkcard.BatchUpdateCardReqBody{Actions: pointer(actions.value), Sequence: pointer(revision.Sequence), Uuid: optional(revision.UUID)}, bodyBytes: len(actions.value)})
}
func optional(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
