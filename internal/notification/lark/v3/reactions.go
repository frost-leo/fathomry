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
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

// AddReaction adds the application's native emoji reaction to a message.
func (client *Client) AddReaction(ctx context.Context, id fault.Correlation, messageID, emoji string) (*invocation.Receipt[Result], error) {
	path, err := messagePath(messageID)
	if err != nil {
		return nil, err
	}
	if !identifier(emoji) {
		return nil, failure(ErrInput, "reaction")
	}
	return client.execute(ctx, id, requestSpec{operation: "add-reaction", method: "POST", path: path + "/reactions", target: messageID, ackKey: "reaction_id",
		body: &larkim.CreateMessageReactionReqBody{ReactionType: &larkim.Emoji{EmojiType: pointer(emoji)}}})
}

// DeleteReaction removes one reaction, subject to the application's authority.
func (client *Client) DeleteReaction(ctx context.Context, id fault.Correlation, messageID, reactionID string) (*invocation.Receipt[Result], error) {
	path, err := messagePath(messageID)
	if err != nil {
		return nil, err
	}
	if !identifier(reactionID) {
		return nil, failure(ErrInput, "reaction-id")
	}
	return client.execute(ctx, id, requestSpec{operation: "delete-reaction", method: "DELETE", path: path + "/reactions/" + reactionID, target: messageID})
}

// ListReactions reads one explicit page without polling.
func (client *Client) ListReactions(ctx context.Context, id fault.Correlation, messageID string, page Page) (*invocation.Receipt[Result], error) {
	path, err := messagePath(messageID)
	if err != nil {
		return nil, err
	}
	query, err := page.query()
	if err != nil {
		return nil, err
	}
	return client.execute(ctx, id, requestSpec{operation: "list-reactions", method: "GET", path: path + "/reactions", query: query, target: messageID, ackKey: "page"})
}
