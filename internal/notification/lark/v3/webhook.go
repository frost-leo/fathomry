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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

func webhookSign(timestamp, secret string) string {
	// Feishu uses timestamp + newline + secret as the HMAC key and an empty message.
	mac := hmac.New(sha256.New, []byte(timestamp+"\n"+secret))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// SendWebhook sends to the source's fixed signed custom bot. It cannot upload,
// query, recall, receive or target an individual user. No message ID is invented
// from the custom-bot acknowledgement. The 20 KiB bound includes the signature.
func (client *Client) SendWebhook(ctx context.Context, id fault.Correlation, content Content) (*invocation.Receipt[Result], error) {
	if err := client.ready("webhook", true); err != nil {
		return nil, err
	}
	switch content.kind {
	case "text", "post", "image", "share_chat", "interactive":
	default:
		return nil, failure(ErrUnsupported, "webhook-content")
	}
	if !contentValid(content) {
		return nil, failure(ErrInput, "webhook")
	}
	if content.kind == "interactive" {
		var shape struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(content.json.Bytes(), &shape)
		if shape.Type != "" {
			return nil, failure(ErrUnsupported, "webhook-card-reference")
		}
	}
	return client.execute(ctx, id, requestSpec{operation: "webhook", method: "POST", webhook: true, bodyBytes: len(content.json.value), webhookContent: content})
}
func (owned *owner) webhookBody(content Content) any {
	stamp := strconv.FormatInt(time.Now().Unix(), 10)
	body := map[string]any{"timestamp": stamp, "sign": webhookSign(stamp, owned.settings.WebhookSecret), "msg_type": content.kind}
	key := "content"
	if content.kind == "interactive" {
		key = "card"
	}
	body[key] = json.RawMessage(content.json.value)
	return body
}
