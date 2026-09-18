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
	"net/mail"
	"strconv"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

// Recipient supplies an explicit ID kind: open_id, union_id, user_id, email or
// chat_id. It never silently resolves or substitutes another account.
type Recipient struct {
	private
	Type, ID string
}

func (recipient Recipient) valid() bool {
	switch recipient.Type {
	case "open_id", "union_id", "user_id", "chat_id":
		return identifier(recipient.ID)
	case "email":
		if len(recipient.ID) > 320 {
			return false
		}
		parsed, err := mail.ParseAddress(recipient.ID)
		return err == nil && parsed.Address == recipient.ID
	default:
		return false
	}
}
func pointer[T any](value T) *T { return &value }
func messagePath(id string) (string, error) {
	if !identifier(id) {
		return "", failure(ErrInput, "message-id")
	}
	return "/open-apis/im/v1/messages/" + id, nil
}
func contentValid(value Content) bool { return value.kind != "" && value.json.value != "" }

// Send creates one application-bot message. UUID is an explicit caller-owned
// deduplication identity (1..50 bytes); the platform's window is finite. Reuse it
// only for the same intent. Unknown outcomes never trigger automatic resending.
func (client *Client) Send(ctx context.Context, id fault.Correlation, to Recipient, uuid string, content Content) (*invocation.Receipt[Result], error) {
	if !to.valid() || !identifier(uuid) || len(uuid) > 50 || !contentValid(content) {
		return nil, failure(ErrInput, "send")
	}
	return client.execute(ctx, id, requestSpec{operation: "send", method: "POST", path: "/open-apis/im/v1/messages",
		query: larkcore.QueryParams{"receive_id_type": {to.Type}}, target: to.Type + ":" + to.ID, uuid: uuid, ackKey: "message_id",
		body:      &larkim.CreateMessageReqBody{ReceiveId: pointer(to.ID), MsgType: pointer(content.kind), Content: pointer(content.json.value), Uuid: pointer(uuid)},
		bodyBytes: len(content.json.value), wireLimit: messageLimit(content)})
}

// Reply creates a reply or thread reply and retains its own UUID/effect evidence.
func (client *Client) Reply(ctx context.Context, id fault.Correlation, messageID, uuid string, inThread bool, content Content) (*invocation.Receipt[Result], error) {
	path, err := messagePath(messageID)
	if err != nil {
		return nil, err
	}
	if !identifier(uuid) || len(uuid) > 50 || !contentValid(content) {
		return nil, failure(ErrInput, "reply")
	}
	return client.execute(ctx, id, requestSpec{operation: "reply", method: "POST", path: path + "/reply", target: messageID, uuid: uuid, ackKey: "message_id",
		body:      &larkim.ReplyMessageReqBody{Content: pointer(content.json.value), MsgType: pointer(content.kind), ReplyInThread: pointer(inThread), Uuid: pointer(uuid)},
		bodyBytes: len(content.json.value), wireLimit: messageLimit(content)})
}

// UpdateMessage edits text/rich-post content. Interactive cards use PatchMessage.
// A successful edit cannot erase prior visibility or prove every client refreshed.
func (client *Client) UpdateMessage(ctx context.Context, id fault.Correlation, messageID string, content Content) (*invocation.Receipt[Result], error) {
	path, err := messagePath(messageID)
	if err != nil {
		return nil, err
	}
	if content.kind != "text" && content.kind != "post" {
		return nil, failure(ErrUnsupported, "update-message")
	}
	return client.execute(ctx, id, requestSpec{operation: "update-message", method: "PUT", path: path, target: messageID,
		body: &larkim.UpdateMessageReqBody{MsgType: pointer(content.kind), Content: pointer(content.json.value)}, bodyBytes: len(content.json.value), wireLimit: messageLimit(content)})
}

// PatchMessage replaces a sent interactive card's content. CardKit entities and
// element sequence updates are separate operations.
func (client *Client) PatchMessage(ctx context.Context, id fault.Correlation, messageID string, content Content) (*invocation.Receipt[Result], error) {
	path, err := messagePath(messageID)
	if err != nil {
		return nil, err
	}
	if content.kind != "interactive" {
		return nil, failure(ErrUnsupported, "patch-message")
	}
	return client.execute(ctx, id, requestSpec{operation: "patch-message", method: "PATCH", path: path, target: messageID,
		body: &larkim.PatchMessageReqBody{Content: pointer(content.json.value)}, bodyBytes: len(content.json.value), wireLimit: messageLimit(content)})
}

// Recall requests recall of one message. This is a separate effect, not rollback,
// deletion of uploaded assets or proof a recipient never saw the message.
func (client *Client) Recall(ctx context.Context, id fault.Correlation, messageID string) (*invocation.Receipt[Result], error) {
	path, err := messagePath(messageID)
	if err != nil {
		return nil, err
	}
	return client.execute(ctx, id, requestSpec{operation: "recall", method: "DELETE", path: path, target: messageID})
}

// GetMessage queries current message information, including deletion/update flags
// and original card JSON (user_card_content), rather than its text-only projection.
// A merged result includes its parent and related descendants in native order.
func (client *Client) GetMessage(ctx context.Context, id fault.Correlation, messageID string) (*invocation.Receipt[Result], error) {
	path, err := messagePath(messageID)
	if err != nil {
		return nil, err
	}
	return client.execute(ctx, id, requestSpec{operation: "get-message", method: "GET", path: path, target: messageID, ackKey: "get-message",
		query: larkcore.QueryParams{"card_msg_content_type": {"user_card_content"}, "user_id_type": {"open_id"}}})
}

// ListMessages reads one chat/thread page. Start/End are Unix seconds as required
// by Feishu; zero omits that bound. No background poller or auto-pager is created.
func (client *Client) ListMessages(ctx context.Context, id fault.Correlation, containerType, containerID string, start, end int64, page Page) (*invocation.Receipt[Result], error) {
	if containerType != "chat" && containerType != "thread" || !identifier(containerID) || start < 0 || end < 0 || end != 0 && end < start {
		return nil, failure(ErrInput, "list-messages")
	}
	query, err := page.query()
	if err != nil {
		return nil, err
	}
	query.Set("container_id_type", containerType)
	query.Set("container_id", containerID)
	query.Set("card_msg_content_type", "user_card_content")
	if start != 0 {
		query.Set("start_time", strconv.FormatInt(start, 10))
	}
	if end != 0 {
		query.Set("end_time", strconv.FormatInt(end, 10))
	}
	return client.execute(ctx, id, requestSpec{operation: "list-messages", method: "GET", path: "/open-apis/im/v1/messages", query: query, target: containerID, ackKey: "message-page"})
}

// ReadUsers queries one page of server-reported readers, not inferred human
// comprehension or chart rendering. Permissions and message age are platform limits.
func (client *Client) ReadUsers(ctx context.Context, id fault.Correlation, messageID string, page Page) (*invocation.Receipt[Result], error) {
	path, err := messagePath(messageID)
	if err != nil {
		return nil, err
	}
	query, err := page.query()
	if err != nil {
		return nil, err
	}
	query.Set("user_id_type", "open_id")
	return client.execute(ctx, id, requestSpec{operation: "read-users", method: "GET", path: path + "/read_users", query: query, target: messageID, ackKey: "page"})
}

// Forward forwards a message with its own caller-owned deduplication UUID.
func (client *Client) Forward(ctx context.Context, id fault.Correlation, messageID, uuid string, to Recipient) (*invocation.Receipt[Result], error) {
	path, err := messagePath(messageID)
	if err != nil {
		return nil, err
	}
	if !to.valid() || !identifier(uuid) || len(uuid) > 50 {
		return nil, failure(ErrInput, "forward")
	}
	return client.execute(ctx, id, requestSpec{operation: "forward", method: "POST", path: path + "/forward",
		query: larkcore.QueryParams{"receive_id_type": {to.Type}, "uuid": {uuid}}, body: &larkim.ForwardMessageReqBody{ReceiveId: pointer(to.ID)}, target: messageID, uuid: uuid, ackKey: "message_id"})
}

// MergeForward forwards 1..50 messages from one conversation. InvalidMessageIDs
// retains per-message rejection independently of an acknowledged merged message.
// The input slice is borrowed during the call; retained related IDs are copied.
func (client *Client) MergeForward(ctx context.Context, id fault.Correlation, to Recipient, uuid string, messageIDs []string) (*invocation.Receipt[Result], error) {
	if !to.valid() || !identifier(uuid) || len(uuid) > 50 || len(messageIDs) == 0 || len(messageIDs) > 50 {
		return nil, failure(ErrInput, "merge-forward")
	}
	for _, messageID := range messageIDs {
		if !identifier(messageID) {
			return nil, failure(ErrInput, "merge-forward")
		}
	}
	return client.execute(ctx, id, requestSpec{operation: "merge-forward", method: "POST", path: "/open-apis/im/v1/messages/merge_forward",
		query: larkcore.QueryParams{"receive_id_type": {to.Type}, "uuid": {uuid}}, body: &larkim.MergeForwardMessageReqBody{ReceiveId: pointer(to.ID), MessageIdList: messageIDs},
		target: to.Type + ":" + to.ID, uuid: uuid, related: messageIDs, ackKey: "merged_message"})
}
func messageLimit(content Content) int {
	if content.kind == "text" {
		return 150 << 10
	}
	return 30 << 10
}

// MessageInfo is a copied, immutable subset of a queried message. JSONData on the
// result retains other native fields without exposing mutable SDK models.
type MessageInfo struct {
	private
	id, chat, kind, content, sender string
	deleted, updated                bool
	deletedKnown, updatedKnown      bool
}

func (value MessageInfo) ID() string       { return value.id }
func (value MessageInfo) ChatID() string   { return value.chat }
func (value MessageInfo) Type() string     { return value.kind }
func (value MessageInfo) Content() string  { return value.content }
func (value MessageInfo) SenderID() string { return value.sender }

// Deleted returns the deletion flag and whether the API supplied it.
func (value MessageInfo) Deleted() (bool, bool) { return value.deleted, value.deletedKnown }

// Updated returns the edit flag and whether the API supplied it.
func (value MessageInfo) Updated() (bool, bool) { return value.updated, value.updatedKnown }

// Messages returns caller-owned snapshots from get/list responses; other
// operations return an empty slice. Original native JSON stays independently held.
func (value Result) Messages() []MessageInfo {
	return append([]MessageInfo(nil), value.messages...)
}

func messageSnapshots(entries []json.RawMessage, target string) ([]MessageInfo, error) {
	native := make(map[string]*larkim.Message, len(entries))
	items := make([]MessageInfo, 0, len(entries))
	for _, entry := range entries {
		fields, err := exactFields(entry, "message_id", "msg_type", "chat_id", "body", "sender", "deleted", "updated", "upper_message_id")
		if err != nil {
			return nil, failure(ErrResponse, "message", err)
		}
		for name, members := range map[string][]string{"body": {"content"}, "sender": {"id", "id_type", "sender_type", "tenant_key"}} {
			if raw, present := fields[name]; present && string(raw) != "null" {
				if _, err := exactFields(raw, members...); err != nil {
					return nil, failure(ErrResponse, "message", err)
				}
			}
		}
		var item larkim.Message
		if err := json.Unmarshal(entry, &item); err != nil || !identifier(deref(item.MessageId)) {
			return nil, failure(ErrResponse, "message", err)
		}
		id := deref(item.MessageId)
		if native[id] != nil {
			return nil, failure(ErrResponse, "message-identity")
		}
		native[id] = &item
		info := MessageInfo{id: deref(item.MessageId), chat: deref(item.ChatId), kind: deref(item.MsgType), deleted: deref(item.Deleted), updated: deref(item.Updated)}
		info.deletedKnown = item.Deleted != nil
		info.updatedKnown = item.Updated != nil
		if item.Body != nil {
			info.content = deref(item.Body.Content)
		}
		if item.Sender != nil {
			info.sender = deref(item.Sender.Id)
		}
		items = append(items, info)
	}
	if target != "" {
		if err := validateMessageFamily(native, target); err != nil {
			return nil, err
		}
	}
	return items, nil
}

func validateMessageFamily(messages map[string]*larkim.Message, target string) error {
	parent := messages[target]
	if parent == nil || len(messages) > 1 && deref(parent.MsgType) != "merge_forward" {
		return failure(ErrResponse, "message-identity")
	}
	visited := map[string]uint8{target: 2}
	for id := range messages {
		path := []string{}
		for visited[id] != 2 {
			if visited[id] == 1 {
				return failure(ErrResponse, "message-family")
			}
			visited[id] = 1
			path = append(path, id)
			upper := deref(messages[id].UpperMessageId)
			if ancestor := messages[upper]; ancestor == nil || deref(ancestor.MsgType) != "merge_forward" {
				return failure(ErrResponse, "message-family")
			}
			id = upper
		}
		for _, id := range path {
			visited[id] = 2
		}
	}
	return nil
}
func deref[T any](value *T) (zero T) {
	if value != nil {
		return *value
	}
	return zero
}
