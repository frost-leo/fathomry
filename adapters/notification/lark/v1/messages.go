/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package lark

import (
	"context"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/notification/lark/v3"
)

// These conveniences preserve established defaults. Do exposes complete native
// query/body/response choices for the corresponding closed operation.

// Send performs one explicit native notification operation without retry.
func (client *Client) Send(ctx context.Context, to Recipient, uuid string, content Content) (Result, error) {
	return client.dispatch(ctx, "Send", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.Send(work, id, native.Recipient{Type: to.Type, ID: to.ID}, uuid, content.native)
	})
}

// Reply performs one explicit native notification operation without retry.
func (client *Client) Reply(ctx context.Context, messageID, uuid string, inThread bool, content Content) (Result, error) {
	return client.dispatch(ctx, "Reply", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.Reply(work, id, messageID, uuid, inThread, content.native)
	})
}

// UpdateMessage performs one explicit native notification operation without retry.
func (client *Client) UpdateMessage(ctx context.Context, messageID string, content Content) (Result, error) {
	return client.dispatch(ctx, "UpdateMessage", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.UpdateMessage(work, id, messageID, content.native)
	})
}

// PatchMessage performs one explicit native notification operation without retry.
func (client *Client) PatchMessage(ctx context.Context, messageID string, content Content) (Result, error) {
	return client.dispatch(ctx, "PatchMessage", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.PatchMessage(work, id, messageID, content.native)
	})
}

// Recall performs one explicit native notification operation without retry.
func (client *Client) Recall(ctx context.Context, messageID string) (Result, error) {
	return client.dispatch(ctx, "Recall", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.Recall(work, id, messageID)
	})
}

// GetMessage performs one explicit native notification operation without retry.
func (client *Client) GetMessage(ctx context.Context, messageID string) (Result, error) {
	return client.dispatch(ctx, "GetMessage", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.GetMessage(work, id, messageID)
	})
}

// ListMessages performs one explicit native notification operation without retry.
func (client *Client) ListMessages(ctx context.Context, containerType, containerID string, start, end int64, page Page) (Result, error) {
	return client.dispatch(ctx, "ListMessages", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.ListMessages(work, id, containerType, containerID, start, end, native.Page{Size: page.Size, Token: page.Token})
	})
}

// ReadUsers performs one explicit native notification operation without retry.
func (client *Client) ReadUsers(ctx context.Context, messageID string, page Page) (Result, error) {
	return client.dispatch(ctx, "ReadUsers", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.ReadUsers(work, id, messageID, native.Page{Size: page.Size, Token: page.Token})
	})
}

// Forward performs one explicit native notification operation without retry.
func (client *Client) Forward(ctx context.Context, messageID, uuid string, to Recipient) (Result, error) {
	return client.dispatch(ctx, "Forward", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.Forward(work, id, messageID, uuid, native.Recipient{Type: to.Type, ID: to.ID})
	})
}

// MergeForward performs one explicit native notification operation without retry.
func (client *Client) MergeForward(ctx context.Context, to Recipient, uuid string, messageIDs []string) (Result, error) {
	return client.dispatch(ctx, "MergeForward", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.MergeForward(work, id, native.Recipient{Type: to.Type, ID: to.ID}, uuid, messageIDs)
	})
}

// ResolveEmail performs one explicit native notification operation without retry.
func (client *Client) ResolveEmail(ctx context.Context, email string) (Result, error) {
	return client.dispatch(ctx, "ResolveEmail", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.ResolveEmail(work, id, email)
	})
}

// DownloadImage performs one explicit native notification operation without retry.
func (client *Client) DownloadImage(ctx context.Context, key string) (Result, error) {
	return client.dispatch(ctx, "DownloadImage", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.DownloadImage(work, id, key)
	})
}

// DownloadFile performs one explicit native notification operation without retry.
func (client *Client) DownloadFile(ctx context.Context, key string) (Result, error) {
	return client.dispatch(ctx, "DownloadFile", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.DownloadFile(work, id, key)
	})
}

// DownloadResource performs one explicit native notification operation without retry.
func (client *Client) DownloadResource(ctx context.Context, messageID, key, kind string) (Result, error) {
	return client.dispatch(ctx, "DownloadResource", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.DownloadResource(work, id, messageID, key, kind)
	})
}

// CreateCard performs one explicit native notification operation without retry.
func (client *Client) CreateCard(ctx context.Context, content Content) (Result, error) {
	return client.dispatch(ctx, "CreateCard", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.CreateCard(work, id, content.native)
	})
}

// UpdateCard performs one explicit native notification operation without retry.
func (client *Client) UpdateCard(ctx context.Context, cardID string, revision Revision, content Content) (Result, error) {
	return client.dispatch(ctx, "UpdateCard", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.UpdateCard(work, id, cardID, native.Revision{Sequence: revision.Sequence, UUID: revision.UUID}, content.native)
	})
}

// CardSettings performs one explicit native notification operation without retry.
func (client *Client) CardSettings(ctx context.Context, cardID string, revision Revision, settings JSON) (Result, error) {
	return client.dispatch(ctx, "CardSettings", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.CardSettings(work, id, cardID, native.Revision{Sequence: revision.Sequence, UUID: revision.UUID}, settings.native)
	})
}

// BatchUpdateCard performs one explicit native notification operation without retry.
func (client *Client) BatchUpdateCard(ctx context.Context, cardID string, revision Revision, actions JSON) (Result, error) {
	return client.dispatch(ctx, "BatchUpdateCard", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.BatchUpdateCard(work, id, cardID, native.Revision{Sequence: revision.Sequence, UUID: revision.UUID}, actions.native)
	})
}

// InsertElements performs one explicit native notification operation without retry.
func (client *Client) InsertElements(ctx context.Context, cardID string, revision Revision, position, target string, elements JSON) (Result, error) {
	return client.dispatch(ctx, "InsertElements", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.InsertElements(work, id, cardID, native.Revision{Sequence: revision.Sequence, UUID: revision.UUID}, position, target, elements.native)
	})
}

// UpdateElement performs one explicit native notification operation without retry.
func (client *Client) UpdateElement(ctx context.Context, cardID, elementID string, revision Revision, element JSON) (Result, error) {
	return client.dispatch(ctx, "UpdateElement", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.UpdateElement(work, id, cardID, elementID, native.Revision{Sequence: revision.Sequence, UUID: revision.UUID}, element.native)
	})
}

// PatchElement performs one explicit native notification operation without retry.
func (client *Client) PatchElement(ctx context.Context, cardID, elementID string, revision Revision, partial JSON) (Result, error) {
	return client.dispatch(ctx, "PatchElement", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.PatchElement(work, id, cardID, elementID, native.Revision{Sequence: revision.Sequence, UUID: revision.UUID}, partial.native)
	})
}

// DeleteElement performs one explicit native notification operation without retry.
func (client *Client) DeleteElement(ctx context.Context, cardID, elementID string, revision Revision) (Result, error) {
	return client.dispatch(ctx, "DeleteElement", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.DeleteElement(work, id, cardID, elementID, native.Revision{Sequence: revision.Sequence, UUID: revision.UUID})
	})
}

// ElementContent performs one explicit native notification operation without retry.
func (client *Client) ElementContent(ctx context.Context, cardID, elementID string, revision Revision, content string) (Result, error) {
	return client.dispatch(ctx, "ElementContent", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.ElementContent(work, id, cardID, elementID, native.Revision{Sequence: revision.Sequence, UUID: revision.UUID}, content)
	})
}

// AddReaction performs one explicit native notification operation without retry.
func (client *Client) AddReaction(ctx context.Context, messageID, emoji string) (Result, error) {
	return client.dispatch(ctx, "AddReaction", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.AddReaction(work, id, messageID, emoji)
	})
}

// DeleteReaction performs one explicit native notification operation without retry.
func (client *Client) DeleteReaction(ctx context.Context, messageID, reactionID string) (Result, error) {
	return client.dispatch(ctx, "DeleteReaction", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.DeleteReaction(work, id, messageID, reactionID)
	})
}

// ListReactions performs one explicit native notification operation without retry.
func (client *Client) ListReactions(ctx context.Context, messageID string, page Page) (Result, error) {
	return client.dispatch(ctx, "ListReactions", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.ListReactions(work, id, messageID, native.Page{Size: page.Size, Token: page.Token})
	})
}

// SendWebhook performs one explicit native notification operation without retry.
func (client *Client) SendWebhook(ctx context.Context, content Content) (Result, error) {
	return client.dispatch(ctx, "SendWebhook", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.SendWebhook(work, id, content.native)
	})
}

func (client *Client) UploadImage(ctx context.Context, name string, data []byte) (Result, error) {
	return client.UploadAsset(ctx, true, AssetInput{Name: name, ImageType: "message", Bytes: data}, RequestOptions{})
}
func (client *Client) UploadFile(ctx context.Context, input AssetInput) (Result, error) {
	return client.UploadAsset(ctx, false, input, RequestOptions{})
}
