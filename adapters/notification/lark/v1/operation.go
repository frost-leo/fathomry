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
	"io"
	"net/http"
	"net/url"
)

// Operation is a closed notification-native route, not an arbitrary HTTP URL.
type Operation string

const (
	// IMV1BatchMessageDelete selects the pinned native im.v1.batchMessage.Delete operation.
	IMV1BatchMessageDelete Operation = "im.v1.batchMessage.Delete"
	// IMV1BatchMessageGetProgress selects the pinned native im.v1.batchMessage.GetProgress operation.
	IMV1BatchMessageGetProgress Operation = "im.v1.batchMessage.GetProgress"
	// IMV1BatchMessageReadUser selects the pinned native im.v1.batchMessage.ReadUser operation.
	IMV1BatchMessageReadUser Operation = "im.v1.batchMessage.ReadUser"
	// IMV1ChatCreate selects the pinned native im.v1.chat.Create operation.
	IMV1ChatCreate Operation = "im.v1.chat.Create"
	// IMV1ChatDelete selects the pinned native im.v1.chat.Delete operation.
	IMV1ChatDelete Operation = "im.v1.chat.Delete"
	// IMV1ChatGet selects the pinned native im.v1.chat.Get operation.
	IMV1ChatGet Operation = "im.v1.chat.Get"
	// IMV1ChatLink selects the pinned native im.v1.chat.Link operation.
	IMV1ChatLink Operation = "im.v1.chat.Link"
	// IMV1ChatList selects the pinned native im.v1.chat.List operation.
	IMV1ChatList Operation = "im.v1.chat.List"
	// IMV1ChatSearch selects the pinned native im.v1.chat.Search operation.
	IMV1ChatSearch Operation = "im.v1.chat.Search"
	// IMV1ChatUpdate selects the pinned native im.v1.chat.Update operation.
	IMV1ChatUpdate Operation = "im.v1.chat.Update"
	// IMV1ChatAnnouncementGet selects the pinned native im.v1.chatAnnouncement.Get operation.
	IMV1ChatAnnouncementGet Operation = "im.v1.chatAnnouncement.Get"
	// IMV1ChatAnnouncementPatch selects the pinned native im.v1.chatAnnouncement.Patch operation.
	IMV1ChatAnnouncementPatch Operation = "im.v1.chatAnnouncement.Patch"
	// IMV1ChatManagersAddManagers selects the pinned native im.v1.chatManagers.AddManagers operation.
	IMV1ChatManagersAddManagers Operation = "im.v1.chatManagers.AddManagers"
	// IMV1ChatManagersDeleteManagers selects the pinned native im.v1.chatManagers.DeleteManagers operation.
	IMV1ChatManagersDeleteManagers Operation = "im.v1.chatManagers.DeleteManagers"
	// IMV1ChatMembersCreate selects the pinned native im.v1.chatMembers.Create operation.
	IMV1ChatMembersCreate Operation = "im.v1.chatMembers.Create"
	// IMV1ChatMembersDelete selects the pinned native im.v1.chatMembers.Delete operation.
	IMV1ChatMembersDelete Operation = "im.v1.chatMembers.Delete"
	// IMV1ChatMembersGet selects the pinned native im.v1.chatMembers.Get operation.
	IMV1ChatMembersGet Operation = "im.v1.chatMembers.Get"
	// IMV1ChatMembersIsInChat selects the pinned native im.v1.chatMembers.IsInChat operation.
	IMV1ChatMembersIsInChat Operation = "im.v1.chatMembers.IsInChat"
	// IMV1ChatMembersMeJoin selects the pinned native im.v1.chatMembers.MeJoin operation.
	IMV1ChatMembersMeJoin Operation = "im.v1.chatMembers.MeJoin"
	// IMV1ChatMenuItemPatch selects the pinned native im.v1.chatMenuItem.Patch operation.
	IMV1ChatMenuItemPatch Operation = "im.v1.chatMenuItem.Patch"
	// IMV1ChatMenuTreeCreate selects the pinned native im.v1.chatMenuTree.Create operation.
	IMV1ChatMenuTreeCreate Operation = "im.v1.chatMenuTree.Create"
	// IMV1ChatMenuTreeDelete selects the pinned native im.v1.chatMenuTree.Delete operation.
	IMV1ChatMenuTreeDelete Operation = "im.v1.chatMenuTree.Delete"
	// IMV1ChatMenuTreeGet selects the pinned native im.v1.chatMenuTree.Get operation.
	IMV1ChatMenuTreeGet Operation = "im.v1.chatMenuTree.Get"
	// IMV1ChatMenuTreeSort selects the pinned native im.v1.chatMenuTree.Sort operation.
	IMV1ChatMenuTreeSort Operation = "im.v1.chatMenuTree.Sort"
	// IMV1ChatModerationGet selects the pinned native im.v1.chatModeration.Get operation.
	IMV1ChatModerationGet Operation = "im.v1.chatModeration.Get"
	// IMV1ChatModerationUpdate selects the pinned native im.v1.chatModeration.Update operation.
	IMV1ChatModerationUpdate Operation = "im.v1.chatModeration.Update"
	// IMV1ChatTabCreate selects the pinned native im.v1.chatTab.Create operation.
	IMV1ChatTabCreate Operation = "im.v1.chatTab.Create"
	// IMV1ChatTabDeleteTabs selects the pinned native im.v1.chatTab.DeleteTabs operation.
	IMV1ChatTabDeleteTabs Operation = "im.v1.chatTab.DeleteTabs"
	// IMV1ChatTabListTabs selects the pinned native im.v1.chatTab.ListTabs operation.
	IMV1ChatTabListTabs Operation = "im.v1.chatTab.ListTabs"
	// IMV1ChatTabSortTabs selects the pinned native im.v1.chatTab.SortTabs operation.
	IMV1ChatTabSortTabs Operation = "im.v1.chatTab.SortTabs"
	// IMV1ChatTabUpdateTabs selects the pinned native im.v1.chatTab.UpdateTabs operation.
	IMV1ChatTabUpdateTabs Operation = "im.v1.chatTab.UpdateTabs"
	// IMV1ChatTopNoticeDeleteTopNotice selects the pinned native im.v1.chatTopNotice.DeleteTopNotice operation.
	IMV1ChatTopNoticeDeleteTopNotice Operation = "im.v1.chatTopNotice.DeleteTopNotice"
	// IMV1ChatTopNoticePutTopNotice selects the pinned native im.v1.chatTopNotice.PutTopNotice operation.
	IMV1ChatTopNoticePutTopNotice Operation = "im.v1.chatTopNotice.PutTopNotice"
	// IMV1FileCreate selects the pinned native im.v1.file.Create operation.
	IMV1FileCreate Operation = "im.v1.file.Create"
	// IMV1FileGet selects the pinned native im.v1.file.Get operation.
	IMV1FileGet Operation = "im.v1.file.Get"
	// IMV1ImageCreate selects the pinned native im.v1.image.Create operation.
	IMV1ImageCreate Operation = "im.v1.image.Create"
	// IMV1ImageGet selects the pinned native im.v1.image.Get operation.
	IMV1ImageGet Operation = "im.v1.image.Get"
	// IMV1MessageCreate selects the pinned native im.v1.message.Create operation.
	IMV1MessageCreate Operation = "im.v1.message.Create"
	// IMV1MessageDelete selects the pinned native im.v1.message.Delete operation.
	IMV1MessageDelete Operation = "im.v1.message.Delete"
	// IMV1MessageForward selects the pinned native im.v1.message.Forward operation.
	IMV1MessageForward Operation = "im.v1.message.Forward"
	// IMV1MessageGet selects the pinned native im.v1.message.Get operation.
	IMV1MessageGet Operation = "im.v1.message.Get"
	// IMV1MessageList selects the pinned native im.v1.message.List operation.
	IMV1MessageList Operation = "im.v1.message.List"
	// IMV1MessageMergeForward selects the pinned native im.v1.message.MergeForward operation.
	IMV1MessageMergeForward Operation = "im.v1.message.MergeForward"
	// IMV1MessagePatch selects the pinned native im.v1.message.Patch operation.
	IMV1MessagePatch Operation = "im.v1.message.Patch"
	// IMV1MessagePushFollowUp selects the pinned native im.v1.message.PushFollowUp operation.
	IMV1MessagePushFollowUp Operation = "im.v1.message.PushFollowUp"
	// IMV1MessageReadUsers selects the pinned native im.v1.message.ReadUsers operation.
	IMV1MessageReadUsers Operation = "im.v1.message.ReadUsers"
	// IMV1MessageReply selects the pinned native im.v1.message.Reply operation.
	IMV1MessageReply Operation = "im.v1.message.Reply"
	// IMV1MessageSearch selects the pinned native im.v1.message.Search operation.
	IMV1MessageSearch Operation = "im.v1.message.Search"
	// IMV1MessageUpdate selects the pinned native im.v1.message.Update operation.
	IMV1MessageUpdate Operation = "im.v1.message.Update"
	// IMV1MessageUrgentApp selects the pinned native im.v1.message.UrgentApp operation.
	IMV1MessageUrgentApp Operation = "im.v1.message.UrgentApp"
	// IMV1MessageUrgentPhone selects the pinned native im.v1.message.UrgentPhone operation.
	IMV1MessageUrgentPhone Operation = "im.v1.message.UrgentPhone"
	// IMV1MessageUrgentSms selects the pinned native im.v1.message.UrgentSms operation.
	IMV1MessageUrgentSms Operation = "im.v1.message.UrgentSms"
	// IMV1MessageReactionBatchQuery selects the pinned native im.v1.messageReaction.BatchQuery operation.
	IMV1MessageReactionBatchQuery Operation = "im.v1.messageReaction.BatchQuery"
	// IMV1MessageReactionCreate selects the pinned native im.v1.messageReaction.Create operation.
	IMV1MessageReactionCreate Operation = "im.v1.messageReaction.Create"
	// IMV1MessageReactionDelete selects the pinned native im.v1.messageReaction.Delete operation.
	IMV1MessageReactionDelete Operation = "im.v1.messageReaction.Delete"
	// IMV1MessageReactionList selects the pinned native im.v1.messageReaction.List operation.
	IMV1MessageReactionList Operation = "im.v1.messageReaction.List"
	// IMV1MessageResourceGet selects the pinned native im.v1.messageResource.Get operation.
	IMV1MessageResourceGet Operation = "im.v1.messageResource.Get"
	// IMV1PinCreate selects the pinned native im.v1.pin.Create operation.
	IMV1PinCreate Operation = "im.v1.pin.Create"
	// IMV1PinDelete selects the pinned native im.v1.pin.Delete operation.
	IMV1PinDelete Operation = "im.v1.pin.Delete"
	// IMV1PinList selects the pinned native im.v1.pin.List operation.
	IMV1PinList Operation = "im.v1.pin.List"
	// IMV1ThreadForward selects the pinned native im.v1.thread.Forward operation.
	IMV1ThreadForward Operation = "im.v1.thread.Forward"
	// IMV2AppFeedCardCreate selects the pinned native im.v2.appFeedCard.Create operation.
	IMV2AppFeedCardCreate Operation = "im.v2.appFeedCard.Create"
	// IMV2AppFeedCardBatchDelete selects the pinned native im.v2.appFeedCardBatch.Delete operation.
	IMV2AppFeedCardBatchDelete Operation = "im.v2.appFeedCardBatch.Delete"
	// IMV2AppFeedCardBatchUpdate selects the pinned native im.v2.appFeedCardBatch.Update operation.
	IMV2AppFeedCardBatchUpdate Operation = "im.v2.appFeedCardBatch.Update"
	// IMV2BizEntityTagRelationCreate selects the pinned native im.v2.bizEntityTagRelation.Create operation.
	IMV2BizEntityTagRelationCreate Operation = "im.v2.bizEntityTagRelation.Create"
	// IMV2BizEntityTagRelationGet selects the pinned native im.v2.bizEntityTagRelation.Get operation.
	IMV2BizEntityTagRelationGet Operation = "im.v2.bizEntityTagRelation.Get"
	// IMV2BizEntityTagRelationUpdate selects the pinned native im.v2.bizEntityTagRelation.Update operation.
	IMV2BizEntityTagRelationUpdate Operation = "im.v2.bizEntityTagRelation.Update"
	// IMV2ChatSearch selects the pinned native im.v2.chat.Search operation.
	IMV2ChatSearch Operation = "im.v2.chat.Search"
	// IMV2ChatButtonUpdate selects the pinned native im.v2.chatButton.Update operation.
	IMV2ChatButtonUpdate Operation = "im.v2.chatButton.Update"
	// IMV2FeedCardBotTimeSentive selects the pinned native im.v2.feedCard.BotTimeSentive operation.
	IMV2FeedCardBotTimeSentive Operation = "im.v2.feedCard.BotTimeSentive"
	// IMV2FeedCardPatch selects the pinned native im.v2.feedCard.Patch operation.
	IMV2FeedCardPatch Operation = "im.v2.feedCard.Patch"
	// IMV2TagCreate selects the pinned native im.v2.tag.Create operation.
	IMV2TagCreate Operation = "im.v2.tag.Create"
	// IMV2TagPatch selects the pinned native im.v2.tag.Patch operation.
	IMV2TagPatch Operation = "im.v2.tag.Patch"
	// IMV2UrlPreviewBatchUpdate selects the pinned native im.v2.urlPreview.BatchUpdate operation.
	IMV2UrlPreviewBatchUpdate Operation = "im.v2.urlPreview.BatchUpdate"
	// CardKitV1CardBatchUpdate selects the pinned native cardkit.v1.card.BatchUpdate operation.
	CardKitV1CardBatchUpdate Operation = "cardkit.v1.card.BatchUpdate"
	// CardKitV1CardCreate selects the pinned native cardkit.v1.card.Create operation.
	CardKitV1CardCreate Operation = "cardkit.v1.card.Create"
	// CardKitV1CardIdConvert selects the pinned native cardkit.v1.card.IdConvert operation.
	CardKitV1CardIdConvert Operation = "cardkit.v1.card.IdConvert"
	// CardKitV1CardSettings selects the pinned native cardkit.v1.card.Settings operation.
	CardKitV1CardSettings Operation = "cardkit.v1.card.Settings"
	// CardKitV1CardUpdate selects the pinned native cardkit.v1.card.Update operation.
	CardKitV1CardUpdate Operation = "cardkit.v1.card.Update"
	// CardKitV1CardElementContent selects the pinned native cardkit.v1.cardElement.Content operation.
	CardKitV1CardElementContent Operation = "cardkit.v1.cardElement.Content"
	// CardKitV1CardElementCreate selects the pinned native cardkit.v1.cardElement.Create operation.
	CardKitV1CardElementCreate Operation = "cardkit.v1.cardElement.Create"
	// CardKitV1CardElementDelete selects the pinned native cardkit.v1.cardElement.Delete operation.
	CardKitV1CardElementDelete Operation = "cardkit.v1.cardElement.Delete"
	// CardKitV1CardElementPatch selects the pinned native cardkit.v1.cardElement.Patch operation.
	CardKitV1CardElementPatch Operation = "cardkit.v1.cardElement.Patch"
	// CardKitV1CardElementUpdate selects the pinned native cardkit.v1.cardElement.Update operation.
	CardKitV1CardElementUpdate Operation = "cardkit.v1.cardElement.Update"
	// ContactV3UserBatchGetId selects the pinned native contact.v3.user.BatchGetId operation.
	ContactV3UserBatchGetId Operation = "contact.v3.user.BatchGetId"
	// BotInfo discovers bot metadata without starting SDK Channel policy.
	BotInfo Operation = "bot.v3.info"
	// LegacyBatchSend submits an asynchronous batch through the documented v4
	// HTTP endpoint. Its batch ID is not a normal editable message ID.
	LegacyBatchSend Operation = "message.v4.batch_send"
)

// Request freezes native path, query and JSON body values. Use the operation's
// native API fields; omitted/null/empty/zero remain distinct. No owning SDK value
// is accepted. Platform permissions and semantic validation remain external.
type Request struct {
	private
	native native.Request
}

func NewRequest(operation Operation, path []string, query url.Values, body JSON) (Request, error) {
	value, err := native.NewRequest(native.Operation(operation), path, query, body.native)
	return Request{native: value}, translate(err, "request")
}
func (value Request) Operation() Operation { return Operation(value.native.Operation()) }

type RequestOptions struct {
	private
	RequestID string
	Headers   http.Header
}

func (client *Client) Do(ctx context.Context, input Request, options RequestOptions) (Result, error) {
	return client.dispatch(ctx, string(input.Operation()), func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.Do(work, id, input.native, native.RequestOptions{RequestID: options.RequestID, Headers: options.Headers})
	})
}

// AssetInput borrows Bytes or Reader (exactly one) through actual return. Reader
// must cooperate; timeout cannot forcibly stop arbitrary Go code.
type AssetInput struct {
	private
	Name, FileType, ImageType string
	Duration                  *int
	Bytes                     []byte
	Reader                    io.Reader
}

func (client *Client) UploadAsset(ctx context.Context, image bool, input AssetInput, options RequestOptions) (Result, error) {
	return client.dispatch(ctx, "upload", func(work context.Context, connection *native.Client, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return connection.UploadAsset(work, id, image, native.AssetInput{Name: input.Name, FileType: input.FileType, ImageType: input.ImageType, Duration: input.Duration, Bytes: input.Bytes, Reader: input.Reader},
			native.RequestOptions{RequestID: options.RequestID, Headers: options.Headers})
	})
}
