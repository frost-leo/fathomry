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

// Code generated from the pinned v3.12.1 notification resource declarations.
// Routes are a closed technical catalog, not owning SDK clients.
package lark

const (
	// IMV1BatchMessageDelete selects im/v1 batchMessage.Delete.
	IMV1BatchMessageDelete Operation = "im.v1.batchMessage.Delete"
	// IMV1BatchMessageGetProgress selects im/v1 batchMessage.GetProgress.
	IMV1BatchMessageGetProgress Operation = "im.v1.batchMessage.GetProgress"
	// IMV1BatchMessageReadUser selects im/v1 batchMessage.ReadUser.
	IMV1BatchMessageReadUser Operation = "im.v1.batchMessage.ReadUser"
	// IMV1ChatCreate selects im/v1 chat.Create.
	IMV1ChatCreate Operation = "im.v1.chat.Create"
	// IMV1ChatDelete selects im/v1 chat.Delete.
	IMV1ChatDelete Operation = "im.v1.chat.Delete"
	// IMV1ChatGet selects im/v1 chat.Get.
	IMV1ChatGet Operation = "im.v1.chat.Get"
	// IMV1ChatLink selects im/v1 chat.Link.
	IMV1ChatLink Operation = "im.v1.chat.Link"
	// IMV1ChatList selects im/v1 chat.List.
	IMV1ChatList Operation = "im.v1.chat.List"
	// IMV1ChatSearch selects im/v1 chat.Search.
	IMV1ChatSearch Operation = "im.v1.chat.Search"
	// IMV1ChatUpdate selects im/v1 chat.Update.
	IMV1ChatUpdate Operation = "im.v1.chat.Update"
	// IMV1ChatAnnouncementGet selects im/v1 chatAnnouncement.Get.
	IMV1ChatAnnouncementGet Operation = "im.v1.chatAnnouncement.Get"
	// IMV1ChatAnnouncementPatch selects im/v1 chatAnnouncement.Patch.
	IMV1ChatAnnouncementPatch Operation = "im.v1.chatAnnouncement.Patch"
	// IMV1ChatManagersAddManagers selects im/v1 chatManagers.AddManagers.
	IMV1ChatManagersAddManagers Operation = "im.v1.chatManagers.AddManagers"
	// IMV1ChatManagersDeleteManagers selects im/v1 chatManagers.DeleteManagers.
	IMV1ChatManagersDeleteManagers Operation = "im.v1.chatManagers.DeleteManagers"
	// IMV1ChatMembersCreate selects im/v1 chatMembers.Create.
	IMV1ChatMembersCreate Operation = "im.v1.chatMembers.Create"
	// IMV1ChatMembersDelete selects im/v1 chatMembers.Delete.
	IMV1ChatMembersDelete Operation = "im.v1.chatMembers.Delete"
	// IMV1ChatMembersGet selects im/v1 chatMembers.Get.
	IMV1ChatMembersGet Operation = "im.v1.chatMembers.Get"
	// IMV1ChatMembersIsInChat selects im/v1 chatMembers.IsInChat.
	IMV1ChatMembersIsInChat Operation = "im.v1.chatMembers.IsInChat"
	// IMV1ChatMembersMeJoin selects im/v1 chatMembers.MeJoin.
	IMV1ChatMembersMeJoin Operation = "im.v1.chatMembers.MeJoin"
	// IMV1ChatMenuItemPatch selects im/v1 chatMenuItem.Patch.
	IMV1ChatMenuItemPatch Operation = "im.v1.chatMenuItem.Patch"
	// IMV1ChatMenuTreeCreate selects im/v1 chatMenuTree.Create.
	IMV1ChatMenuTreeCreate Operation = "im.v1.chatMenuTree.Create"
	// IMV1ChatMenuTreeDelete selects im/v1 chatMenuTree.Delete.
	IMV1ChatMenuTreeDelete Operation = "im.v1.chatMenuTree.Delete"
	// IMV1ChatMenuTreeGet selects im/v1 chatMenuTree.Get.
	IMV1ChatMenuTreeGet Operation = "im.v1.chatMenuTree.Get"
	// IMV1ChatMenuTreeSort selects im/v1 chatMenuTree.Sort.
	IMV1ChatMenuTreeSort Operation = "im.v1.chatMenuTree.Sort"
	// IMV1ChatModerationGet selects im/v1 chatModeration.Get.
	IMV1ChatModerationGet Operation = "im.v1.chatModeration.Get"
	// IMV1ChatModerationUpdate selects im/v1 chatModeration.Update.
	IMV1ChatModerationUpdate Operation = "im.v1.chatModeration.Update"
	// IMV1ChatTabCreate selects im/v1 chatTab.Create.
	IMV1ChatTabCreate Operation = "im.v1.chatTab.Create"
	// IMV1ChatTabDeleteTabs selects im/v1 chatTab.DeleteTabs.
	IMV1ChatTabDeleteTabs Operation = "im.v1.chatTab.DeleteTabs"
	// IMV1ChatTabListTabs selects im/v1 chatTab.ListTabs.
	IMV1ChatTabListTabs Operation = "im.v1.chatTab.ListTabs"
	// IMV1ChatTabSortTabs selects im/v1 chatTab.SortTabs.
	IMV1ChatTabSortTabs Operation = "im.v1.chatTab.SortTabs"
	// IMV1ChatTabUpdateTabs selects im/v1 chatTab.UpdateTabs.
	IMV1ChatTabUpdateTabs Operation = "im.v1.chatTab.UpdateTabs"
	// IMV1ChatTopNoticeDeleteTopNotice selects im/v1 chatTopNotice.DeleteTopNotice.
	IMV1ChatTopNoticeDeleteTopNotice Operation = "im.v1.chatTopNotice.DeleteTopNotice"
	// IMV1ChatTopNoticePutTopNotice selects im/v1 chatTopNotice.PutTopNotice.
	IMV1ChatTopNoticePutTopNotice Operation = "im.v1.chatTopNotice.PutTopNotice"
	// IMV1FileCreate selects im/v1 file.Create.
	IMV1FileCreate Operation = "im.v1.file.Create"
	// IMV1FileGet selects im/v1 file.Get.
	IMV1FileGet Operation = "im.v1.file.Get"
	// IMV1ImageCreate selects im/v1 image.Create.
	IMV1ImageCreate Operation = "im.v1.image.Create"
	// IMV1ImageGet selects im/v1 image.Get.
	IMV1ImageGet Operation = "im.v1.image.Get"
	// IMV1MessageCreate selects im/v1 message.Create.
	IMV1MessageCreate Operation = "im.v1.message.Create"
	// IMV1MessageDelete selects im/v1 message.Delete.
	IMV1MessageDelete Operation = "im.v1.message.Delete"
	// IMV1MessageForward selects im/v1 message.Forward.
	IMV1MessageForward Operation = "im.v1.message.Forward"
	// IMV1MessageGet selects im/v1 message.Get.
	IMV1MessageGet Operation = "im.v1.message.Get"
	// IMV1MessageList selects im/v1 message.List.
	IMV1MessageList Operation = "im.v1.message.List"
	// IMV1MessageMergeForward selects im/v1 message.MergeForward.
	IMV1MessageMergeForward Operation = "im.v1.message.MergeForward"
	// IMV1MessagePatch selects im/v1 message.Patch.
	IMV1MessagePatch Operation = "im.v1.message.Patch"
	// IMV1MessagePushFollowUp selects im/v1 message.PushFollowUp.
	IMV1MessagePushFollowUp Operation = "im.v1.message.PushFollowUp"
	// IMV1MessageReadUsers selects im/v1 message.ReadUsers.
	IMV1MessageReadUsers Operation = "im.v1.message.ReadUsers"
	// IMV1MessageReply selects im/v1 message.Reply.
	IMV1MessageReply Operation = "im.v1.message.Reply"
	// IMV1MessageSearch selects im/v1 message.Search.
	IMV1MessageSearch Operation = "im.v1.message.Search"
	// IMV1MessageUpdate selects im/v1 message.Update.
	IMV1MessageUpdate Operation = "im.v1.message.Update"
	// IMV1MessageUrgentApp selects im/v1 message.UrgentApp.
	IMV1MessageUrgentApp Operation = "im.v1.message.UrgentApp"
	// IMV1MessageUrgentPhone selects im/v1 message.UrgentPhone.
	IMV1MessageUrgentPhone Operation = "im.v1.message.UrgentPhone"
	// IMV1MessageUrgentSms selects im/v1 message.UrgentSms.
	IMV1MessageUrgentSms Operation = "im.v1.message.UrgentSms"
	// IMV1MessageReactionBatchQuery selects im/v1 messageReaction.BatchQuery.
	IMV1MessageReactionBatchQuery Operation = "im.v1.messageReaction.BatchQuery"
	// IMV1MessageReactionCreate selects im/v1 messageReaction.Create.
	IMV1MessageReactionCreate Operation = "im.v1.messageReaction.Create"
	// IMV1MessageReactionDelete selects im/v1 messageReaction.Delete.
	IMV1MessageReactionDelete Operation = "im.v1.messageReaction.Delete"
	// IMV1MessageReactionList selects im/v1 messageReaction.List.
	IMV1MessageReactionList Operation = "im.v1.messageReaction.List"
	// IMV1MessageResourceGet selects im/v1 messageResource.Get.
	IMV1MessageResourceGet Operation = "im.v1.messageResource.Get"
	// IMV1PinCreate selects im/v1 pin.Create.
	IMV1PinCreate Operation = "im.v1.pin.Create"
	// IMV1PinDelete selects im/v1 pin.Delete.
	IMV1PinDelete Operation = "im.v1.pin.Delete"
	// IMV1PinList selects im/v1 pin.List.
	IMV1PinList Operation = "im.v1.pin.List"
	// IMV1ThreadForward selects im/v1 thread.Forward.
	IMV1ThreadForward Operation = "im.v1.thread.Forward"
	// IMV2AppFeedCardCreate selects im/v2 appFeedCard.Create.
	IMV2AppFeedCardCreate Operation = "im.v2.appFeedCard.Create"
	// IMV2AppFeedCardBatchDelete selects im/v2 appFeedCardBatch.Delete.
	IMV2AppFeedCardBatchDelete Operation = "im.v2.appFeedCardBatch.Delete"
	// IMV2AppFeedCardBatchUpdate selects im/v2 appFeedCardBatch.Update.
	IMV2AppFeedCardBatchUpdate Operation = "im.v2.appFeedCardBatch.Update"
	// IMV2BizEntityTagRelationCreate selects im/v2 bizEntityTagRelation.Create.
	IMV2BizEntityTagRelationCreate Operation = "im.v2.bizEntityTagRelation.Create"
	// IMV2BizEntityTagRelationGet selects im/v2 bizEntityTagRelation.Get.
	IMV2BizEntityTagRelationGet Operation = "im.v2.bizEntityTagRelation.Get"
	// IMV2BizEntityTagRelationUpdate selects im/v2 bizEntityTagRelation.Update.
	IMV2BizEntityTagRelationUpdate Operation = "im.v2.bizEntityTagRelation.Update"
	// IMV2ChatSearch selects im/v2 chat.Search.
	IMV2ChatSearch Operation = "im.v2.chat.Search"
	// IMV2ChatButtonUpdate selects im/v2 chatButton.Update.
	IMV2ChatButtonUpdate Operation = "im.v2.chatButton.Update"
	// IMV2FeedCardBotTimeSentive selects im/v2 feedCard.BotTimeSentive.
	IMV2FeedCardBotTimeSentive Operation = "im.v2.feedCard.BotTimeSentive"
	// IMV2FeedCardPatch selects im/v2 feedCard.Patch.
	IMV2FeedCardPatch Operation = "im.v2.feedCard.Patch"
	// IMV2TagCreate selects im/v2 tag.Create.
	IMV2TagCreate Operation = "im.v2.tag.Create"
	// IMV2TagPatch selects im/v2 tag.Patch.
	IMV2TagPatch Operation = "im.v2.tag.Patch"
	// IMV2UrlPreviewBatchUpdate selects im/v2 urlPreview.BatchUpdate.
	IMV2UrlPreviewBatchUpdate Operation = "im.v2.urlPreview.BatchUpdate"
	// CardKitV1CardBatchUpdate selects cardkit/v1 card.BatchUpdate.
	CardKitV1CardBatchUpdate Operation = "cardkit.v1.card.BatchUpdate"
	// CardKitV1CardCreate selects cardkit/v1 card.Create.
	CardKitV1CardCreate Operation = "cardkit.v1.card.Create"
	// CardKitV1CardIdConvert selects cardkit/v1 card.IdConvert.
	CardKitV1CardIdConvert Operation = "cardkit.v1.card.IdConvert"
	// CardKitV1CardSettings selects cardkit/v1 card.Settings.
	CardKitV1CardSettings Operation = "cardkit.v1.card.Settings"
	// CardKitV1CardUpdate selects cardkit/v1 card.Update.
	CardKitV1CardUpdate Operation = "cardkit.v1.card.Update"
	// CardKitV1CardElementContent selects cardkit/v1 cardElement.Content.
	CardKitV1CardElementContent Operation = "cardkit.v1.cardElement.Content"
	// CardKitV1CardElementCreate selects cardkit/v1 cardElement.Create.
	CardKitV1CardElementCreate Operation = "cardkit.v1.cardElement.Create"
	// CardKitV1CardElementDelete selects cardkit/v1 cardElement.Delete.
	CardKitV1CardElementDelete Operation = "cardkit.v1.cardElement.Delete"
	// CardKitV1CardElementPatch selects cardkit/v1 cardElement.Patch.
	CardKitV1CardElementPatch Operation = "cardkit.v1.cardElement.Patch"
	// CardKitV1CardElementUpdate selects cardkit/v1 cardElement.Update.
	CardKitV1CardElementUpdate Operation = "cardkit.v1.cardElement.Update"
	// ContactV3UserBatchGetId selects contact/v3 user.BatchGetId.
	ContactV3UserBatchGetId Operation = "contact.v3.user.BatchGetId"
	// BotInfo is the SDK Channel bot identity discovery endpoint, without Channel policy.
	BotInfo Operation = "bot.v3.info"
	// LegacyBatchSend is documented HTTP submission, not generated BatchMessage.Create.
	LegacyBatchSend Operation = "message.v4.batch_send"
)

var operations = map[Operation]operationSpec{
	LegacyBatchSend:                  {method: "POST", path: "/open-apis/message/v4/batch_send/", tenant: true, data: true},
	IMV1BatchMessageDelete:           {method: "DELETE", path: "/open-apis/im/v1/batch_messages/:batch_message_id", query: []string{}, user: false, tenant: true, data: false, download: false, upload: false},
	IMV1BatchMessageGetProgress:      {method: "GET", path: "/open-apis/im/v1/batch_messages/:batch_message_id/get_progress", query: []string{}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV1BatchMessageReadUser:         {method: "GET", path: "/open-apis/im/v1/batch_messages/:batch_message_id/read_user", query: []string{}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV1ChatCreate:                   {method: "POST", path: "/open-apis/im/v1/chats", query: []string{"user_id_type", "set_bot_manager", "uuid"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ChatDelete:                   {method: "DELETE", path: "/open-apis/im/v1/chats/:chat_id", query: []string{}, user: true, tenant: true, data: false, download: false, upload: false},
	IMV1ChatGet:                      {method: "GET", path: "/open-apis/im/v1/chats/:chat_id", query: []string{"user_id_type"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ChatLink:                     {method: "POST", path: "/open-apis/im/v1/chats/:chat_id/link", query: []string{}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ChatList:                     {method: "GET", path: "/open-apis/im/v1/chats", query: []string{"user_id_type", "sort_type", "page_token", "page_size", "types"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ChatSearch:                   {method: "GET", path: "/open-apis/im/v1/chats/search", query: []string{"user_id_type", "query", "page_token", "page_size"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ChatUpdate:                   {method: "PUT", path: "/open-apis/im/v1/chats/:chat_id", query: []string{"user_id_type"}, user: true, tenant: true, data: false, download: false, upload: false},
	IMV1ChatAnnouncementGet:          {method: "GET", path: "/open-apis/im/v1/chats/:chat_id/announcement", query: []string{"user_id_type"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ChatAnnouncementPatch:        {method: "PATCH", path: "/open-apis/im/v1/chats/:chat_id/announcement", query: []string{}, user: true, tenant: true, data: false, download: false, upload: false},
	IMV1ChatManagersAddManagers:      {method: "POST", path: "/open-apis/im/v1/chats/:chat_id/managers/add_managers", query: []string{"member_id_type"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ChatManagersDeleteManagers:   {method: "POST", path: "/open-apis/im/v1/chats/:chat_id/managers/delete_managers", query: []string{"member_id_type"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ChatMembersCreate:            {method: "POST", path: "/open-apis/im/v1/chats/:chat_id/members", query: []string{"member_id_type", "succeed_type"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ChatMembersDelete:            {method: "DELETE", path: "/open-apis/im/v1/chats/:chat_id/members", query: []string{"member_id_type"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ChatMembersGet:               {method: "GET", path: "/open-apis/im/v1/chats/:chat_id/members", query: []string{"member_id_type", "page_size", "page_token", "check_security_conf"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ChatMembersIsInChat:          {method: "GET", path: "/open-apis/im/v1/chats/:chat_id/members/is_in_chat", query: []string{}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ChatMembersMeJoin:            {method: "PATCH", path: "/open-apis/im/v1/chats/:chat_id/members/me_join", query: []string{}, user: true, tenant: true, data: false, download: false, upload: false},
	IMV1ChatMenuItemPatch:            {method: "PATCH", path: "/open-apis/im/v1/chats/:chat_id/menu_items/:menu_item_id", query: []string{}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV1ChatMenuTreeCreate:           {method: "POST", path: "/open-apis/im/v1/chats/:chat_id/menu_tree", query: []string{}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV1ChatMenuTreeDelete:           {method: "DELETE", path: "/open-apis/im/v1/chats/:chat_id/menu_tree", query: []string{}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV1ChatMenuTreeGet:              {method: "GET", path: "/open-apis/im/v1/chats/:chat_id/menu_tree", query: []string{}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV1ChatMenuTreeSort:             {method: "POST", path: "/open-apis/im/v1/chats/:chat_id/menu_tree/sort", query: []string{}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV1ChatModerationGet:            {method: "GET", path: "/open-apis/im/v1/chats/:chat_id/moderation", query: []string{"user_id_type", "page_size", "page_token"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ChatModerationUpdate:         {method: "PUT", path: "/open-apis/im/v1/chats/:chat_id/moderation", query: []string{"user_id_type"}, user: true, tenant: true, data: false, download: false, upload: false},
	IMV1ChatTabCreate:                {method: "POST", path: "/open-apis/im/v1/chats/:chat_id/chat_tabs", query: []string{}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ChatTabDeleteTabs:            {method: "DELETE", path: "/open-apis/im/v1/chats/:chat_id/chat_tabs/delete_tabs", query: []string{}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ChatTabListTabs:              {method: "GET", path: "/open-apis/im/v1/chats/:chat_id/chat_tabs/list_tabs", query: []string{}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ChatTabSortTabs:              {method: "POST", path: "/open-apis/im/v1/chats/:chat_id/chat_tabs/sort_tabs", query: []string{}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ChatTabUpdateTabs:            {method: "POST", path: "/open-apis/im/v1/chats/:chat_id/chat_tabs/update_tabs", query: []string{}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ChatTopNoticeDeleteTopNotice: {method: "POST", path: "/open-apis/im/v1/chats/:chat_id/top_notice/delete_top_notice", query: []string{}, user: true, tenant: true, data: false, download: false, upload: false},
	IMV1ChatTopNoticePutTopNotice:    {method: "POST", path: "/open-apis/im/v1/chats/:chat_id/top_notice/put_top_notice", query: []string{}, user: true, tenant: true, data: false, download: false, upload: false},
	IMV1FileCreate:                   {method: "POST", path: "/open-apis/im/v1/files", query: []string{}, user: true, tenant: true, data: true, download: false, upload: true},
	IMV1FileGet:                      {method: "GET", path: "/open-apis/im/v1/files/:file_key", query: []string{}, user: false, tenant: true, data: false, download: true, upload: false},
	IMV1ImageCreate:                  {method: "POST", path: "/open-apis/im/v1/images", query: []string{}, user: true, tenant: true, data: true, download: false, upload: true},
	IMV1ImageGet:                     {method: "GET", path: "/open-apis/im/v1/images/:image_key", query: []string{}, user: false, tenant: true, data: false, download: true, upload: false},
	IMV1MessageCreate:                {method: "POST", path: "/open-apis/im/v1/messages", query: []string{"receive_id_type"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1MessageDelete:                {method: "DELETE", path: "/open-apis/im/v1/messages/:message_id", query: []string{}, user: true, tenant: true, data: false, download: false, upload: false},
	IMV1MessageForward:               {method: "POST", path: "/open-apis/im/v1/messages/:message_id/forward", query: []string{"receive_id_type", "uuid"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1MessageGet:                   {method: "GET", path: "/open-apis/im/v1/messages/:message_id", query: []string{"user_id_type", "card_msg_content_type", "with_sender_name"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1MessageList:                  {method: "GET", path: "/open-apis/im/v1/messages", query: []string{"container_id_type", "container_id", "start_time", "end_time", "sort_type", "page_size", "page_token", "card_msg_content_type", "only_thread_root_messages", "with_sender_name"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1MessageMergeForward:          {method: "POST", path: "/open-apis/im/v1/messages/merge_forward", query: []string{"receive_id_type", "uuid"}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV1MessagePatch:                 {method: "PATCH", path: "/open-apis/im/v1/messages/:message_id", query: []string{}, user: true, tenant: true, data: false, download: false, upload: false},
	IMV1MessagePushFollowUp:          {method: "POST", path: "/open-apis/im/v1/messages/:message_id/push_follow_up", query: []string{}, user: false, tenant: true, data: false, download: false, upload: false},
	IMV1MessageReadUsers:             {method: "GET", path: "/open-apis/im/v1/messages/:message_id/read_users", query: []string{"user_id_type", "page_size", "page_token"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1MessageReply:                 {method: "POST", path: "/open-apis/im/v1/messages/:message_id/reply", query: []string{}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1MessageSearch:                {method: "POST", path: "/open-apis/im/v1/messages/search", query: []string{"page_size", "page_token", "user_id_type"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1MessageUpdate:                {method: "PUT", path: "/open-apis/im/v1/messages/:message_id", query: []string{}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV1MessageUrgentApp:             {method: "PATCH", path: "/open-apis/im/v1/messages/:message_id/urgent_app", query: []string{"user_id_type"}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV1MessageUrgentPhone:           {method: "PATCH", path: "/open-apis/im/v1/messages/:message_id/urgent_phone", query: []string{"user_id_type"}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV1MessageUrgentSms:             {method: "PATCH", path: "/open-apis/im/v1/messages/:message_id/urgent_sms", query: []string{"user_id_type"}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV1MessageReactionBatchQuery:    {method: "POST", path: "/open-apis/im/v1/messages/reactions/batch_query", query: []string{"user_id_type"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1MessageReactionCreate:        {method: "POST", path: "/open-apis/im/v1/messages/:message_id/reactions", query: []string{}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1MessageReactionDelete:        {method: "DELETE", path: "/open-apis/im/v1/messages/:message_id/reactions/:reaction_id", query: []string{}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1MessageReactionList:          {method: "GET", path: "/open-apis/im/v1/messages/:message_id/reactions", query: []string{"reaction_type", "page_token", "page_size", "user_id_type"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1MessageResourceGet:           {method: "GET", path: "/open-apis/im/v1/messages/:message_id/resources/:file_key", query: []string{"type"}, user: true, tenant: true, data: false, download: true, upload: false},
	IMV1PinCreate:                    {method: "POST", path: "/open-apis/im/v1/pins", query: []string{}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1PinDelete:                    {method: "DELETE", path: "/open-apis/im/v1/pins/:message_id", query: []string{}, user: true, tenant: true, data: false, download: false, upload: false},
	IMV1PinList:                      {method: "GET", path: "/open-apis/im/v1/pins", query: []string{"chat_id", "start_time", "end_time", "page_size", "page_token"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV1ThreadForward:                {method: "POST", path: "/open-apis/im/v1/threads/:thread_id/forward", query: []string{"receive_id_type", "uuid"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV2AppFeedCardCreate:            {method: "POST", path: "/open-apis/im/v2/app_feed_card", query: []string{"user_id_type"}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV2AppFeedCardBatchDelete:       {method: "DELETE", path: "/open-apis/im/v2/app_feed_card/batch", query: []string{"user_id_type"}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV2AppFeedCardBatchUpdate:       {method: "PUT", path: "/open-apis/im/v2/app_feed_card/batch", query: []string{"user_id_type"}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV2BizEntityTagRelationCreate:   {method: "POST", path: "/open-apis/im/v2/biz_entity_tag_relation", query: []string{}, user: false, tenant: true, data: false, download: false, upload: false},
	IMV2BizEntityTagRelationGet:      {method: "GET", path: "/open-apis/im/v2/biz_entity_tag_relation", query: []string{"tag_biz_type", "biz_entity_id", "bot_id"}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV2BizEntityTagRelationUpdate:   {method: "PUT", path: "/open-apis/im/v2/biz_entity_tag_relation", query: []string{}, user: false, tenant: true, data: false, download: false, upload: false},
	IMV2ChatSearch:                   {method: "POST", path: "/open-apis/im/v2/chats/search", query: []string{"page_size", "page_token", "user_id_type"}, user: true, tenant: true, data: true, download: false, upload: false},
	IMV2ChatButtonUpdate:             {method: "PUT", path: "/open-apis/im/v2/chat_button", query: []string{"user_id_type"}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV2FeedCardBotTimeSentive:       {method: "PATCH", path: "/open-apis/im/v2/feed_cards/bot_time_sentive", query: []string{"user_id_type"}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV2FeedCardPatch:                {method: "PATCH", path: "/open-apis/im/v2/feed_cards/:feed_card_id", query: []string{"user_id_type"}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV2TagCreate:                    {method: "POST", path: "/open-apis/im/v2/tags", query: []string{}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV2TagPatch:                     {method: "PATCH", path: "/open-apis/im/v2/tags/:tag_id", query: []string{}, user: false, tenant: true, data: true, download: false, upload: false},
	IMV2UrlPreviewBatchUpdate:        {method: "POST", path: "/open-apis/im/v2/url_previews/batch_update", query: []string{}, user: false, tenant: true, data: false, download: false, upload: false},
	CardKitV1CardBatchUpdate:         {method: "POST", path: "/open-apis/cardkit/v1/cards/:card_id/batch_update", query: []string{}, user: false, tenant: true, data: false, download: false, upload: false},
	CardKitV1CardCreate:              {method: "POST", path: "/open-apis/cardkit/v1/cards", query: []string{}, user: false, tenant: true, data: true, download: false, upload: false},
	CardKitV1CardIdConvert:           {method: "POST", path: "/open-apis/cardkit/v1/cards/id_convert", query: []string{}, user: false, tenant: true, data: true, download: false, upload: false},
	CardKitV1CardSettings:            {method: "PATCH", path: "/open-apis/cardkit/v1/cards/:card_id/settings", query: []string{}, user: false, tenant: true, data: false, download: false, upload: false},
	CardKitV1CardUpdate:              {method: "PUT", path: "/open-apis/cardkit/v1/cards/:card_id", query: []string{}, user: false, tenant: true, data: false, download: false, upload: false},
	CardKitV1CardElementContent:      {method: "PUT", path: "/open-apis/cardkit/v1/cards/:card_id/elements/:element_id/content", query: []string{}, user: false, tenant: true, data: false, download: false, upload: false},
	CardKitV1CardElementCreate:       {method: "POST", path: "/open-apis/cardkit/v1/cards/:card_id/elements", query: []string{}, user: false, tenant: true, data: false, download: false, upload: false},
	CardKitV1CardElementDelete:       {method: "DELETE", path: "/open-apis/cardkit/v1/cards/:card_id/elements/:element_id", query: []string{}, user: false, tenant: true, data: false, download: false, upload: false},
	CardKitV1CardElementPatch:        {method: "PATCH", path: "/open-apis/cardkit/v1/cards/:card_id/elements/:element_id", query: []string{}, user: false, tenant: true, data: false, download: false, upload: false},
	CardKitV1CardElementUpdate:       {method: "PUT", path: "/open-apis/cardkit/v1/cards/:card_id/elements/:element_id", query: []string{}, user: false, tenant: true, data: false, download: false, upload: false},
	ContactV3UserBatchGetId:          {method: "POST", path: "/open-apis/contact/v3/users/batch_get_id", query: []string{"user_id_type"}, user: false, tenant: true, data: true, download: false, upload: false},
	BotInfo:                          {method: "GET", path: "/open-apis/bot/v3/info", tenant: true, data: true},
}
