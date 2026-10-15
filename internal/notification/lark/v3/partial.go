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
	"encoding/json"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkimv2 "github.com/larksuite/oapi-sdk-go/v3/service/im/v2"
)

// These native fields report distinct target scopes. Pending membership is not
// rejection; neither is interchangeable with failed cards or reaction queries.
var responseScopes = map[Operation][]string{
	IMV1ChatMembersCreate:         {"invalid_id_list", "not_existed_id_list", "pending_approval_id_list"},
	IMV1ChatMembersDelete:         {"invalid_id_list"},
	IMV1MessageReactionBatchQuery: {"fail_msg_reaction_details"},
	IMV2AppFeedCardCreate:         {"failed_cards"},
	IMV2AppFeedCardBatchDelete:    {"failed_cards"},
	IMV2AppFeedCardBatchUpdate:    {"failed_cards"},
	IMV2ChatButtonUpdate:          {"failed_user_reasons"},
	IMV2FeedCardBotTimeSentive:    {"failed_user_reasons"},
	IMV2FeedCardPatch:             {"failed_user_reasons"},
	IMV2TagCreate:                 {"create_tag_fail_reason"},
	IMV2TagPatch:                  {"patch_tag_fail_reason"},
}

// PartialFields returns detached exact native failure/pending fields. Map
// membership distinguishes absent fields; null, [] and {} retain their spelling.
func (value Result) PartialFields() map[string][]byte {
	if value.scopeDetails == nil {
		return nil
	}
	result := make(map[string][]byte, len(value.scopeDetails))
	for key, raw := range value.scopeDetails {
		result[key] = []byte(raw)
	}
	return result
}
func (value *Result) readScopes(names []string) error {
	if len(names) == 0 {
		return nil
	}
	fields, err := exactFields([]byte(value.data), names...)
	if err != nil {
		return failure(ErrResponse, "partial-scope", err)
	}
	value.scopeDetails = map[string]string{}
	for _, name := range names {
		raw, present := fields[name]
		if !present {
			continue
		}
		value.scopeDetails[name] = string(raw)
		if string(raw) == "null" {
			continue
		}
		if err := validateScope(name, raw); err != nil {
			return failure(ErrResponse, "partial-scope", err)
		}
		if name == "create_tag_fail_reason" || name == "patch_tag_fail_reason" {
			if _, err := exactFields(raw, "duplicate_id"); err != nil {
				return failure(ErrResponse, "tag-failure", err)
			}
			value.partialScope = true
		} else {
			var items []json.RawMessage
			if json.Unmarshal(raw, &items) != nil {
				return failure(ErrResponse, "partial-scope")
			}
			value.partialScope = value.partialScope || len(items) > 0
		}
	}
	return nil
}

func validateScope(name string, raw []byte) error {
	var target any
	switch name {
	case "invalid_id_list", "not_existed_id_list", "pending_approval_id_list":
		target = new([]string)
	case "fail_msg_reaction_details":
		target = new([]*larkim.FailMsgReactionDetails)
	case "failed_cards":
		target = new([]*larkimv2.OpenFailedUserAppFeedCardItem)
	case "failed_user_reasons":
		target = new([]*larkimv2.FailedReason)
	case "create_tag_fail_reason":
		target = new(larkimv2.CreateTagFailReason)
	case "patch_tag_fail_reason":
		target = new(larkimv2.PatchTagFailReason)
	default:
		return failure(ErrResponse, "partial-schema")
	}
	return json.Unmarshal(raw, target)
}
