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
	"slices"
)

// InvalidTargets preserves each legacy batch namespace, including absent/null/[].
func (value Result) InvalidTargets() map[string][]string {
	if value.invalidTargets == nil {
		return nil
	}
	result := make(map[string][]string, len(value.invalidTargets))
	for key, ids := range value.invalidTargets {
		result[key] = slices.Clone(ids)
	}
	return result
}
func (value *Result) readBatchPartial() error {
	names := []string{"invalid_department_ids", "invalid_open_ids", "invalid_user_ids", "invalid_union_ids"}
	fields, err := exactFields([]byte(value.data), names...)
	if err != nil {
		return failure(ErrResponse, "batch-partial", err)
	}
	value.invalidTargets = map[string][]string{}
	for _, name := range names {
		if raw, present := fields[name]; present {
			var ids []string
			if json.Unmarshal(raw, &ids) != nil {
				return failure(ErrResponse, "batch-partial")
			}
			value.invalidTargets[name] = ids
		}
	}
	return nil
}
func validateLegacyBatch(body JSON) error {
	fields, err := exactFields(body.Bytes(), "msg_type", "content", "card", "department_ids", "open_ids", "user_ids", "union_ids")
	if err != nil {
		return err
	}
	var kind string
	if json.Unmarshal(fields["msg_type"], &kind) != nil {
		return failure(ErrInput, "batch-kind")
	}
	key, limit := "content", 30<<10
	switch kind {
	case "text":
		limit = 150 << 10
	case "post", "image", "share_chat":
	case "interactive":
		key = "card"
	default:
		return failure(ErrInput, "batch-kind")
	}
	if len(body.value) > limit {
		return failure(ErrLimit, "batch-body")
	}
	if _, err := exactFields(fields[key]); err != nil {
		return failure(ErrInput, "batch-content", err)
	}
	count := 0
	for _, name := range []string{"department_ids", "open_ids", "user_ids", "union_ids"} {
		if raw, present := fields[name]; present {
			var ids []string
			if json.Unmarshal(raw, &ids) != nil || len(ids) > 200 {
				return failure(ErrInput, "batch-targets")
			}
			for _, id := range ids {
				if !identifier(id) || name == "department_ids" && id == "0" {
					return failure(ErrInput, "batch-target")
				}
			}
			count += len(ids)
		}
	}
	if count == 0 {
		return failure(ErrInput, "batch-targets")
	}
	return nil
}
