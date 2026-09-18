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
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkcontact "github.com/larksuite/oapi-sdk-go/v3/service/contact/v3"
)

// ResolveEmail explicitly resolves exactly one authorized email to an app-scoped
// open_id. It performs no directory enumeration, resigned-user search, fallback
// or sending. Resolution permissions are separate from message permissions.
func (client *Client) ResolveEmail(ctx context.Context, id fault.Correlation, email string) (*invocation.Receipt[Result], error) {
	if !(Recipient{Type: "email", ID: email}).valid() {
		return nil, failure(ErrInput, "resolve-email")
	}
	return client.execute(ctx, id, requestSpec{operation: "resolve-email", method: "POST", path: "/open-apis/contact/v3/users/batch_get_id",
		query: larkcore.QueryParams{"user_id_type": {"open_id"}}, body: &larkcontact.BatchGetIdUserReqBody{Emails: []string{email}, IncludeResigned: pointer(false)},
		target: email, ackKey: "email-recipient"})
}

// ResolvedRecipient is present only for an unambiguous same-email lookup result.
// Absence is not permission to pick another address, account or directory entry.
func (value Result) ResolvedRecipient() (Recipient, bool) {
	if value.resolvedRecipient.ID == "" {
		return Recipient{}, false
	}
	return value.resolvedRecipient, true
}
func (value *Result) readRecipient() error {
	fields, err := exactFields([]byte(value.data), "user_list")
	if err != nil {
		return failure(ErrResponse, "recipient", err)
	}
	var users []json.RawMessage
	if raw := fields["user_list"]; len(raw) == 0 || raw[0] != '[' || json.Unmarshal(raw, &users) != nil || len(users) > 1 {
		return failure(ErrResponse, "recipient")
	}
	if len(users) == 0 {
		return nil
	}
	if _, err := exactFields(users[0], "user_id", "email"); err != nil {
		return failure(ErrResponse, "recipient", err)
	}
	var user struct {
		ID    string `json:"user_id"`
		Email string `json:"email"`
	}
	if json.Unmarshal(users[0], &user) != nil {
		return failure(ErrResponse, "recipient")
	}
	if user.Email != "" && user.Email != value.target {
		return failure(ErrResponse, "recipient-identity")
	}
	if user.ID != "" {
		if !identifier(user.ID) || user.Email != value.target {
			return failure(ErrResponse, "recipient-identity")
		}
		value.resolvedRecipient = Recipient{Type: "open_id", ID: user.ID}
	}
	return nil
}
