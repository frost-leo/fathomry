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
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

func (owned *owner) accessToken(ctx context.Context, call *invocation.Call[Result], result *Result) (string, error, error) {
	value := owned.settings
	if value.Profile == "tenant-token" {
		if !time.Now().Before(time.Unix(value.TokenExpiresUnix, 0)) {
			return "", failure(ErrAuth, "expired-token"), nil
		}
		return value.TenantToken, nil, nil
	}
	select {
	case owned.tokenLock <- struct{}{}:
	case <-ctx.Done():
		return "", failure(ErrAuth, "token-wait", ctx.Err(), context.Cause(ctx)), nil
	}
	defer func() { <-owned.tokenLock }()
	if owned.token != "" && time.Now().Before(owned.expires) {
		return owned.token, nil, nil
	}
	started := time.Now()
	state, requestErr := owned.nativeRequest(ctx, call, requestSpec{operation: "authenticate", method: "POST",
		path: larkcore.TenantAccessTokenInternalUrlPath, body: &larkcore.SelfBuiltAppAccessTokenReq{AppID: value.AppID, AppSecret: value.AppSecret}}, "")
	result.authAttempted = state.entered
	result.authentication = exchange(state)
	if requestErr != nil {
		return "", failure(ErrAuth, "authenticate", requestErr), state.cleanup
	}
	observed, _, err := decodeResponse(state, false)
	result.authentication = observed
	if err != nil {
		return "", failure(ErrAuth, "authenticate", err), state.cleanup
	}
	// The self-built token endpoint returns top-level fields; generated auth/v3
	// models instead place these under data. Use the SDK core's actual token DTO.
	if _, err := exactFields(state.response.RawBody, "expire", "tenant_access_token"); err != nil {
		return "", failure(ErrAuth, "token-response", err), state.cleanup
	}
	var response larkcore.TenantAccessTokenResp
	if json.Unmarshal(state.response.RawBody, &response) != nil || !secretValid(response.TenantAccessToken, 4096) || response.Expire < 1 || response.Expire > 7200 {
		return "", failure(ErrAuth, "token-response"), state.cleanup
	}
	ttl := time.Duration(response.Expire) * time.Second
	skew := min(30*time.Second, ttl/10)
	owned.token = response.TenantAccessToken
	owned.expires = started.Add(ttl - skew)
	if !time.Now().Before(owned.expires) {
		owned.token = ""
		return "", failure(ErrAuth, "expired-token"), state.cleanup
	}
	return owned.token, nil, state.cleanup
}
func (owned *owner) invalidateToken(ctx context.Context, token string) {
	if owned.settings.Profile != "application" {
		return
	}
	select {
	case owned.tokenLock <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-owned.tokenLock }()
	if owned.token == token {
		owned.token = ""
		owned.expires = time.Time{}
	}
}
