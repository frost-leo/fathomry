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

package nacos

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// token is an instance-local credential snapshot. refresh is the conservative
// local refresh point, not proof that the server still grants access.
type token struct {
	value   string
	refresh time.Time
}

// token returns the selected endpoint's cached token or performs one bounded
// password login. The network gate coalesces concurrent refreshes, while short
// cache access uses client.mu so another endpoint's login cannot hide revocation.
func (client *Client) token(ctx context.Context, index int) (string, error) {
	if client.settings.Username == "" {
		return "", nil
	}
	select {
	case client.authGate <- struct{}{}:
		defer func() { <-client.authGate }()
	case <-ctx.Done():
		return "", fail(ErrRead, "authenticate", ctx.Err(), context.Cause(ctx))
	}
	if ctx.Err() != nil {
		return "", fail(ErrRead, "authenticate", ctx.Err(), context.Cause(ctx))
	}
	client.mu.Lock()
	cached := client.tokens[index]
	client.mu.Unlock()
	if time.Now().Before(cached.refresh) {
		return cached.value, nil
	}
	values := url.Values{"username": {client.settings.Username}, "password": {client.settings.Password}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.settings.Servers[index].HTTPURL+"/v1/auth/users/login", strings.NewReader(values.Encode()))
	if err != nil {
		return "", fail(ErrInput, "authenticate", err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := client.http.Do(request)
	if err != nil {
		return "", fail(ErrUnavailable, "authenticate", err, ctx.Err(), context.Cause(ctx))
	}
	defer response.Body.Close()
	if response.StatusCode == 401 || response.StatusCode == 403 {
		return "", fail(ErrDenied, "authenticate", HTTPStatus(response.StatusCode))
	}
	if response.StatusCode != 200 {
		return "", fail(ErrUnavailable, "authenticate", HTTPStatus(response.StatusCode))
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 64<<10+1))
	if err != nil {
		return "", fail(ErrRead, "authenticate", err)
	}
	if len(raw) > 64<<10 {
		return "", fail(ErrLimit, "authenticate")
	}
	if err := validateJSON(raw); err != nil {
		return "", fail(ErrDecode, "authenticate", err)
	}
	var body struct {
		AccessToken string `json:"accessToken"`
		TokenTTL    int64  `json:"tokenTtl"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return "", fail(ErrDecode, "authenticate", err)
	}
	if body.AccessToken == "" || len(body.AccessToken) > 16<<10 || !identifier(body.AccessToken, 16<<10, false) || body.TokenTTL < 1 || body.TokenTTL > 7*24*60*60 {
		return "", fail(ErrDecode, "authenticate")
	}
	if ctx.Err() != nil {
		return "", fail(ErrRead, "authenticate", ctx.Err(), context.Cause(ctx))
	}
	lifetime := time.Duration(body.TokenTTL) * time.Second
	// Refresh before the advertised TTL expires; do not turn cache lifetime into
	// an authorization guarantee or start an unowned background refresh worker.
	client.mu.Lock()
	client.tokens[index] = token{strings.Clone(body.AccessToken), time.Now().Add(lifetime - lifetime/10)}
	client.mu.Unlock()
	return body.AccessToken, nil
}

// forgetToken clears only the credential used by the rejected request. A late
// denial for an older token must not invalidate a concurrently refreshed token.
func (client *Client) forgetToken(index int, rejected string) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.tokens[index].value == rejected {
		client.tokens[index] = token{}
	}
}
