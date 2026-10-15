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
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

func (stage AuthenticationStage) Effect() Effect { return stage.effect }

// AuthenticationStage retains every auxiliary exchange independently of the
// requested operation. Err and Cleanup preserve private native causes.
type AuthenticationStage struct {
	private
	name             string
	attempted        bool
	exchange         Exchange
	primary, cleanup error
	effect           Effect
}

func (stage AuthenticationStage) Name() string       { return stage.name }
func (stage AuthenticationStage) Attempted() bool    { return stage.attempted }
func (stage AuthenticationStage) Exchange() Exchange { return stage.exchange }
func (stage AuthenticationStage) Err() error         { return stage.primary }
func (stage AuthenticationStage) Cleanup() error     { return stage.cleanup }
func (value Result) AuthenticationStages() []AuthenticationStage {
	return append([]AuthenticationStage(nil), value.authStages...)
}

func (owned *owner) credentials(ctx context.Context, request CredentialRequest) (value Credentials, err error) {
	configured := owned.settings
	value = Credentials{AppID: configured.AppID, TenantKey: configured.TenantKey, AppSecret: configured.AppSecret,
		AppTicket: configured.AppTicket, ClientAssertion: configured.ClientAssertion}
	switch configured.Profile {
	case "tenant-token":
		value.AccessToken = configured.TenantToken
	case "user-token":
		value.AccessToken = configured.UserToken
	}
	if configured.TokenExpiresUnix != 0 {
		value.ExpiresAt = time.Unix(configured.TokenExpiresUnix, 0)
	}
	if ctx.Err() != nil {
		return Credentials{}, failure(ErrAuth, "credentials", ctx.Err(), context.Cause(ctx))
	}
	if owned.hooks.Credentials != nil {
		value, err = callBorrowed(ctx, ErrAuth, "credentials-exit", func() (Credentials, error) {
			return owned.hooks.Credentials(ctx, request)
		})
		if err != nil {
			return Credentials{}, failure(ErrAuth, "credentials", err, ctx.Err(), context.Cause(ctx))
		}
	}
	if ctx.Err() != nil {
		return Credentials{}, failure(ErrAuth, "credentials", ctx.Err(), context.Cause(ctx))
	}
	if value.AppID != configured.AppID || value.TenantKey != configured.TenantKey {
		return Credentials{}, failure(ErrAuth, "credential-identity")
	}
	valid := false
	switch configured.Profile {
	case "application", "marketplace":
		valid = secretValid(value.AppSecret, 4096) && value.AccessToken == "" && value.ClientAssertion == "" && value.ExpiresAt.IsZero()
		if configured.Profile == "marketplace" {
			valid = valid && secretValid(value.AppTicket, 4096)
		} else {
			valid = valid && value.AppTicket == ""
		}
	case "tenant-token", "user-token":
		valid = secretValid(value.AccessToken, 4096) && value.AppSecret == "" && value.AppTicket == "" && value.ClientAssertion == "" && time.Now().Before(value.ExpiresAt)
	case "client-assertion":
		valid = secretValid(value.ClientAssertion, 16<<10) && value.AppSecret == "" && value.AppTicket == "" && value.AccessToken == "" && value.ExpiresAt.IsZero()
	}
	if !valid {
		return Credentials{}, failure(ErrAuth, "credential-snapshot")
	}
	return value, nil
}

func (owned *owner) accessToken(ctx context.Context, call *invocation.Call[Result], result *Result) (string, error, error) {
	request := CredentialRequest{Purpose: "api"}
	if owned.settings.Profile == "client-assertion" {
		target, _ := url.Parse(owned.settings.OAuthBaseURL)
		request.Audience = target.Host
	}
	credential, err := owned.credentials(ctx, request)
	if err != nil {
		return "", err, nil
	}
	value := owned.settings
	if value.Profile == "tenant-token" || value.Profile == "user-token" {
		return credential.AccessToken, nil, nil
	}
	select {
	case owned.tokenLock <- struct{}{}:
	case <-ctx.Done():
		return "", failure(ErrAuth, "token-wait", ctx.Err(), context.Cause(ctx)), nil
	}
	defer func() { <-owned.tokenLock }()
	if owned.credential == credential && owned.token != "" && time.Now().Before(owned.expires) {
		return owned.token, nil, nil
	}
	owned.token = ""
	owned.expires = time.Time{}
	owned.credential = credential
	started := time.Now()
	var cleanup error
	acquire := func(name, path, key string, body any, oauth bool) (string, int, error) {
		spec := requestSpec{operation: name, method: "POST", path: path, body: body}
		if oauth {
			spec.baseURL = value.OAuthBaseURL
			if value.AssertionProxyURL != "" {
				spec.baseURL = value.AssertionProxyURL
				spec.path = value.AssertionProxyPrefix + path
				spec.headers = http.Header{larkcore.HeaderXTargetService: {request.Audience}}
			}
		}
		state, primary := owned.nativeRequest(ctx, call, spec, "")
		observed := exchange(state)
		if primary == nil {
			if oauth {
				observed, primary = decodeOAuthResponse(state)
			} else {
				observed, _, primary = decodeResponse(state, false)
			}
		}
		if primary != nil {
			primary = failure(ErrAuth, name, primary)
		}
		stage := AuthenticationStage{name: name, attempted: state.entered, exchange: observed, primary: primary, cleanup: state.cleanup}
		if state.entered {
			stage.effect = Unknown
		}
		if observed.codeKnown && observed.code != 0 && observed.status < 500 {
			stage.effect = Rejected
		}
		result.authAttempted = result.authAttempted || state.entered
		result.authentication = observed
		cleanup = errors.Join(cleanup, state.cleanup)
		defer func() { result.authStages = append(result.authStages, stage) }()
		if primary != nil || state.cleanup != nil {
			return "", 0, primary
		}
		expiryKey := "expire"
		if oauth {
			expiryKey = "expires_in"
		}
		fields, decodeErr := exactFields(state.response.RawBody, key, expiryKey)
		var token string
		var expiry int
		if decodeErr == nil {
			if decodeErr = json.Unmarshal(fields[key], &token); decodeErr == nil {
				decodeErr = json.Unmarshal(fields[expiryKey], &expiry)
			}
		}
		if decodeErr != nil || !secretValid(token, 4096) || expiry < 1 || expiry > 7200 {
			stage.primary = failure(ErrAuth, "token-response", decodeErr)
			return "", 0, stage.primary
		}
		stage.effect = Accepted
		return token, expiry, nil
	}
	var token string
	var ttl int
	switch value.Profile {
	case "application":
		token, ttl, err = acquire("tenant-token", larkcore.TenantAccessTokenInternalUrlPath, "tenant_access_token",
			&larkcore.SelfBuiltTenantAccessTokenReq{AppID: credential.AppID, AppSecret: credential.AppSecret}, false)
	case "marketplace":
		var appToken string
		appToken, _, err = acquire("app-token", larkcore.AppAccessTokenUrlPath, "app_access_token",
			&larkcore.MarketplaceAppAccessTokenReq{AppID: credential.AppID, AppSecret: credential.AppSecret, AppTicket: credential.AppTicket}, false)
		if err == nil && cleanup == nil {
			token, ttl, err = acquire("tenant-token", larkcore.TenantAccessTokenUrlPath, "tenant_access_token",
				&larkcore.MarketplaceTenantAccessTokenReq{AppAccessToken: appToken, TenantKey: credential.TenantKey}, false)
		}
	case "client-assertion":
		token, ttl, err = acquire("client-assertion", larkcore.OAuthTokenUrlPath, "access_token",
			&larkcore.OAuthTokenReq{GrantType: larkcore.GrantTypeJWTBearer, ClientAssertionType: larkcore.ClientAssertionTypeJWTBearer, ClientAssertion: credential.ClientAssertion, ClientID: credential.AppID}, true)
	}
	if err != nil || cleanup != nil {
		return "", err, cleanup
	}
	duration := time.Duration(ttl) * time.Second
	owned.expires = started.Add(duration - min(30*time.Second, duration/10))
	if !time.Now().Before(owned.expires) {
		return "", failure(ErrAuth, "expired-token"), cleanup
	}
	owned.token = token
	return token, nil, cleanup
}

func decodeOAuthResponse(state *requestState) (Exchange, error) {
	observed, _, generic := decodeResponse(state, false)
	if state.response == nil || !observed.codeKnown {
		return observed, generic
	}
	var httpErr error
	if observed.status < 200 || observed.status >= 300 {
		httpErr = failure(ErrHTTP, "status")
	}
	if _, err := exactFields(state.response.RawBody, "access_token", "expires_in", "error", "error_description", "refresh_token", "refresh_token_expires_in", "scope", "token_type"); err != nil {
		return observed, failure(ErrResponse, "oauth-fields", err, httpErr)
	}
	var response larkcore.OAuthTokenResp
	if err := json.Unmarshal(state.response.RawBody, &response); err != nil {
		return observed, failure(ErrResponse, "oauth-response", err, httpErr)
	}
	if observed.code != 0 || response.AccessToken == "" {
		message := response.ErrorDescription
		if message == "" {
			message = response.Error
		}
		if message == "" {
			message = "oauth token response missing access token"
		}
		return observed, failure(ErrAPI, "oauth-response", &larkcore.CodeError{Code: observed.code, Msg: message}, httpErr)
	}
	return observed, generic
}
func (owned *owner) invalidateToken(ctx context.Context, token string) {
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
