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
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/notification/lark/v3"
)

// Settings is strictly loadable source data. Zero bounds use Internal defaults;
// durations are nanoseconds, token expiry is absolute Unix seconds. Credentials
// are sensitive in explicit JSON. Preparation is offline, not service readiness.
type Settings struct {
	Name                 string             `json:"name"`
	Version              uint32             `json:"version"`
	Profile              string             `json:"profile"`
	BaseURL              string             `json:"base_url"`
	AppID                string             `json:"app_id"`
	AppSecret            string             `json:"app_secret"`
	TenantKey            string             `json:"tenant_key"`
	TenantToken          string             `json:"tenant_token"`
	UserToken            string             `json:"user_token"`
	AppTicket            string             `json:"app_ticket"`
	ClientAssertion      string             `json:"client_assertion"`
	OAuthBaseURL         string             `json:"oauth_base_url"`
	AssertionProxyURL    string             `json:"assertion_proxy_url"`
	AssertionProxyPrefix string             `json:"assertion_proxy_prefix"`
	TokenExpiresUnix     int64              `json:"token_expires_unix_seconds"`
	WebhookURL           string             `json:"webhook_url"`
	WebhookSecret        string             `json:"webhook_secret"`
	VerificationToken    string             `json:"verification_token"`
	EncryptKey           string             `json:"encrypt_key"`
	RootCAPEM            string             `json:"root_ca_pem"`
	MaxActive            int                `json:"max_active"`
	QueuedCalls          int                `json:"queued_calls"`
	MaxRequestBytes      int                `json:"max_request_bytes"`
	MaxAssetBytes        int                `json:"max_asset_bytes"`
	MaxResponseBytes     int                `json:"max_response_bytes"`
	Timeout              time.Duration      `json:"timeout_ns"`
	CallbackMaxAge       time.Duration      `json:"callback_max_age_ns"`
	WebSocket            *WebSocketSettings `json:"websocket"`
}

// WebSocketSettings declares one self-built application receiver. Nil disables
// it; a non-nil zero value selects native defaults. AllowedHosts is exact, not a
// wildcard. Lifetime zero requires a cancellable Listen context.
type WebSocketSettings struct {
	AllowedHosts       []string      `json:"allowed_hosts"`
	MaxFrameBytes      int           `json:"max_frame_bytes"`
	MaxMessageBytes    int           `json:"max_message_bytes"`
	MaxPending         int           `json:"max_pending"`
	MaxAssemblies      int           `json:"max_assemblies"`
	MaxFragments       int           `json:"max_fragments"`
	MaxFragmentBytes   int           `json:"max_fragment_bytes"`
	FragmentTimeout    time.Duration `json:"fragment_timeout_ns"`
	AckTimeout         time.Duration `json:"ack_timeout_ns"`
	WriteTimeout       time.Duration `json:"write_timeout_ns"`
	PingInterval       time.Duration `json:"ping_interval_ns"`
	PongTimeout        time.Duration `json:"pong_timeout_ns"`
	MaxConnectAttempts int           `json:"max_connect_attempts"`
	ReconnectMin       time.Duration `json:"reconnect_min_ns"`
	ReconnectMax       time.Duration `json:"reconnect_max_ns"`
	Lifetime           time.Duration `json:"lifetime_ns"`
}
type Credentials struct {
	private
	AppID, TenantKey, AppSecret, AppTicket, AccessToken, ClientAssertion string
	ExpiresAt                                                            time.Time
}

// CredentialRequest distinguishes api and websocket work and their assertion
// Audience. It does not authorize changing the frozen app, tenant or proxy route.
type CredentialRequest struct {
	private
	Purpose, Audience string
}
type WebhookCredentials struct {
	private
	AppID, TenantKey, VerificationToken, EncryptKey string
}

// NativeOptions borrows cooperative, concurrent-safe callbacks. They are invoked
// only under admitted operations, never by Prepare/Validate/Recommend. Errors,
// panics, cancellation and empty tuples fail closed. They must not mutate inputs.
// Callbacks run in a joined worker with no goroutine-affinity guarantee; Goexit
// becomes a failure rather than abandoning the source or operation evidence.
type NativeOptions struct {
	private
	Credentials        func(context.Context, CredentialRequest) (Credentials, error)
	WebhookCredentials func(context.Context) (WebhookCredentials, error)
}

// Dependencies borrows public owners; this adapter never closes Runtime/Inbox.
type Dependencies struct {
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Result]
	Observer *adapters.Observer
	Native   NativeOptions
}

func options(value Settings, hooks NativeOptions) native.OptionsV1 {
	result := native.OptionsV1{Name: value.Name, Version: value.Version, Profile: value.Profile, BaseURL: value.BaseURL,
		AppID: value.AppID, AppSecret: value.AppSecret, TenantKey: value.TenantKey, TenantToken: value.TenantToken,
		UserToken: value.UserToken, AppTicket: value.AppTicket, ClientAssertion: value.ClientAssertion, OAuthBaseURL: value.OAuthBaseURL,
		AssertionProxyURL: value.AssertionProxyURL, AssertionProxyPrefix: value.AssertionProxyPrefix,
		WebhookURL: value.WebhookURL, WebhookSecret: value.WebhookSecret, VerificationToken: value.VerificationToken, EncryptKey: value.EncryptKey,
		RootCAPEM: value.RootCAPEM, MaxActive: value.MaxActive, QueuedCalls: value.QueuedCalls, MaxRequestBytes: value.MaxRequestBytes,
		MaxAssetBytes: value.MaxAssetBytes, MaxResponseBytes: value.MaxResponseBytes, Timeout: value.Timeout, CallbackMaxAge: value.CallbackMaxAge}
	if value.TokenExpiresUnix != 0 {
		result.TokenExpiresAt = time.Unix(value.TokenExpiresUnix, 0)
	}
	if value.WebSocket != nil {
		ws := value.WebSocket
		result.WebSocket = &native.WebSocketOptions{AllowedHosts: ws.AllowedHosts, MaxFrameBytes: ws.MaxFrameBytes, MaxMessageBytes: ws.MaxMessageBytes,
			MaxPending: ws.MaxPending, MaxAssemblies: ws.MaxAssemblies, MaxFragments: ws.MaxFragments, MaxFragmentBytes: ws.MaxFragmentBytes,
			FragmentTimeout: ws.FragmentTimeout, AckTimeout: ws.AckTimeout, WriteTimeout: ws.WriteTimeout, PingInterval: ws.PingInterval, PongTimeout: ws.PongTimeout,
			MaxConnectAttempts: ws.MaxConnectAttempts, ReconnectMin: ws.ReconnectMin, ReconnectMax: ws.ReconnectMax, Lifetime: ws.Lifetime}
	}
	if hooks.Credentials != nil {
		result.Native.Credentials = func(ctx context.Context, request native.CredentialRequest) (native.Credentials, error) {
			value, err := hooks.Credentials(ctx, CredentialRequest{Purpose: request.Purpose, Audience: request.Audience})
			return native.Credentials{AppID: value.AppID, TenantKey: value.TenantKey, AppSecret: value.AppSecret, AppTicket: value.AppTicket,
				AccessToken: value.AccessToken, ClientAssertion: value.ClientAssertion, ExpiresAt: value.ExpiresAt}, err
		}
	}
	if hooks.WebhookCredentials != nil {
		result.Native.WebhookCredentials = func(ctx context.Context) (native.WebhookCredentials, error) {
			value, err := hooks.WebhookCredentials(ctx)
			return native.WebhookCredentials{AppID: value.AppID, TenantKey: value.TenantKey, VerificationToken: value.VerificationToken, EncryptKey: value.EncryptKey}, err
		}
	}
	return result
}

// Validate is offline and callback-free, using the same effective preparation.
func Validate(value Settings) error { _, err := Prepare(value, NativeOptions{}); return err }
