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
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/resource"
)

// OptionsV1 is borrowed only by Select. Version zero selects configuration v1,
// independent of SDK v3, HTTP API versions and card schema 2.0. Profile is explicit:
// application acquires a self-built tenant token; tenant-token freezes a supplied
// token and expiry; webhook requires a signed custom-bot URL and secret.
// AppID/TenantKey describe the selected authority, not a claim of verified scope.
// Optional callback credentials enable HTTP ingress on application profiles.
//
// Defaults: 2 active calls, no queue, 256 KiB JSON/request, 10 MiB asset,
// 12 MiB response, 30 s admission/execution. A webhook envelope is additionally
// capped at 20 KiB. Durations are ns; TokenExpiresAt is an absolute UTC time,
// normalized to Unix seconds in the token_expires_unix_seconds overlay field.
// BaseURL defaults to Feishu; all endpoints require verified HTTPS, no redirects,
// environment proxy, plaintext fallback or native extension callbacks.
// Rotation requires replacing the source. Secrets must never be used as names.
type OptionsV1 struct {
	private
	Name              string
	Version           uint32
	Profile           string
	BaseURL           string
	AppID             string
	AppSecret         string
	TenantKey         string
	TenantToken       string
	TokenExpiresAt    time.Time
	WebhookURL        string
	WebhookSecret     string
	VerificationToken string
	EncryptKey        string
	RootCAPEM         string
	MaxActive         int
	QueuedCalls       int
	MaxRequestBytes   int
	MaxAssetBytes     int
	MaxResponseBytes  int
	Timeout           time.Duration
	CallbackMaxAge    time.Duration
	WebSocket         *WebSocketOptions
}
type settings struct {
	Profile           string         `json:"profile"`
	BaseURL           string         `json:"base_url"`
	AppID             string         `json:"app_id"`
	AppSecret         string         `json:"app_secret"`
	TenantKey         string         `json:"tenant_key"`
	TenantToken       string         `json:"tenant_token"`
	TokenExpiresUnix  int64          `json:"token_expires_unix_seconds"`
	WebhookURL        string         `json:"webhook_url"`
	WebhookSecret     string         `json:"webhook_secret"`
	VerificationToken string         `json:"verification_token"`
	EncryptKey        string         `json:"encrypt_key"`
	RootCAPEM         string         `json:"root_ca_pem"`
	MaxActive         int            `json:"max_active"`
	QueuedCalls       int            `json:"queued_calls"`
	MaxRequestBytes   int            `json:"max_request_bytes"`
	MaxAssetBytes     int            `json:"max_asset_bytes"`
	MaxResponseBytes  int            `json:"max_response_bytes"`
	Timeout           time.Duration  `json:"timeout_ns"`
	CallbackMaxAge    time.Duration  `json:"callback_max_age_ns"`
	WebSocket         socketSettings `json:"websocket"`
}

func defaults(input OptionsV1) settings {
	value := settings{Profile: input.Profile, BaseURL: input.BaseURL, AppID: input.AppID, AppSecret: input.AppSecret,
		TenantKey: input.TenantKey, TenantToken: input.TenantToken,
		WebhookURL: input.WebhookURL, WebhookSecret: input.WebhookSecret, VerificationToken: input.VerificationToken, EncryptKey: input.EncryptKey,
		RootCAPEM: input.RootCAPEM, MaxActive: input.MaxActive, QueuedCalls: input.QueuedCalls,
		MaxRequestBytes: input.MaxRequestBytes, MaxAssetBytes: input.MaxAssetBytes, MaxResponseBytes: input.MaxResponseBytes,
		Timeout: input.Timeout, CallbackMaxAge: input.CallbackMaxAge}
	if !input.TokenExpiresAt.IsZero() {
		value.TokenExpiresUnix = input.TokenExpiresAt.Unix()
	}
	if value.BaseURL == "" {
		value.BaseURL = "https://open.feishu.cn"
	}
	value.WebSocket = socketDefaults(input.WebSocket, value.BaseURL)
	if value.MaxActive == 0 {
		value.MaxActive = 2
	}
	if value.MaxRequestBytes == 0 {
		value.MaxRequestBytes = 256 << 10
	}
	if value.MaxAssetBytes == 0 {
		value.MaxAssetBytes = 10 << 20
	}
	if value.MaxResponseBytes == 0 {
		value.MaxResponseBytes = 12 << 20
	}
	if value.Timeout == 0 {
		value.Timeout = 30 * time.Second
	}
	if value.CallbackMaxAge == 0 {
		value.CallbackMaxAge = 5 * time.Minute
	}
	return value
}
func secretValid(value string, limit int) bool {
	return value != "" && len(value) <= limit && !strings.ContainsAny(value, "\r\n\x00")
}
func endpoint(value string, webhook bool) bool {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" || len(value) > 2048 {
		return false
	}
	if webhook {
		return strings.HasPrefix(parsed.Path, "/open-apis/bot/v2/hook/") && len(parsed.Path) > len("/open-apis/bot/v2/hook/")
	}
	return parsed.Path == "" && parsed.RawPath == ""
}
func validate(value settings) error {
	if err := value.WebSocket.validate(value.Profile); err != nil {
		return err
	}
	if !endpoint(value.BaseURL, false) || value.MaxActive < 1 || value.MaxActive > 16 ||
		value.QueuedCalls < 0 || value.QueuedCalls > 32 || value.MaxRequestBytes < 1024 || value.MaxRequestBytes > 1<<20 ||
		value.MaxAssetBytes < 1024 || value.MaxAssetBytes > 30<<20 || value.MaxResponseBytes < 1024 || value.MaxResponseBytes > 32<<20 ||
		value.Timeout < time.Millisecond || value.Timeout > 5*time.Minute || value.CallbackMaxAge < time.Second || value.CallbackMaxAge > 10*time.Minute {
		return failure(ErrInput, "options")
	}
	switch value.Profile {
	case "application":
		if !identifier(value.AppID) || !secretValid(value.AppSecret, 4096) || value.TenantToken != "" || value.TokenExpiresUnix != 0 {
			return failure(ErrInput, "credentials")
		}
	case "tenant-token":
		if !identifier(value.AppID) || !identifier(value.TenantKey) || !secretValid(value.TenantToken, 4096) || value.TokenExpiresUnix <= 0 || value.AppSecret != "" {
			return failure(ErrInput, "credentials")
		}
	case "webhook":
		if !endpoint(value.WebhookURL, true) || !secretValid(value.WebhookSecret, 4096) || value.AppID != "" || value.AppSecret != "" ||
			value.TenantKey != "" || value.TenantToken != "" || value.TokenExpiresUnix != 0 || value.VerificationToken != "" || value.EncryptKey != "" {
			return failure(ErrInput, "credentials")
		}
	default:
		return failure(ErrUnsupported, "profile")
	}
	if value.Profile != "webhook" && (value.WebhookURL != "" || value.WebhookSecret != "") {
		return failure(ErrInput, "profile")
	}
	if value.TenantKey != "" && !identifier(value.TenantKey) {
		return failure(ErrInput, "tenant")
	}
	if value.VerificationToken != "" || value.EncryptKey != "" {
		if !secretValid(value.VerificationToken, 4096) || !secretValid(value.EncryptKey, 4096) || !identifier(value.TenantKey) {
			return failure(ErrInput, "callback-credentials")
		}
	}
	_, err := value.tls()
	return err
}
func (value settings) tls() (*tls.Config, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	if len(value.RootCAPEM) > 128<<10 {
		return nil, failure(ErrLimit, "trust")
	}
	if value.RootCAPEM != "" {
		config.RootCAs = x509.NewCertPool()
		if !config.RootCAs.AppendCertsFromPEM([]byte(value.RootCAPEM)) {
			return nil, failure(ErrInput, "trust")
		}
	}
	return config, nil
}
func (value settings) evidenceBytes() int64 {
	return int64(value.MaxResponseBytes)*4 + int64(value.MaxRequestBytes)*8 + 32<<10
}
func (value settings) reservation() int64 {
	return int64(value.MaxRequestBytes)*32 + int64(value.MaxAssetBytes)*4 + int64(value.MaxResponseBytes)*32 + value.evidenceBytes() + value.WebSocket.reservation()
}
func (value settings) limits() resource.Limits {
	return resource.Limits{Active: value.MaxActive, Queued: value.QueuedCalls, Bytes: int64(value.MaxActive) * value.reservation(),
		QueuedBytes: int64(value.QueuedCalls) * value.reservation(), MaxLeases: value.maxLeases()}
}

// LimitsV1 returns the defaulted working-envelope policy. Overlays must attach
// their corresponding resolved policy; Bind rejects undersized allowances.
func LimitsV1(options OptionsV1) resource.Limits { return defaults(options).limits() }

// EvidenceBytesV1 is the retained-byte reservation required for one inbox slot.
func EvidenceBytesV1(options OptionsV1) int64 { return defaults(options).evidenceBytes() }

// Profile declares the effective mode, not service or client-rendering evidence.
func (client *Client) Profile() compatibility.Profile {
	if client == nil || client.owner == nil {
		return compatibility.Profile{}
	}
	value := client.owner.settings
	options := []compatibility.Option{{Name: "token-cache", Value: "source-owned"}, {Name: "retry", Value: "disabled"}, {Name: "http", Value: "direct-http1-no-reuse"}, {Name: "extensions", Value: "closed"}, {Name: "http-ingress", Value: strconv.FormatBool(value.EncryptKey != "")}}
	for _, pair := range []struct {
		name  string
		value int64
	}{
		{"max-active", int64(value.MaxActive)}, {"queued-calls", int64(value.QueuedCalls)}, {"request-bytes", int64(value.MaxRequestBytes)},
		{"asset-bytes", int64(value.MaxAssetBytes)}, {"response-bytes", int64(value.MaxResponseBytes)}, {"timeout-ns", int64(value.Timeout)}, {"callback-max-age-ns", int64(value.CallbackMaxAge)},
	} {
		options = append(options, compatibility.Option{Name: pair.name, Value: strconv.FormatInt(pair.value, 10)})
	}
	return compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "lark-" + value.Profile,
		ServiceMode: compatibility.Fact{Kind: compatibility.Declared, Value: "notification"},
		Protocol:    compatibility.Fact{Kind: compatibility.Declared, Value: "https-json-card2"},
		Native:      compatibility.Fact{Kind: compatibility.NotApplicable},
		Options:     options}
}
