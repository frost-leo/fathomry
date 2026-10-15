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
	"github.com/frost-leo/fathomry/internal/resource"
	"time"
)

// NativeOptions borrows cooperative, concurrent-safe providers until actual return.
// Neither source preparation nor validation invokes them. Each request takes one
// coherent snapshot. A successful empty snapshot is an authentication failure.
// Providers run in a synchronously joined worker; goroutine affinity is not
// guaranteed. Panic and Goexit become failures without losing admitted custody.
type NativeOptions struct {
	private
	Credentials        func(context.Context, CredentialRequest) (Credentials, error)
	WebhookCredentials func(context.Context) (WebhookCredentials, error)
}

// Credentials is one source-bound authentication tuple. Only fields selected by
// the source profile may be populated. ExpiresAt applies to supplied access tokens.
type Credentials struct {
	private
	AppID, TenantKey, AppSecret, AppTicket, AccessToken, ClientAssertion string
	ExpiresAt                                                            time.Time
}

// CredentialRequest distinguishes the OAuth and WebSocket assertion audiences.
// Purpose is api or websocket; Audience is empty outside assertion profiles.
type CredentialRequest struct {
	private
	Purpose, Audience string
}

// WebhookCredentials binds a callback token/key pair to the frozen app and tenant.
// EncryptKey may be empty only for an explicitly token-only source.
type WebhookCredentials struct {
	private
	AppID, TenantKey, VerificationToken, EncryptKey string
}

// Budget is the exact effective declaration, not measured RSS or remote quota.
type Budget struct {
	Limits                                resource.Limits
	WorkBytes, EvidenceBytes, SourceBytes int64
	Timeout                               time.Duration
	ReceiverLifetime                      time.Duration
	WebSocket                             bool
	Pending                               int
	EventBytes, AckBytes                  int64
	MaxRequestBytes                       int
}

// Prepared freezes the effective configuration, callback references and budget.
type Prepared struct {
	private
	configuration resource.Prepared[settings]
	native        NativeOptions
	metadata      Budget
}

func PrepareV1(options OptionsV1, layers ...resource.Layer) (Prepared, error) {
	version := options.Version
	if version == 0 {
		version = 1
	}
	var value settings
	configuration, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options),
		Validate: func(resolved settings) error { value = resolved; return validate(resolved) }},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: version, Layers: layers})
	if err != nil {
		return Prepared{}, err
	}
	if options.Native.Credentials != nil && value.Profile == "webhook" {
		return Prepared{}, failure(ErrInput, "credentials-provider")
	}
	if options.Native.WebhookCredentials != nil && value.VerificationToken == "" {
		return Prepared{}, failure(ErrInput, "webhook-provider")
	}
	encoded, _ := json.Marshal(value)
	metadata := Budget{Limits: value.limits(), WorkBytes: value.reservation(), EvidenceBytes: value.evidenceBytes(),
		SourceBytes: int64(len(encoded))*4 + int64(value.MaxActive)*(64<<10) + 64<<10, Timeout: value.Timeout,
		WebSocket: value.WebSocket.Enabled, Pending: value.WebSocket.MaxPending, EventBytes: value.WebSocket.eventBytes(),
		AckBytes: socketEvidenceBytes, MaxRequestBytes: value.MaxRequestBytes, ReceiverLifetime: value.WebSocket.Lifetime}
	return Prepared{configuration: configuration, native: options.Native, metadata: metadata}, nil
}
func (prepared Prepared) Metadata() Budget { return prepared.metadata }
func (prepared Prepared) Description() resource.Description {
	return prepared.configuration.Description()
}
