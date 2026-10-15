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

package mail

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
	smtp "github.com/wneessen/go-mail/smtp"
)

// NativeOptions contains borrowed runtime authority, never loadable settings.
// Hooks must be concurrency-safe, cooperative and non-panicking. They are never
// invoked by PrepareV1. Dial results become owned, including non-nil error results.
type NativeOptions struct {
	private
	DialContext            func(context.Context, string, string) (net.Conn, error)
	Auth                   func(context.Context, tls.ConnectionState) (smtp.Auth, error)
	TLS                    *tls.Config
	ResponseHandlers       map[string]ResponseHandler
	DefaultResponseHandler ResponseHandler
	AlwaysDKIM             *DKIM
}

// Budget describes the exact frozen selection, not physical RSS or a global quota.
type Budget struct {
	Limits                                resource.Limits
	WorkBytes, EvidenceBytes, SourceBytes int64
	Timeout                               time.Duration
}

// Prepared freezes configuration and runtime containers without network or hooks.
type Prepared struct {
	private
	configuration resource.Prepared[settings]
	native        NativeOptions
	metadata      Budget
}

func PrepareV1(options OptionsV1, layers ...resource.Layer) (Prepared, error) {
	configuration, value, err := prepareSettings(options, layers)
	if err != nil {
		return Prepared{}, err
	}
	hooks, extra, err := freezeNative(options.Native)
	if err != nil {
		return Prepared{}, err
	}
	if (value.Auth == "custom") != (hooks.Auth != nil) {
		return Prepared{}, failure(ErrInput, "custom-auth")
	}
	return Prepared{configuration: configuration, native: hooks, metadata: value.budget(extra)}, nil
}

func prepareSettings(options OptionsV1, layers []resource.Layer) (resource.Prepared[settings], settings, error) {
	version := options.Version
	if version == 0 {
		version = 1
	}
	var value settings
	configuration, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: func(resolved settings) error { value = resolved; return validate(resolved) }},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: version, Layers: layers})
	return configuration, value, err
}

// DataBudgetV1 validates loadable data without requiring or invoking runtime
// dependencies. Native-aware callers use PrepareV1 for the exact larger envelope.
func DataBudgetV1(options OptionsV1) (Budget, error) {
	_, value, err := prepareSettings(options, nil)
	if err != nil {
		return Budget{}, err
	}
	_, extra, _ := freezeNative(NativeOptions{})
	return value.budget(extra), nil
}

func (value settings) budget(extra int64) Budget {
	encoded, _ := json.Marshal(value)
	budget := Budget{Limits: value.limits(), WorkBytes: value.reservation(), EvidenceBytes: value.evidenceReservation(),
		SourceBytes: int64(len(encoded))*4 + extra + int64(value.MaxActive)*(2<<20), Timeout: value.Timeout}
	return budget
}

func (prepared Prepared) Metadata() Budget { return prepared.metadata }
func (prepared Prepared) Description() resource.Description {
	return prepared.configuration.Description()
}
func (prepared Prepared) Select() resource.Selection[Source] {
	return resource.Select(prepared.configuration, func(ctx context.Context, value settings) (resource.Resource[Source], error) {
		if ctx.Err() != nil {
			return resource.Resource[Source]{}, failure(ErrInput, "construct", ctx.Err(), context.Cause(ctx))
		}
		owned := &owner{settings: value, hooks: prepared.native}
		return resource.Resource[Source]{Acquired: true, Capability: Source{owner: owned}, Release: owned.close}, nil
	})
}
