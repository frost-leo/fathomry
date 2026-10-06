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
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/configsource/nacos/v2"
)

const (
	ProviderID       = native.ProviderID
	MaxKeys          = native.MaxKeys
	MaxServers       = native.MaxServers
	MaxDocumentBytes = native.MaxDocumentBytes
	MaxTotalBytes    = native.MaxTotalBytes
	MaxWireBytes     = native.MaxWireBytes
)

// Settings is loadable bootstrap or business-source data, not a native Options
// alias. It contains every field of the supported native profile. Callers supply
// it before remote acquisition; this package never reads credentials implicitly.
//
// Name is a required non-secret lowercase label. Empty Namespace uses the native
// default form; AppName defaults to fathomry. Both endpoints are explicit. TLS is
// verified unless AllowInsecure explicitly permits plaintext. Empty roots use
// system trust; nonempty RootCAPEM replaces that trust. Credentials occur together.
//
// Durations use nanoseconds. Zero RequestTimeout/RetryDelay/ReconcileInterval
// select 10s/100ms/30s. ConcurrentRequests defaults to 4 (2..16),
// QueuedRequests=0 disables waiting (0..64), Subscriptions defaults to 1 and must
// be less than concurrency; QueueCapacity defaults to 16 (1..64). These are native
// ceilings, independent of the explicitly supplied public operation runtime.
type Settings struct {
	Name               string        `json:"name" mapstructure:"name"`
	Namespace          string        `json:"namespace" mapstructure:"namespace"`
	AppName            string        `json:"app_name" mapstructure:"app_name"`
	Servers            []Server      `json:"servers" mapstructure:"servers"`
	Keys               []Key         `json:"keys" mapstructure:"keys"`
	DynamicKeys        bool          `json:"dynamic_keys" mapstructure:"dynamic_keys"`
	Writable           bool          `json:"writable" mapstructure:"writable"`
	Username           string        `json:"username" mapstructure:"username"`
	Password           string        `json:"password" mapstructure:"password"`
	RootCAPEM          string        `json:"root_ca_pem" mapstructure:"root_ca_pem"`
	AllowInsecure      bool          `json:"allow_insecure" mapstructure:"allow_insecure"`
	RequestTimeout     time.Duration `json:"request_timeout_ns" mapstructure:"request_timeout_ns"`
	RetryDelay         time.Duration `json:"retry_delay_ns" mapstructure:"retry_delay_ns"`
	ReconcileInterval  time.Duration `json:"reconcile_interval_ns" mapstructure:"reconcile_interval_ns"`
	ConcurrentRequests int           `json:"concurrent_requests" mapstructure:"concurrent_requests"`
	QueuedRequests     int           `json:"queued_requests" mapstructure:"queued_requests"`
	Subscriptions      int           `json:"subscriptions" mapstructure:"subscriptions"`
	QueueCapacity      int           `json:"queue_capacity" mapstructure:"queue_capacity"`
}

// Server supplies both authorized endpoints; no peer discovery or port inference.
type Server struct {
	HTTPURL     string `json:"http_url" mapstructure:"http_url"`
	GRPCAddress string `json:"grpc_address" mapstructure:"grpc_address"`
}

// Key selects a document in the configured namespace. Empty Group uses
// DEFAULT_GROUP. DataID is required. Both have native 128-byte identifier bounds.
type Key struct {
	Group  string `json:"group" mapstructure:"group"`
	DataID string `json:"data_id" mapstructure:"data_id"`
}

// ObserveOptions bounds already-acquired complete batches. Zero selects 2;
// valid 1..16. Overflow discards oldest and marks the next delivered batch Gap.
// Work admission reserves 16 MiB plus 4 MiB per slot, not measured process RSS.
type ObserveOptions struct {
	QueueCapacity int `json:"queue_capacity" mapstructure:"queue_capacity"`
}

// Validate checks the full native profile without opening a client or probing a
// service. It does not establish readiness or authorize writes.
func Validate(value Settings) error {
	selected, err := options(value)
	if err != nil {
		return err
	}
	return translate(native.ValidateOptions(selected), "validate")
}
func options(value Settings) (native.OptionsV1, error) {
	if len(value.Servers) > MaxServers || len(value.Keys) > MaxKeys {
		return native.OptionsV1{}, fail(ErrLimit, "settings")
	}
	result := native.OptionsV1{
		Name: value.Name, Namespace: value.Namespace, AppName: value.AppName, DynamicKeys: value.DynamicKeys, Writable: value.Writable,
		Username: value.Username, Password: value.Password, RootCAPEM: value.RootCAPEM, AllowInsecure: value.AllowInsecure,
		RequestTimeout: value.RequestTimeout, RetryDelay: value.RetryDelay, ReconcileInterval: value.ReconcileInterval,
		ConcurrentRequests: value.ConcurrentRequests, QueuedRequests: value.QueuedRequests, Subscriptions: value.Subscriptions, QueueCapacity: value.QueueCapacity,
	}
	for _, server := range value.Servers {
		result.Servers = append(result.Servers, native.ServerV1{HTTPURL: server.HTTPURL, GRPCAddress: server.GRPCAddress})
	}
	for _, key := range value.Keys {
		result.Keys = append(result.Keys, nativeKey(key))
	}
	return result, nil
}
func nativeKey(value Key) native.KeyV1 { return native.KeyV1{Group: value.Group, DataID: value.DataID} }
func publicKey(value native.KeyV1) Key { return Key{Group: value.Group, DataID: value.DataID} }

// Dependencies binds caller-owned operation admission and required evidence.
// The caller owns runtime shutdown and evidence delivery/acknowledgement.
type Dependencies struct {
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Evidence]
	Observer *adapters.Observer
}
