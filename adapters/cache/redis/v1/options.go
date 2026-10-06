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

package redis

import (
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/cache/redis/v9"
)

// Settings is strict-loadable configuration, not a runtime handle. Version zero
// selects format 1; durations are integer nanoseconds. Zero fields select Internal
// defaults except DB/queue/redirects/lifetime (zero disables or selects DB0).
// MaxIdleTime nil selects five minutes; a non-nil zero disables idle expiry.
// Explicit JSON contains secrets; ordinary formatting and logging are redacted.
// Endpoint/trust authority and every command grant are explicit.
type Settings struct {
	Name                     string            `json:"name" mapstructure:"name"`
	Version                  uint32            `json:"version" mapstructure:"version"`
	Mode                     string            `json:"mode" mapstructure:"mode"`
	UniversalMode            string            `json:"universal_mode" mapstructure:"universal_mode"`
	Addrs                    []string          `json:"addrs" mapstructure:"addrs"`
	AllowedAddrs             []string          `json:"allowed_addrs" mapstructure:"allowed_addrs"`
	Shards                   map[string]string `json:"shards" mapstructure:"shards"`
	MasterName               string            `json:"master_name" mapstructure:"master_name"`
	DB                       int               `json:"db" mapstructure:"db"`
	Protocol                 int               `json:"protocol" mapstructure:"protocol"`
	Username                 string            `json:"username" mapstructure:"username"`
	Password                 string            `json:"password" mapstructure:"password"`
	SentinelUsername         string            `json:"sentinel_username" mapstructure:"sentinel_username"`
	SentinelPassword         string            `json:"sentinel_password" mapstructure:"sentinel_password"`
	Plaintext                bool              `json:"plaintext" mapstructure:"plaintext"`
	RootCAPEM                string            `json:"root_ca_pem" mapstructure:"root_ca_pem"`
	ServerName               string            `json:"server_name" mapstructure:"server_name"`
	CertificatePEM           string            `json:"certificate_pem" mapstructure:"certificate_pem"`
	PrivateKeyPEM            string            `json:"private_key_pem" mapstructure:"private_key_pem"`
	ReadOnly                 bool              `json:"read_only" mapstructure:"read_only"`
	MaxActive                int               `json:"max_active" mapstructure:"max_active"`
	QueuedCalls              int               `json:"queued_calls" mapstructure:"queued_calls"`
	MaxConnections           int               `json:"max_connections" mapstructure:"max_connections"`
	MaxCommands              int               `json:"max_commands" mapstructure:"max_commands"`
	MaxArgs                  int               `json:"max_args" mapstructure:"max_args"`
	MaxRequestBytes          int               `json:"max_request_bytes" mapstructure:"max_request_bytes"`
	MaxReplyBytes            int               `json:"max_reply_bytes" mapstructure:"max_reply_bytes"`
	MaxReplyElements         int               `json:"max_reply_elements" mapstructure:"max_reply_elements"`
	Timeout                  time.Duration     `json:"timeout_ns" mapstructure:"timeout_ns"`
	CloseTimeout             time.Duration     `json:"close_timeout_ns" mapstructure:"close_timeout_ns"`
	MaxIdleTime              *time.Duration    `json:"max_idle_time_ns" mapstructure:"max_idle_time_ns"`
	MaxLifetime              time.Duration     `json:"max_lifetime_ns" mapstructure:"max_lifetime_ns"`
	MaxRedirects             int               `json:"max_redirects" mapstructure:"max_redirects"`
	Commands                 []string          `json:"commands" mapstructure:"commands"`
	AdminCommands            []string          `json:"admin_commands" mapstructure:"admin_commands"`
	ExperimentalCache        bool              `json:"experimental_cache" mapstructure:"experimental_cache"`
	CacheEntries             int               `json:"cache_entries" mapstructure:"cache_entries"`
	CacheBytes               int64             `json:"cache_bytes" mapstructure:"cache_bytes"`
	CacheMaxStaleness        time.Duration     `json:"cache_max_staleness_ns" mapstructure:"cache_max_staleness_ns"`
	ExperimentalAutoPipeline bool              `json:"experimental_auto_pipeline" mapstructure:"experimental_auto_pipeline"`
	AllowSessions            bool              `json:"allow_sessions" mapstructure:"allow_sessions"`
	AllowSubscriptions       bool              `json:"allow_subscriptions" mapstructure:"allow_subscriptions"`
	MaintenanceMode          string            `json:"maintenance_mode" mapstructure:"maintenance_mode"`
}

func options(value Settings) native.OptionsV1 {
	return native.OptionsV1{
		Name:                     value.Name,
		Version:                  value.Version,
		Mode:                     value.Mode,
		UniversalMode:            value.UniversalMode,
		Addrs:                    value.Addrs,
		AllowedAddrs:             value.AllowedAddrs,
		Shards:                   value.Shards,
		MasterName:               value.MasterName,
		DB:                       value.DB,
		Protocol:                 value.Protocol,
		Username:                 value.Username,
		Password:                 value.Password,
		SentinelUsername:         value.SentinelUsername,
		SentinelPassword:         value.SentinelPassword,
		Plaintext:                value.Plaintext,
		RootCAPEM:                value.RootCAPEM,
		ServerName:               value.ServerName,
		CertificatePEM:           value.CertificatePEM,
		PrivateKeyPEM:            value.PrivateKeyPEM,
		ReadOnly:                 value.ReadOnly,
		MaxActive:                value.MaxActive,
		QueuedCalls:              value.QueuedCalls,
		MaxConnections:           value.MaxConnections,
		MaxCommands:              value.MaxCommands,
		MaxArgs:                  value.MaxArgs,
		MaxRequestBytes:          value.MaxRequestBytes,
		MaxReplyBytes:            value.MaxReplyBytes,
		MaxReplyElements:         value.MaxReplyElements,
		Timeout:                  value.Timeout,
		CloseTimeout:             value.CloseTimeout,
		MaxLifetime:              value.MaxLifetime,
		MaxRedirects:             value.MaxRedirects,
		Commands:                 value.Commands,
		AdminCommands:            value.AdminCommands,
		ExperimentalCache:        value.ExperimentalCache,
		CacheEntries:             value.CacheEntries,
		CacheBytes:               value.CacheBytes,
		CacheMaxStaleness:        value.CacheMaxStaleness,
		ExperimentalAutoPipeline: value.ExperimentalAutoPipeline,
		AllowSessions:            value.AllowSessions,
		AllowSubscriptions:       value.AllowSubscriptions,
		MaintenanceMode:          value.MaintenanceMode,
	}
}

// Validate is offline; it does not construct clients or change native logging.
func Validate(value Settings) error { _, err := Prepare(value); return err }

// Dependencies belong to composition and must be shared across views and
// overlapping generations. Observers are optional; required Evidence is not.
type Dependencies struct {
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Result]
	Observer *adapters.Observer
}
