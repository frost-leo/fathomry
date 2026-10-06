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

package mysql

import (
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/database/mysql/v1"
)

// Dependencies borrows caller-owned admission and required evidence custody.
type Dependencies struct {
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Result]
	Observer *adapters.Observer
}

const (
	ProviderID            = native.ProviderID
	MaxSQLBytes           = native.MaxSQLBytes
	MaxArguments          = native.MaxArguments
	MaxArgumentBytes      = native.MaxArgumentBytes
	MaxColumns            = native.MaxColumns
	MaxPreparedStatements = native.MaxPreparedStatements
)

// Settings is loadable data covering the supported native profile, not a DSN
// or runtime handle. Credentials and trust are explicit. Durations are integer
// nanoseconds; zero selects native defaults, except zero idle/lifetime limits
// disable expiration and zero QueuedCalls disables queueing.
// MaxIdleConnections=0 selects MaxConnections; -1 disables idle reuse.
// Validate checks settings, not service readiness; use Client.Ping separately.
// Values and non-nil pointers are automatically redacted when logged directly.
// Do not pass a typed-nil *Settings to slog: normalize it to untyped nil or omit
// the attribute. Explicit JSON encoding remains ordinary configuration data.
type Settings struct {
	Name                    string        `json:"name" mapstructure:"name"`
	Address                 string        `json:"address" mapstructure:"address"`
	Port                    uint16        `json:"port" mapstructure:"port"`
	Database                string        `json:"database" mapstructure:"database"`
	User                    string        `json:"user" mapstructure:"user"`
	Password                string        `json:"password" mapstructure:"password"`
	RootCAPEM               string        `json:"root_ca_pem" mapstructure:"root_ca_pem"`
	ServerName              string        `json:"server_name" mapstructure:"server_name"`
	Plaintext               bool          `json:"plaintext" mapstructure:"plaintext"`
	MaxConnections          int           `json:"max_connections" mapstructure:"max_connections"`
	MaxIdleTime             time.Duration `json:"max_idle_time_ns" mapstructure:"max_idle_time_ns"`
	MaxLifetime             time.Duration `json:"max_lifetime_ns" mapstructure:"max_lifetime_ns"`
	QueuedCalls             int           `json:"queued_calls" mapstructure:"queued_calls"`
	Timeout                 time.Duration `json:"timeout_ns" mapstructure:"timeout_ns"`
	CloseTimeout            time.Duration `json:"close_timeout_ns" mapstructure:"close_timeout_ns"`
	MaxRows                 int           `json:"max_rows" mapstructure:"max_rows"`
	MaxResultBytes          int           `json:"max_result_bytes" mapstructure:"max_result_bytes"`
	Network                 string        `json:"network" mapstructure:"network"`
	ServerCertificateSHA256 string        `json:"server_certificate_sha256" mapstructure:"server_certificate_sha256"`
	ParseTime               bool          `json:"parse_time" mapstructure:"parse_time"`
	ClientFoundRows         bool          `json:"client_found_rows" mapstructure:"client_found_rows"`
	ColumnsWithAlias        bool          `json:"columns_with_alias" mapstructure:"columns_with_alias"`
	Authentication          string        `json:"authentication" mapstructure:"authentication"`
	MaxIdleConnections      int           `json:"max_idle_connections" mapstructure:"max_idle_connections"`
	ReadTimeout             time.Duration `json:"read_timeout_ns" mapstructure:"read_timeout_ns"`
	WriteTimeout            time.Duration `json:"write_timeout_ns" mapstructure:"write_timeout_ns"`
	TransactionTimeout      time.Duration `json:"transaction_timeout_ns" mapstructure:"transaction_timeout_ns"`
	MaxPacketBytes          int           `json:"max_packet_bytes" mapstructure:"max_packet_bytes"`
	MaxResponseBytes        int           `json:"max_response_bytes" mapstructure:"max_response_bytes"`
}

func options(value Settings) native.OptionsV1 {
	return native.OptionsV1{
		Name:                    value.Name,
		Address:                 value.Address,
		Port:                    value.Port,
		Database:                value.Database,
		User:                    value.User,
		Password:                value.Password,
		RootCAPEM:               value.RootCAPEM,
		ServerName:              value.ServerName,
		Plaintext:               value.Plaintext,
		MaxConnections:          value.MaxConnections,
		MaxIdleTime:             value.MaxIdleTime,
		MaxLifetime:             value.MaxLifetime,
		QueuedCalls:             value.QueuedCalls,
		Timeout:                 value.Timeout,
		CloseTimeout:            value.CloseTimeout,
		MaxRows:                 value.MaxRows,
		MaxResultBytes:          value.MaxResultBytes,
		Network:                 value.Network,
		ServerCertificateSHA256: value.ServerCertificateSHA256,
		ParseTime:               value.ParseTime,
		ClientFoundRows:         value.ClientFoundRows,
		ColumnsWithAlias:        value.ColumnsWithAlias,
		Authentication:          value.Authentication,
		MaxIdleConnections:      value.MaxIdleConnections,
		ReadTimeout:             value.ReadTimeout,
		WriteTimeout:            value.WriteTimeout,
		TransactionTimeout:      value.TransactionTimeout,
		MaxPacketBytes:          value.MaxPacketBytes,
		MaxResponseBytes:        value.MaxResponseBytes,
	}
}

// Validate freezes and checks settings without pool construction or service I/O.
func Validate(value Settings) error {
	_, err := native.Select(options(value))
	return translate(err, "validate")
}
