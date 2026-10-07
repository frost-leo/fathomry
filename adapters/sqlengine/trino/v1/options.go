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

package trino

import (
	"context"
	"time"

	"github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/sqlengine/trino/v0"
)

// ProviderID is the native integration identity, independent of public API v1.
const ProviderID = native.ProviderID

// Dependencies borrows caller-owned operation admission and independent evidence.
type Dependencies struct {
	private
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Result]
	Observer *adapters.Observer
}

// Settings is explicit loadable data, not a DSN or ambient environment reader.
// Durations are integer nanoseconds. Zero positive limits choose native defaults;
// Version zero selects format 1. False permission gates remain false. Credentials
// require HTTPS; Plaintext explicitly permits unauthenticated HTTP. Intentional
// configuration JSON serialization can disclose secrets; ordinary logging cannot.
type Settings struct {
	Name             string        `json:"name" mapstructure:"name"`
	Version          uint32        `json:"version" mapstructure:"version"`
	Endpoint         string        `json:"endpoint" mapstructure:"endpoint"`
	User             string        `json:"user" mapstructure:"user"`
	Password         string        `json:"password" mapstructure:"password"`
	BearerToken      string        `json:"bearer_token" mapstructure:"bearer_token"`
	RootCAPEM        string        `json:"root_ca_pem" mapstructure:"root_ca_pem"`
	Plaintext        bool          `json:"plaintext" mapstructure:"plaintext"`
	Catalog          string        `json:"catalog" mapstructure:"catalog"`
	Schema           string        `json:"schema" mapstructure:"schema"`
	Writes           bool          `json:"writes" mapstructure:"writes"`
	Maintenance      bool          `json:"maintenance" mapstructure:"maintenance"`
	MaxActive        int           `json:"max_active" mapstructure:"max_active"`
	MaxSQLBytes      int           `json:"max_sql_bytes" mapstructure:"max_sql_bytes"`
	MaxParameters    int           `json:"max_parameters" mapstructure:"max_parameters"`
	MaxRows          int           `json:"max_rows" mapstructure:"max_rows"`
	MaxColumns       int           `json:"max_columns" mapstructure:"max_columns"`
	MaxPageBytes     int           `json:"max_page_bytes" mapstructure:"max_page_bytes"`
	MaxResultBytes   int           `json:"max_result_bytes" mapstructure:"max_result_bytes"`
	MaxPages         int           `json:"max_pages" mapstructure:"max_pages"`
	MaxWireBytes     int64         `json:"max_wire_bytes" mapstructure:"max_wire_bytes"`
	Timeout          time.Duration `json:"timeout_ns" mapstructure:"timeout_ns"`
	CleanupTimeout   time.Duration `json:"cleanup_timeout_ns" mapstructure:"cleanup_timeout_ns"`
	MaxReadRows      int           `json:"max_read_rows" mapstructure:"max_read_rows"`
	MaxReadPages     int           `json:"max_read_pages" mapstructure:"max_read_pages"`
	MaxReadWireBytes int64         `json:"max_read_wire_bytes" mapstructure:"max_read_wire_bytes"`
	ReadTimeout      time.Duration `json:"read_timeout_ns" mapstructure:"read_timeout_ns"`
}

func options(value Settings) native.OptionsV1 {
	return native.OptionsV1{Name: value.Name,
		Version:          value.Version,
		Endpoint:         value.Endpoint,
		User:             value.User,
		Password:         value.Password,
		BearerToken:      value.BearerToken,
		RootCAPEM:        value.RootCAPEM,
		Plaintext:        value.Plaintext,
		Catalog:          value.Catalog,
		Schema:           value.Schema,
		Writes:           value.Writes,
		Maintenance:      value.Maintenance,
		MaxActive:        value.MaxActive,
		MaxSQLBytes:      value.MaxSQLBytes,
		MaxParameters:    value.MaxParameters,
		MaxRows:          value.MaxRows,
		MaxColumns:       value.MaxColumns,
		MaxPageBytes:     value.MaxPageBytes,
		MaxResultBytes:   value.MaxResultBytes,
		MaxPages:         value.MaxPages,
		MaxWireBytes:     value.MaxWireBytes,
		Timeout:          value.Timeout,
		CleanupTimeout:   value.CleanupTimeout,
		MaxReadRows:      value.MaxReadRows,
		MaxReadPages:     value.MaxReadPages,
		MaxReadWireBytes: value.MaxReadWireBytes,
		ReadTimeout:      value.ReadTimeout}
}
func settingsFor(value native.OptionsV1) Settings {
	return Settings{Name: value.Name,
		Version:          value.Version,
		Endpoint:         value.Endpoint,
		User:             value.User,
		Password:         value.Password,
		BearerToken:      value.BearerToken,
		RootCAPEM:        value.RootCAPEM,
		Plaintext:        value.Plaintext,
		Catalog:          value.Catalog,
		Schema:           value.Schema,
		Writes:           value.Writes,
		Maintenance:      value.Maintenance,
		MaxActive:        value.MaxActive,
		MaxSQLBytes:      value.MaxSQLBytes,
		MaxParameters:    value.MaxParameters,
		MaxRows:          value.MaxRows,
		MaxColumns:       value.MaxColumns,
		MaxPageBytes:     value.MaxPageBytes,
		MaxResultBytes:   value.MaxResultBytes,
		MaxPages:         value.MaxPages,
		MaxWireBytes:     value.MaxWireBytes,
		Timeout:          value.Timeout,
		CleanupTimeout:   value.CleanupTimeout,
		MaxReadRows:      value.MaxReadRows,
		MaxReadPages:     value.MaxReadPages,
		MaxReadWireBytes: value.MaxReadWireBytes,
		ReadTimeout:      value.ReadTimeout}
}

// Validate performs only pure resolved preparation, never readiness SQL.
func Validate(value Settings) error {
	_, err := native.PrepareV1(options(value))
	return translate(err, "validate")
}

// Configuration defaults once before strict typed layers. Absent fields inherit;
// explicit zero positive bounds reject instead of silently restoring defaults.
func Configuration(defaults Settings) (configsource.Schema[Settings], error) {
	prepared, err := native.PrepareV1(options(defaults))
	if err != nil {
		return configsource.Schema[Settings]{}, translate(err, "validate")
	}
	return configsource.Schema[Settings]{Version: 1, Defaults: settingsFor(prepared.Options()),
		Validate: func(_ context.Context, value Settings) error {
			_, err := native.PrepareResolvedV1(options(value))
			return translate(err, "validate")
		}}, nil
}
