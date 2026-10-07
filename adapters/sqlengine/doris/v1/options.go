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

package doris

import (
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/sqlengine/doris/v1"
)

const (
	ProviderID  = native.ProviderID
	MaxSQLBytes = native.MaxSQLBytes
	MaxColumns  = native.MaxColumns
)

// Dependencies borrows caller-owned operation admission and required evidence.
type Dependencies struct {
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Result]
	Observer *adapters.Observer
}

// Settings is strict-loadable data, not a DSN, runtime handle or trust callback.
// Durations are nanoseconds. Zero bounds select native defaults; Queued=0 disables
// queueing. Explicit endpoints/credentials/trust are never discovered. HTTPOrigins
// is copied at preparation. Formatting is redacted; explicit JSON is sensitive.
// Normalize optional nil *Settings to untyped nil before logging.
type Settings struct {
	Name                 string        `json:"name" mapstructure:"name"`
	SQLAddress           string        `json:"sql_address" mapstructure:"sql_address"`
	SQLServerName        string        `json:"sql_server_name" mapstructure:"sql_server_name"`
	HTTPOrigins          []string      `json:"http_origins" mapstructure:"http_origins"`
	Database             string        `json:"database" mapstructure:"database"`
	User                 string        `json:"user" mapstructure:"user"`
	Password             string        `json:"password" mapstructure:"password"`
	RootCAPEM            string        `json:"root_ca_pem" mapstructure:"root_ca_pem"`
	Plaintext            bool          `json:"plaintext" mapstructure:"plaintext"`
	Active               int           `json:"active" mapstructure:"active"`
	Queued               int           `json:"queued" mapstructure:"queued"`
	Timeout              time.Duration `json:"timeout_ns" mapstructure:"timeout_ns"`
	MaxBatchBytes        int           `json:"max_batch_bytes" mapstructure:"max_batch_bytes"`
	MaxRows              int           `json:"max_rows" mapstructure:"max_rows"`
	MaxResultBytes       int           `json:"max_result_bytes" mapstructure:"max_result_bytes"`
	MaxPacketBytes       int           `json:"max_packet_bytes" mapstructure:"max_packet_bytes"`
	MaxResponseBytes     int           `json:"max_response_bytes" mapstructure:"max_response_bytes"`
	MaxHTTPResponseBytes int           `json:"max_http_response_bytes" mapstructure:"max_http_response_bytes"`
	CursorTimeout        time.Duration `json:"cursor_timeout_ns" mapstructure:"cursor_timeout_ns"`
	MaxPageRows          int           `json:"max_page_rows" mapstructure:"max_page_rows"`
	MaxPageBytes         int           `json:"max_page_bytes" mapstructure:"max_page_bytes"`
	MaxCursorRows        int           `json:"max_cursor_rows" mapstructure:"max_cursor_rows"`
}

func options(value Settings) native.OptionsV1 {
	return native.OptionsV1{Name: value.Name, SQLAddress: value.SQLAddress, SQLServerName: value.SQLServerName,
		HTTPOrigins: value.HTTPOrigins, Database: value.Database, User: value.User, Password: value.Password,
		RootCAPEM: value.RootCAPEM, Plaintext: value.Plaintext, Active: value.Active, Queued: value.Queued,
		Timeout: value.Timeout, MaxBatchBytes: value.MaxBatchBytes, MaxRows: value.MaxRows, MaxResultBytes: value.MaxResultBytes,
		MaxPacketBytes: value.MaxPacketBytes, MaxResponseBytes: value.MaxResponseBytes, MaxHTTPResponseBytes: value.MaxHTTPResponseBytes,
		CursorTimeout: value.CursorTimeout, MaxPageRows: value.MaxPageRows, MaxPageBytes: value.MaxPageBytes, MaxCursorRows: value.MaxCursorRows}
}

// Validate is pure local preparation; it never probes Doris.
func Validate(value Settings) error {
	_, err := native.PrepareV1(options(value))
	return translate(err, "validate")
}
