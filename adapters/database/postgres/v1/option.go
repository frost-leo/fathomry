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

package postgres

import (
	"time"

	"github.com/frost-leo/fathomry/adapters/database/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/database/pgx/v5"
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
	MaxSavepoints         = native.MaxSavepoints
	familyRecords         = MaxPreparedStatements + MaxSavepoints + 2
)

// Settings is loadable data covering the supported native profile, not a DSN
// or runtime handle. Credentials and trust are explicit. Durations are integer
// nanoseconds; zero selects native defaults, except zero idle/lifetime limits
// disable expiration and zero QueuedCalls disables queueing.
// ParserHome explicitly authorizes the native parser's HOME metadata probes.
// Validate checks settings, not service readiness; use Client.Ping separately.
// Values and non-nil pointers are automatically redacted when logged directly.
// Do not pass a typed-nil *Settings to slog: normalize it to untyped nil or omit
// the attribute. Explicit JSON encoding remains ordinary configuration data.
type Settings struct {
	Name            string        `json:"name" mapstructure:"name"`
	Address         string        `json:"address" mapstructure:"address"`
	Port            uint16        `json:"port" mapstructure:"port"`
	Database        string        `json:"database" mapstructure:"database"`
	User            string        `json:"user" mapstructure:"user"`
	Password        string        `json:"password" mapstructure:"password"`
	RootCAPEM       string        `json:"root_ca_pem" mapstructure:"root_ca_pem"`
	ServerName      string        `json:"server_name" mapstructure:"server_name"`
	Plaintext       bool          `json:"plaintext" mapstructure:"plaintext"`
	MaxConnections  int           `json:"max_connections" mapstructure:"max_connections"`
	MaxIdleTime     time.Duration `json:"max_idle_time_ns" mapstructure:"max_idle_time_ns"`
	MaxLifetime     time.Duration `json:"max_lifetime_ns" mapstructure:"max_lifetime_ns"`
	QueuedCalls     int           `json:"queued_calls" mapstructure:"queued_calls"`
	Timeout         time.Duration `json:"timeout_ns" mapstructure:"timeout_ns"`
	CloseTimeout    time.Duration `json:"close_timeout_ns" mapstructure:"close_timeout_ns"`
	MaxRows         int           `json:"max_rows" mapstructure:"max_rows"`
	MaxResultBytes  int           `json:"max_result_bytes" mapstructure:"max_result_bytes"`
	ParserHome      string        `json:"parser_home" mapstructure:"parser_home"`
	MaxMessageBytes int           `json:"max_message_bytes" mapstructure:"max_message_bytes"`
}

func options(value Settings) native.OptionsV1 {
	return native.OptionsV1{
		Name:            value.Name,
		Address:         value.Address,
		Port:            value.Port,
		Database:        value.Database,
		User:            value.User,
		Password:        value.Password,
		RootCAPEM:       value.RootCAPEM,
		ServerName:      value.ServerName,
		Plaintext:       value.Plaintext,
		MaxConnections:  value.MaxConnections,
		MaxIdleTime:     value.MaxIdleTime,
		MaxLifetime:     value.MaxLifetime,
		QueuedCalls:     value.QueuedCalls,
		Timeout:         value.Timeout,
		CloseTimeout:    value.CloseTimeout,
		MaxRows:         value.MaxRows,
		MaxResultBytes:  value.MaxResultBytes,
		ParserHome:      value.ParserHome,
		MaxMessageBytes: value.MaxMessageBytes,
	}
}

// Validate freezes and checks settings without pool construction or service I/O.
func Validate(value Settings) error {
	_, err := native.Select(options(value))
	return translate(err, "validate")
}

// Recommend validates settings and derives budgets from the supported native
// reservation model. The extra 64 KiB covers bounded public attribution.
func Recommend(value Settings) (database.Policy, error) {
	if err := Validate(value); err != nil {
		return database.Policy{}, err
	}
	limits := native.LimitsV1(options(value))
	rows, result, wire := value.MaxRows, value.MaxResultBytes, value.MaxMessageBytes
	if rows == 0 {
		rows = 1024
	}
	if result == 0 {
		result = 4 << 20
	}
	if wire == 0 {
		wire = 1 << 20
	}
	evidence := int64(result+2*wire) + int64(rows)*(MaxColumns*24+48) + 64<<10
	budget := database.Budget{WorkBytes: limits.Bytes/int64(limits.Active) + 64<<10, EvidenceBytes: evidence + 64<<10}
	capacity := 1 + limits.Active*familyRecords + limits.Queued
	return database.Policy{
		Budget: budget,
		Runtime: adapters.Options{MaxActive: 1 + limits.Active, MaxQueued: limits.Queued,
			MaxWorkBytes:   sourceWorkBytes + int64(limits.Active)*budget.WorkBytes,
			MaxQueuedBytes: int64(limits.Queued) * budget.WorkBytes},
		Evidence: adapters.EvidenceOptions{Capacity: capacity, MaxBytes: sourceEvidenceBytes + int64(capacity-1)*budget.EvidenceBytes},
	}, nil
}

// Isolation selects one supported PostgreSQL isolation level.
type Isolation string

const (
	ReadCommitted  Isolation = "read committed"
	RepeatableRead Isolation = "repeatable read"
	Serializable   Isolation = "serializable"
)

// Access selects explicit PostgreSQL transaction access.
type Access string

const (
	ReadOnly  Access = "read only"
	ReadWrite Access = "read write"
)

// TxOptions requires explicit isolation/access. Deferrable requires serializable
// read-only. It accepts no custom BEGIN/COMMIT SQL or retry policy.
type TxOptions struct {
	Isolation  Isolation
	Access     Access
	Deferrable bool
}
