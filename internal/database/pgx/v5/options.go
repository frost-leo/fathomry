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

package pgx

import (
	"crypto/tls"
	"crypto/x509"
	"net/netip"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/jackc/pgx/v5"
	sdkpgconn "github.com/jackc/pgx/v5/pgconn"
)

// OptionsV1 is the first process-local bootstrap contract. Select borrows it
// during preparation only. Layers may override its settings using the documented
// JSON field names; Name is the source identity, not an overridable setting.
// No SDK object, DSN, resolver, codec, tracer or caller callback is accepted.
type OptionsV1 struct {
	private
	Name string
	// Address is one non-unspecified literal IP; DNS, sockets and failover are not supported.
	Address string
	// Port must be explicit and nonzero.
	Port uint16
	// Database, User and Password must be nonempty UTF-8 without NUL, bounded
	// respectively to 256, 256 and 4096 bytes. Credentials are never discovered.
	Database string
	User     string
	Password string
	// RootCAPEM supplies the complete explicit trust set (maximum 64 KiB).
	// ServerName is required with TLS; verification cannot be disabled.
	RootCAPEM  string
	ServerName string
	// Plaintext explicitly permits unencrypted connections for isolated testing.
	Plaintext bool
	// ParserHome must equal HOME (empty if unset) on supported Unix systems.
	// It authorizes ONLY the native parser's metadata probes of the home
	// .postgresql certificate paths and standard socket directories. No contents
	// or settings from those paths are used. PG-prefixed environment variables
	// must be absent or empty; process environment must stay static during Select
	// and Assemble. The package never changes it.
	ParserHome string
	// MaxConnections defaults to 4, range 1–32. Connection establishment is
	// serialized; already-open connections execute concurrently.
	MaxConnections int
	// Idle/lifetime expiration defaults off. Positive values are 1 ms–24 h;
	// idle maintenance runs once per second and never interrupts held resources.
	MaxIdleTime time.Duration
	MaxLifetime time.Duration
	// QueuedCalls defaults to 0 (reject overload), range 0–64.
	QueuedCalls int
	// Timeout applies separately to admission and execution. Execution includes
	// connection acquisition and result consumption. Default 10 s; range 1 ms–1 min.
	Timeout time.Duration
	// CloseTimeout is the native graceful-close budget, default 5 s, range
	// 1 ms–30 s. It does not override pgconn's asynchronous cleanup lifetime.
	CloseTimeout time.Duration
	// MaxRows defaults to 1024, range 1–65536; MaxResultBytes defaults to 4 MiB,
	// range 1 KiB–16 MiB. These bound retained raw cells and column names, not RSS.
	MaxRows        int
	MaxResultBytes int
	// MaxMessageBytes bounds each native protocol message body. Default 1 MiB,
	// range 1 KiB–4 MiB. It does not bound total response traffic or native heap.
	MaxMessageBytes int
}

type settings struct {
	Address         string        `json:"address"`
	Port            uint16        `json:"port"`
	Database        string        `json:"database"`
	User            string        `json:"user"`
	Password        string        `json:"password"`
	RootCAPEM       string        `json:"root_ca_pem"`
	ServerName      string        `json:"server_name"`
	Plaintext       bool          `json:"plaintext"`
	ParserHome      string        `json:"parser_home"`
	MaxConnections  int           `json:"max_connections"`
	MaxIdleTime     time.Duration `json:"max_idle_time_ns"`
	MaxLifetime     time.Duration `json:"max_lifetime_ns"`
	QueuedCalls     int           `json:"queued_calls"`
	Timeout         time.Duration `json:"timeout_ns"`
	CloseTimeout    time.Duration `json:"close_timeout_ns"`
	MaxRows         int           `json:"max_rows"`
	MaxResultBytes  int           `json:"max_result_bytes"`
	MaxMessageBytes int           `json:"max_message_bytes"`
}

func defaults(input OptionsV1) settings {
	value := settings{input.Address, input.Port, input.Database, input.User, input.Password,
		input.RootCAPEM, input.ServerName, input.Plaintext, input.ParserHome,
		input.MaxConnections, input.MaxIdleTime, input.MaxLifetime, input.QueuedCalls, input.Timeout, input.CloseTimeout,
		input.MaxRows, input.MaxResultBytes, input.MaxMessageBytes}
	if value.MaxConnections == 0 {
		value.MaxConnections = 4
	}
	if value.Timeout == 0 {
		value.Timeout = 10 * time.Second
	}
	if value.CloseTimeout == 0 {
		value.CloseTimeout = 5 * time.Second
	}
	if value.MaxRows == 0 {
		value.MaxRows = 1024
	}
	if value.MaxResultBytes == 0 {
		value.MaxResultBytes = 4 << 20
	}
	if value.MaxMessageBytes == 0 {
		value.MaxMessageBytes = 1 << 20
	}
	return value
}

func validText(value string, maximum int, empty bool) bool {
	return (empty || value != "") && len(value) <= maximum &&
		utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func validate(value settings) error {
	address, err := netip.ParseAddr(value.Address)
	if err != nil || address.Zone() != "" || address.Unmap().IsUnspecified() || value.Port == 0 ||
		!validText(value.Database, 256, false) || !validText(value.User, 256, false) ||
		!validText(value.Password, 4096, false) || !validText(value.ParserHome, 4096, true) ||
		value.MaxConnections < 1 || value.MaxConnections > 32 || value.QueuedCalls < 0 || value.QueuedCalls > 64 ||
		value.Timeout < time.Millisecond || value.Timeout > time.Minute ||
		value.CloseTimeout < time.Millisecond || value.CloseTimeout > 30*time.Second ||
		value.MaxRows < 1 || value.MaxRows > 65536 || value.MaxResultBytes < 1024 || value.MaxResultBytes > 16<<20 ||
		value.MaxMessageBytes < 1024 || value.MaxMessageBytes > 4<<20 {
		return failure(ErrInput, "options")
	}
	for _, duration := range []time.Duration{value.MaxIdleTime, value.MaxLifetime} {
		if duration < 0 || duration > 24*time.Hour || duration > 0 && duration < time.Millisecond {
			return failure(ErrInput, "expiration")
		}
	}
	_, err = tlsConfig(value)
	return err
}

func tlsConfig(value settings) (*tls.Config, error) {
	if value.Plaintext {
		if value.RootCAPEM != "" || value.ServerName != "" {
			return nil, failure(ErrInput, "tls")
		}
		return nil, nil
	}
	if !validText(value.ServerName, 256, false) || len(value.RootCAPEM) == 0 || len(value.RootCAPEM) > 64<<10 {
		return nil, failure(ErrInput, "tls")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(value.RootCAPEM)) {
		return nil, failure(ErrInput, "tls")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: value.ServerName}, nil
}

func parserEnvironment(home string) error {
	if runtime.GOOS == "windows" || runtime.GOOS == "plan9" {
		return failure(ErrUnsupported, "parser-platform")
	}
	actual, err := os.UserHomeDir()
	if err != nil {
		actual = ""
	}
	if actual != home {
		return failure(ErrEnvironment, "parser-home")
	}
	for _, entry := range os.Environ() {
		name, value, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "PG") && value != "" {
			return failure(ErrEnvironment, "parser-environment")
		}
	}
	return nil
}

// The socket-shaped seed bypasses configTLS entirely: even sslmode=disable
// reads sslrootcert BEFORE branching in pgx v5.11.0. Explicit user/password avoid
// account/password-file lookup. This seed is never used to make a connection.
const parserSeed = "host=/fathomry-parser-only port=5432 user=fathomry-parser password=unused dbname=fathomry-parser sslmode=disable target_session_attrs=any default_query_exec_mode=exec statement_cache_capacity=0 description_cache_capacity=0"

func nativeConfig(value settings) (*sdk.ConnConfig, error) {
	if err := parserEnvironment(value.ParserHome); err != nil {
		return nil, err
	}
	config, err := sdk.ParseConfig(parserSeed)
	if err != nil {
		return nil, failure(ErrInput, "parse", err)
	}
	trust, err := tlsConfig(value)
	if err != nil {
		return nil, err
	}
	address, _ := netip.ParseAddr(value.Address)
	config.Host, config.Port, config.Database = address.Unmap().String(), value.Port, value.Database
	config.User, config.Password, config.TLSConfig = value.User, value.Password, trust
	config.Fallbacks = nil
	config.ConnectTimeout = value.Timeout
	config.RuntimeParams = map[string]string{"application_name": "fathomry", "client_encoding": "UTF8", "DateStyle": "ISO", "TimeZone": "UTC"}
	config.MinProtocolVersion, config.MaxProtocolVersion = "3.0", "3.0"
	config.RequireAuth = "scram-sha-256"
	config.ChannelBinding = "prefer"
	config.MaxProtocolMessageBodyLen = value.MaxMessageBytes
	config.BuildFrontend = boundedFrontend
	// A non-nil discard handler prevents pgx's unbounded notification buffer.
	config.OnNotification = func(*sdkpgconn.PgConn, *sdkpgconn.Notification) {}
	return config, nil
}

func (value settings) reservation() int64 {
	// Cell arrays plus simultaneous old/new row-list storage during growth.
	// Fixed overhead covers allocator rounding and bounded metadata, not RSS.
	return int64(MaxSQLBytes+MaxArgumentBytes+value.MaxMessageBytes+value.MaxResultBytes) +
		int64(MaxPreparedStatements)*(int64(MaxSQLBytes+value.MaxMessageBytes)+4<<20) +
		int64(value.MaxRows)*(MaxColumns*24+4*24) + 64<<10
}
func (value settings) evidenceReservation() int64 {
	return int64(value.MaxResultBytes+2*value.MaxMessageBytes) +
		int64(value.MaxRows)*(MaxColumns*24+2*24) + 64<<10
}
func (value settings) limits() resource.Limits {
	return resource.Limits{Active: value.MaxConnections, Queued: value.QueuedCalls,
		Bytes: int64(value.MaxConnections) * value.reservation(), QueuedBytes: int64(value.QueuedCalls) * value.reservation(), MaxLeases: MaxPreparedStatements + MaxSavepoints + 2}
}

// Profile returns fresh, non-sensitive effective options from the same prepared
// settings that constructed this source. It makes no service-support claim.
func (database *Database) Profile() compatibility.Profile {
	if database == nil || database.owner == nil {
		return compatibility.Profile{}
	}
	value := database.owner.settings
	declared := func(value string) compatibility.Fact {
		return compatibility.Fact{Kind: compatibility.Declared, Value: value}
	}
	options := []compatibility.Option{
		{Name: "query-mode", Value: "exec"}, {Name: "statement-cache", Value: "0"}, {Name: "description-cache", Value: "0"},
		{Name: "authentication", Value: "scram-sha-256"}, {Name: "channel-binding", Value: "prefer"},
		{Name: "plaintext", Value: strconv.FormatBool(value.Plaintext)},
		{Name: "max-connections", Value: strconv.Itoa(value.MaxConnections)},
		{Name: "max-idle-time-ns", Value: strconv.FormatInt(int64(value.MaxIdleTime), 10)},
		{Name: "max-lifetime-ns", Value: strconv.FormatInt(int64(value.MaxLifetime), 10)},
		{Name: "session-return", Value: "discard-all"},
		{Name: "queued-calls", Value: strconv.Itoa(value.QueuedCalls)},
		{Name: "timeout-ns", Value: strconv.FormatInt(int64(value.Timeout), 10)},
		{Name: "close-timeout-ns", Value: strconv.FormatInt(int64(value.CloseTimeout), 10)},
		{Name: "max-rows", Value: strconv.Itoa(value.MaxRows)},
		{Name: "max-result-bytes", Value: strconv.Itoa(value.MaxResultBytes)},
		{Name: "max-message-bytes", Value: strconv.Itoa(value.MaxMessageBytes)},
	}
	return compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "pgx-puddle-synchronous",
		ServiceMode: declared("single-server"), Protocol: declared("postgresql-3.0"), Native: compatibility.Fact{Kind: compatibility.NotApplicable}, Options: options}
}
