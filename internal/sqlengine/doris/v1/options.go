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
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/resource"
)

// OptionsV1 is borrowed during Select and frozen, including slices. Addresses and
// credentials are explicit, never discovered from DSNs, environment or globals.
// SQLAddress is an IP:port. HTTPOrigins are exact scheme://host:port authorities:
// first is initial FE/BE; the remainder are authorized redirect destinations.
// TLS requires explicit roots; SQLServerName verifies SQL's certificate identity.
// Plaintext is an explicit isolated-test mode, not a TLS fallback.
type OptionsV1 struct {
	private
	Name          string
	SQLAddress    string
	SQLServerName string
	HTTPOrigins   []string
	Database      string
	User          string
	Password      string
	RootCAPEM     string
	Plaintext     bool
	// Active defaults to 4 (1–32), Queued to 0 (0–64).
	Active int
	Queued int
	// Timeout defaults to 10s (1ms–1min) for each admission/execution phase.
	// The caller deadline bounds their combined lifetime, including local I/O.
	Timeout time.Duration
	// Batch/SQL/result sizes are byte bounds, not process RSS.
	// Defaults: batch 1MiB, rows 1024, retained result 4MiB, packet 1MiB,
	// SQL response 8MiB (including authentication), HTTP response 64KiB.
	MaxBatchBytes        int
	MaxRows              int
	MaxResultBytes       int
	MaxPacketBytes       int
	MaxResponseBytes     int
	MaxHTTPResponseBytes int
}
type settings struct {
	SQLAddress           string        `json:"sql_address"`
	SQLServerName        string        `json:"sql_server_name"`
	HTTPOrigins          []string      `json:"http_origins"`
	Database             string        `json:"database"`
	User                 string        `json:"user"`
	Password             string        `json:"password"`
	RootCAPEM            string        `json:"root_ca_pem"`
	Plaintext            bool          `json:"plaintext"`
	Active               int           `json:"active"`
	Queued               int           `json:"queued"`
	Timeout              time.Duration `json:"timeout_ns"`
	MaxBatchBytes        int           `json:"max_batch_bytes"`
	MaxRows              int           `json:"max_rows"`
	MaxResultBytes       int           `json:"max_result_bytes"`
	MaxPacketBytes       int           `json:"max_packet_bytes"`
	MaxResponseBytes     int           `json:"max_response_bytes"`
	MaxHTTPResponseBytes int           `json:"max_http_response_bytes"`
}

func defaults(o OptionsV1) settings {
	s := settings{SQLAddress: o.SQLAddress, SQLServerName: o.SQLServerName,
		HTTPOrigins: append([]string(nil), o.HTTPOrigins...), Database: o.Database,
		User: o.User, Password: o.Password, RootCAPEM: o.RootCAPEM, Plaintext: o.Plaintext,
		Active: o.Active, Queued: o.Queued, Timeout: o.Timeout, MaxBatchBytes: o.MaxBatchBytes,
		MaxRows: o.MaxRows, MaxResultBytes: o.MaxResultBytes, MaxPacketBytes: o.MaxPacketBytes,
		MaxResponseBytes: o.MaxResponseBytes, MaxHTTPResponseBytes: o.MaxHTTPResponseBytes}
	if s.Active == 0 {
		s.Active = 4
	}
	if s.Timeout == 0 {
		s.Timeout = 10 * time.Second
	}
	if s.MaxBatchBytes == 0 {
		s.MaxBatchBytes = 1 << 20
	}
	if s.MaxRows == 0 {
		s.MaxRows = 1024
	}
	if s.MaxResultBytes == 0 {
		s.MaxResultBytes = 4 << 20
	}
	if s.MaxPacketBytes == 0 {
		s.MaxPacketBytes = 1 << 20
	}
	if s.MaxResponseBytes == 0 {
		s.MaxResponseBytes = 8 << 20
	}
	if s.MaxHTTPResponseBytes == 0 {
		s.MaxHTTPResponseBytes = 64 << 10
	}
	return s
}

func validText(value string, bound int, empty bool) bool {
	return (empty || value != "") && len(value) <= bound && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}
func identifier(value string, bound int) bool {
	if value == "" || len(value) > bound {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_') {
			return false
		}
	}
	return true
}
func validLabel(value string) bool {
	if len(value) > 128 || value == "" {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}
func origin(value string, plaintext bool) (*url.URL, error) {
	parsed, err := url.Parse(value)
	if err != nil || len(value) > 512 || parsed == nil {
		return nil, failure(ErrInput, "origin")
	}
	port, portErr := strconv.ParseUint(parsed.Port(), 10, 16)
	if (plaintext && parsed.Scheme != "http" || !plaintext && parsed.Scheme != "https") ||
		parsed.Hostname() == "" || portErr != nil || port == 0 || parsed.User != nil ||
		parsed.Path != "" || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.ForceQuery ||
		parsed.Fragment != "" || parsed.Opaque != "" || strings.ContainsAny(parsed.Host, "\\%") {
		return nil, failure(ErrInput, "origin")
	}
	if address, err := netip.ParseAddr(parsed.Hostname()); err == nil &&
		(address.IsUnspecified() || address.Unmap().IsUnspecified() || address.Zone() != "") {
		return nil, failure(ErrInput, "origin")
	}
	return parsed, nil
}
func validate(s settings) error {
	if s.SQLAddress == "" && len(s.HTTPOrigins) == 0 || len(s.HTTPOrigins) > 16 ||
		!identifier(s.Database, 128) || !validText(s.User, 256, false) || strings.Contains(s.User, ":") ||
		!validText(s.Password, 4096, true) || s.Active < 1 || s.Active > 32 || s.Queued < 0 || s.Queued > 64 ||
		s.Timeout < time.Millisecond || s.Timeout > time.Minute ||
		s.MaxBatchBytes < 1024 || s.MaxBatchBytes > 8<<20 || s.MaxRows < 1 || s.MaxRows > 65536 ||
		s.MaxResultBytes < 1024 || s.MaxResultBytes > 16<<20 ||
		s.MaxPacketBytes < 1024 || s.MaxPacketBytes > 4<<20 ||
		s.MaxResponseBytes < 1024 || s.MaxResponseBytes > 32<<20 ||
		s.MaxHTTPResponseBytes < 1024 || s.MaxHTTPResponseBytes > 1<<20 {
		return failure(ErrInput, "options")
	}
	if s.SQLAddress != "" {
		addr, err := netip.ParseAddrPort(s.SQLAddress)
		if err != nil || addr.Port() == 0 || addr.Addr().IsUnspecified() || addr.Addr().Unmap().IsUnspecified() || addr.Addr().Zone() != "" {
			return failure(ErrInput, "sql-address")
		}
	}
	for _, value := range s.HTTPOrigins {
		if _, err := origin(value, s.Plaintext); err != nil {
			return err
		}
	}
	if s.Plaintext {
		if s.RootCAPEM != "" || s.SQLServerName != "" {
			return failure(ErrInput, "tls")
		}
	} else {
		if s.SQLAddress != "" && !validText(s.SQLServerName, 253, false) {
			return failure(ErrInput, "sql-identity")
		}
		if _, err := s.trust(); err != nil {
			return err
		}
	}
	return nil
}
func (s settings) trust() (*tls.Config, error) {
	if s.Plaintext {
		return nil, nil
	}
	if len(s.RootCAPEM) == 0 || len(s.RootCAPEM) > 64<<10 {
		return nil, failure(ErrInput, "roots")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(s.RootCAPEM)) {
		return nil, failure(ErrInput, "roots")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, nil
}
func (s settings) reservation() int64 {
	// Both JSON decoders can grow buffers to nearly twice their input. Reserve
	// those alongside the frozen payload, raw rows/fields, keys and decode copies,
	// independently of the SQL/result limits (which can be configured smaller).
	return int64(12*s.MaxBatchBytes+4*s.MaxResponseBytes+4*s.MaxResultBytes+8*s.MaxHTTPResponseBytes+MaxSQLBytes+256<<10) + int64(s.MaxRows)*MaxColumns*64
}
func (s settings) evidenceReservation() int64 {
	return int64(s.MaxResultBytes+s.MaxPacketBytes+4*s.MaxHTTPResponseBytes+64<<10) + int64(s.MaxRows)*MaxColumns*48
}

// LimitsV1 recommends policy for defaulted options, not differently overlaid bounds.
func LimitsV1(options OptionsV1) resource.Limits {
	s := defaults(options)
	return resource.Limits{Active: s.Active, Queued: s.Queued, Bytes: int64(s.Active) * s.reservation(), QueuedBytes: int64(s.Queued) * s.reservation(), MaxLeases: 1}
}

// Profile returns copied, non-secret declarations, never inferred service support.
func (c *Client) Profile() compatibility.Profile {
	if c == nil || c.owner == nil {
		return compatibility.Profile{}
	}
	s := c.owner.settings
	return compatibility.Profile{ImplementationModule: compatibility.FrameworkModule,
		SDKMode: "mysql-text-single-use-and-http", ServiceMode: compatibility.Fact{Kind: compatibility.Declared, Value: "doris"},
		Protocol: compatibility.Fact{Kind: compatibility.Declared, Value: "mysql-41-and-stream-load-json"},
		Native:   compatibility.Fact{Kind: compatibility.NotApplicable},
		Options: []compatibility.Option{
			{Name: "plaintext", Value: strconv.FormatBool(s.Plaintext)},
			{Name: "sql-enabled", Value: strconv.FormatBool(s.SQLAddress != "")},
			{Name: "stream-load-enabled", Value: strconv.FormatBool(len(s.HTTPOrigins) != 0)},
			{Name: "automatic-mutation-retries", Value: "disabled"},
			{Name: "authentication", Value: "mysql-native-password-and-http-basic"},
			{Name: "sql-pooling", Value: "disabled"},
			{Name: "active", Value: strconv.Itoa(s.Active)}, {Name: "queued", Value: strconv.Itoa(s.Queued)},
			{Name: "timeout-ns", Value: strconv.FormatInt(int64(s.Timeout), 10)},
			{Name: "max-batch-bytes", Value: strconv.Itoa(s.MaxBatchBytes)}, {Name: "max-rows", Value: strconv.Itoa(s.MaxRows)},
			{Name: "max-result-bytes", Value: strconv.Itoa(s.MaxResultBytes)}, {Name: "max-packet-bytes", Value: strconv.Itoa(s.MaxPacketBytes)},
			{Name: "max-response-bytes", Value: strconv.Itoa(s.MaxResponseBytes)}, {Name: "max-http-response-bytes", Value: strconv.Itoa(s.MaxHTTPResponseBytes)},
		}}
}

func (s settings) dialer() *net.Dialer { return &net.Dialer{Timeout: s.Timeout} }
