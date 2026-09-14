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
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

// OptionsV1 is borrowed only during Select. Version zero selects format 1.
// Endpoint is an origin without credentials, path, query or fragment. Credentials
// require HTTPS; Plaintext explicitly allows unauthenticated trusted-network HTTP.
// RootCAPEM replaces system roots when supplied. No ambient proxy or SDK DSN is used.
// Writes and Maintenance are coarse capability gates, NOT a SQL authorization
// sandbox. Composition must supply authorized SQL and server-side privileges.
type OptionsV1 struct {
	private
	Name           string
	Version        uint32
	Endpoint       string
	User           string
	Password       string
	BearerToken    string
	RootCAPEM      string
	Plaintext      bool
	Catalog        string
	Schema         string
	Writes         bool
	Maintenance    bool
	MaxActive      int
	MaxSQLBytes    int
	MaxParameters  int
	MaxRows        int
	MaxColumns     int
	MaxPageBytes   int
	MaxResultBytes int
	MaxPages       int
	MaxWireBytes   int64
	Timeout        time.Duration
	CleanupTimeout time.Duration
}

type settings struct {
	Endpoint       string        `json:"endpoint"`
	User           string        `json:"user"`
	Password       string        `json:"password"`
	BearerToken    string        `json:"bearer_token"`
	RootCAPEM      string        `json:"root_ca_pem"`
	Plaintext      bool          `json:"plaintext"`
	Catalog        string        `json:"catalog"`
	Schema         string        `json:"schema"`
	Writes         bool          `json:"writes"`
	Maintenance    bool          `json:"maintenance"`
	MaxActive      int           `json:"max_active"`
	MaxSQLBytes    int           `json:"max_sql_bytes"`
	MaxParameters  int           `json:"max_parameters"`
	MaxRows        int           `json:"max_rows"`
	MaxColumns     int           `json:"max_columns"`
	MaxPageBytes   int           `json:"max_page_bytes"`
	MaxResultBytes int           `json:"max_result_bytes"`
	MaxPages       int           `json:"max_pages"`
	MaxWireBytes   int64         `json:"max_wire_bytes"`
	Timeout        time.Duration `json:"timeout_ns"`
	CleanupTimeout time.Duration `json:"cleanup_timeout_ns"`
}

func defaults(o OptionsV1) settings {
	s := settings{Endpoint: o.Endpoint, User: o.User, Password: o.Password, BearerToken: o.BearerToken,
		RootCAPEM: o.RootCAPEM, Plaintext: o.Plaintext, Catalog: o.Catalog, Schema: o.Schema,
		Writes: o.Writes, Maintenance: o.Maintenance, MaxActive: o.MaxActive, MaxSQLBytes: o.MaxSQLBytes,
		MaxParameters: o.MaxParameters, MaxRows: o.MaxRows, MaxColumns: o.MaxColumns,
		MaxPageBytes: o.MaxPageBytes, MaxResultBytes: o.MaxResultBytes, MaxPages: o.MaxPages,
		MaxWireBytes: o.MaxWireBytes, Timeout: o.Timeout, CleanupTimeout: o.CleanupTimeout}
	if s.MaxActive == 0 {
		s.MaxActive = 2
	}
	if s.MaxSQLBytes == 0 {
		s.MaxSQLBytes = 1 << 20
	}
	if s.MaxParameters == 0 {
		s.MaxParameters = 65536
	}
	if s.MaxRows == 0 {
		s.MaxRows = 65536
	}
	if s.MaxColumns == 0 {
		s.MaxColumns = 256
	}
	if s.MaxPageBytes == 0 {
		s.MaxPageBytes = 1 << 20
	}
	if s.MaxResultBytes == 0 {
		s.MaxResultBytes = 8 << 20
	}
	if s.MaxPages == 0 {
		s.MaxPages = 1024
	}
	if s.MaxWireBytes == 0 {
		s.MaxWireBytes = 64 << 20
	}
	if s.Timeout == 0 {
		s.Timeout = time.Minute
	}
	if s.CleanupTimeout == 0 {
		s.CleanupTimeout = 5 * time.Second
	}
	return s
}

func identifier(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' ||
			char >= '0' && char <= '9' || char == '_') {
			return false
		}
	}
	return true
}
func ascii(value string, limit int, empty bool) bool {
	if len(value) > limit || !empty && value == "" {
		return false
	}
	for _, char := range value {
		if char < 32 || char > 126 {
			return false
		}
	}
	return true
}
func origin(value string, plaintext bool) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil || len(value) > 1024 || u == nil || u.Hostname() == "" ||
		u.User != nil || u.Path != "" || u.RawPath != "" || u.RawQuery != "" ||
		u.ForceQuery || u.Fragment != "" || u.Opaque != "" || u.String() != value ||
		u.Scheme != "https" && !(plaintext && u.Scheme == "http") ||
		strings.ContainsAny(u.Host, "%\\ \t\r\n") {
		return nil, failure(ErrInput, "endpoint")
	}
	host := u.Hostname()
	if net.ParseIP(host) == nil {
		if len(host) > 253 || strings.Contains(host, "..") || strings.HasPrefix(host, ".") || strings.HasSuffix(host, ".") {
			return nil, failure(ErrInput, "endpoint")
		}
		for _, char := range host {
			if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '.' || char == '-') {
				return nil, failure(ErrInput, "endpoint")
			}
		}
	}
	if strings.HasSuffix(u.Host, ":") {
		return nil, failure(ErrInput, "endpoint")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil || n == 0 || strconv.FormatUint(n, 10) != port {
			return nil, failure(ErrInput, "endpoint")
		}
	}
	return u, nil
}
func roots(value string) (*x509.CertPool, error) {
	if value == "" {
		return nil, nil
	}
	if len(value) > 64<<10 {
		return nil, failure(ErrInput, "trust")
	}
	pool := x509.NewCertPool()
	remaining := bytes.TrimSpace([]byte(value))
	if len(remaining) == 0 {
		return nil, failure(ErrInput, "trust")
	}
	for len(remaining) != 0 {
		if !bytes.HasPrefix(remaining, []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, failure(ErrInput, "trust")
		}
		block, rest := pem.Decode(remaining)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, failure(ErrInput, "trust")
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, failure(ErrInput, "trust", err)
		}
		pool.AddCert(cert)
		remaining = bytes.TrimSpace(rest)
	}
	return pool, nil
}
func validate(s settings) error {
	u, err := origin(s.Endpoint, s.Plaintext)
	if err != nil {
		return err
	}
	if !ascii(s.User, 128, false) || strings.ContainsAny(s.User, ":,= ") ||
		!ascii(s.Password, 4096, true) || !ascii(s.BearerToken, 8192, true) ||
		s.Password != "" && s.BearerToken != "" || strings.Contains(s.BearerToken, " ") ||
		u.Scheme == "http" && (s.Password != "" || s.BearerToken != "" || s.RootCAPEM != "") ||
		s.Catalog != "" && !identifier(s.Catalog) || s.Schema != "" && !identifier(s.Schema) ||
		s.Schema != "" && s.Catalog == "" || s.Maintenance && !s.Writes {
		return failure(ErrInput, "source")
	}
	if _, err = roots(s.RootCAPEM); err != nil {
		return err
	}
	if s.MaxActive < 1 || s.MaxActive > 8 ||
		s.MaxSQLBytes < 128 || s.MaxSQLBytes > 4<<20 || s.MaxParameters < 1 || s.MaxParameters > 65536 ||
		s.MaxRows < 1 || s.MaxRows > 1<<20 || s.MaxColumns < 1 || s.MaxColumns > 1024 ||
		s.MaxPageBytes < 128 || s.MaxPageBytes > 8<<20 || s.MaxResultBytes < 128 || s.MaxResultBytes > 32<<20 ||
		s.MaxPages < 1 || s.MaxPages > 4096 || s.MaxWireBytes < int64(s.MaxPageBytes) || s.MaxWireBytes > 256<<20 ||
		s.Timeout < time.Millisecond || s.Timeout > 30*time.Minute ||
		s.CleanupTimeout < time.Millisecond || s.CleanupTimeout > time.Minute {
		return failure(ErrInput, "limits")
	}
	return nil
}
func (s settings) reservation() int64 {
	return int64(64*s.MaxPageBytes + 3*s.MaxResultBytes + 16*s.MaxSQLBytes + 65536)
}
func (s settings) evidenceBytes() int64 {
	return int64(s.MaxResultBytes + 8*s.MaxPageBytes + 65536)
}

// LimitsV1 supplies zero-queue admission for the defaulted options. These are
// declared working envelopes, not an RSS guarantee. Overlays require matching
// effective limits; aliases inherit one authoritative resource's allowance.
func LimitsV1(options OptionsV1) resource.Limits {
	s := defaults(options)
	return resource.Limits{Active: s.MaxActive, Bytes: int64(s.MaxActive) * s.reservation(), MaxLeases: 1}
}
