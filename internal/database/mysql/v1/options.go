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
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"net"
	"net/netip"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/go-sql-driver/mysql"
)

// OptionsV1 is explicit process-local bootstrap, borrowed only during Select.
// TLS uses explicit PEM roots and server identity, without DSN or global registries.
// Plaintext is an explicit isolated-test mode. No caller callbacks/native config escape.
type OptionsV1 struct {
	private
	Name string
	// Network defaults to tcp; unix requires an absolute socket Address and Port=0.
	Network    string
	Address    string
	Port       uint16
	Database   string
	User       string
	Password   string
	RootCAPEM  string
	ServerName string
	// ServerCertificateSHA256 is an alternative to a DNS/IP ServerName, not
	// an option to trust any certificate. The exact leaf AND chain are verified.
	ServerCertificateSHA256 string
	Plaintext               bool
	ParseTime               bool
	ClientFoundRows         bool
	ColumnsWithAlias        bool
	// Authentication defaults to caching_sha2_password only. Selecting
	// mysql_native_password additionally permits that legacy greeting/auth switch;
	// it does not require the server's initial default plugin to match the account.
	Authentication string
	// MaxConnections defaults to 4 (1–32); QueuedCalls defaults to 0 (0–64).
	MaxConnections int
	// MaxIdleConnections defaults to MaxConnections; -1 disables idle reuse.
	// Idle/lifetime expiration defaults off; positive values are at most 24h.
	MaxIdleConnections int
	MaxIdleTime        time.Duration
	MaxLifetime        time.Duration
	QueuedCalls        int
	// Timeout defaults to 10s; CloseTimeout to 5s; TransactionTimeout to 1min.
	// All are finite nanosecond durations, range 1ms–1min.
	Timeout            time.Duration
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	CloseTimeout       time.Duration
	TransactionTimeout time.Duration
	// MaxRows defaults to 1024 (1–65536). MaxResultBytes defaults to 4MiB
	// (1KiB–16MiB); it counts retained cell bytes and column names, not RSS.
	MaxRows        int
	MaxResultBytes int
	// MaxPacketBytes defaults to 1MiB (1KiB–4MiB), enforced BEFORE a MySQL
	// packet header reaches native allocation, on plaintext and owned TLS paths.
	MaxPacketBytes int
	// MaxResponseBytes defaults to 8MiB (1KiB–32MiB) per native command,
	// including packet headers/metadata/drain. Oversize retires the connection.
	MaxResponseBytes int
}
type settings struct {
	Network                 string        `json:"network"`
	Address                 string        `json:"address"`
	Port                    uint16        `json:"port"`
	Database                string        `json:"database"`
	User                    string        `json:"user"`
	Password                string        `json:"password"`
	RootCAPEM               string        `json:"root_ca_pem"`
	ServerName              string        `json:"server_name"`
	ServerCertificateSHA256 string        `json:"server_certificate_sha256"`
	Plaintext               bool          `json:"plaintext"`
	ParseTime               bool          `json:"parse_time"`
	ClientFoundRows         bool          `json:"client_found_rows"`
	ColumnsWithAlias        bool          `json:"columns_with_alias"`
	Authentication          string        `json:"authentication"`
	MaxConnections          int           `json:"max_connections"`
	MaxIdleConnections      int           `json:"max_idle_connections"`
	MaxIdleTime             time.Duration `json:"max_idle_time_ns"`
	MaxLifetime             time.Duration `json:"max_lifetime_ns"`
	QueuedCalls             int           `json:"queued_calls"`
	Timeout                 time.Duration `json:"timeout_ns"`
	ReadTimeout             time.Duration `json:"read_timeout_ns"`
	WriteTimeout            time.Duration `json:"write_timeout_ns"`
	CloseTimeout            time.Duration `json:"close_timeout_ns"`
	TransactionTimeout      time.Duration `json:"transaction_timeout_ns"`
	MaxRows                 int           `json:"max_rows"`
	MaxResultBytes          int           `json:"max_result_bytes"`
	MaxPacketBytes          int           `json:"max_packet_bytes"`
	MaxResponseBytes        int           `json:"max_response_bytes"`
}

func defaults(v OptionsV1) settings {
	s := settings{Network: v.Network, Address: v.Address, Port: v.Port, Database: v.Database, User: v.User, Password: v.Password,
		RootCAPEM: v.RootCAPEM, ServerName: v.ServerName, ServerCertificateSHA256: v.ServerCertificateSHA256, Plaintext: v.Plaintext,
		Authentication: v.Authentication, ParseTime: v.ParseTime, ClientFoundRows: v.ClientFoundRows, ColumnsWithAlias: v.ColumnsWithAlias,
		MaxConnections: v.MaxConnections, MaxIdleConnections: v.MaxIdleConnections, MaxIdleTime: v.MaxIdleTime, MaxLifetime: v.MaxLifetime,
		QueuedCalls: v.QueuedCalls, Timeout: v.Timeout, ReadTimeout: v.ReadTimeout, WriteTimeout: v.WriteTimeout, CloseTimeout: v.CloseTimeout,
		TransactionTimeout: v.TransactionTimeout, MaxRows: v.MaxRows, MaxResultBytes: v.MaxResultBytes, MaxPacketBytes: v.MaxPacketBytes, MaxResponseBytes: v.MaxResponseBytes}
	if s.Network == "" {
		s.Network = "tcp"
	}
	if s.Authentication == "" {
		s.Authentication = "caching_sha2_password"
	}
	if s.MaxConnections == 0 {
		s.MaxConnections = 4
	}
	if s.Timeout == 0 {
		s.Timeout = 10 * time.Second
	}
	if s.ReadTimeout == 0 {
		s.ReadTimeout = s.Timeout
	}
	if s.WriteTimeout == 0 {
		s.WriteTimeout = s.Timeout
	}
	if s.MaxIdleConnections == 0 {
		s.MaxIdleConnections = s.MaxConnections
	}
	if s.CloseTimeout == 0 {
		s.CloseTimeout = 5 * time.Second
	}
	if s.TransactionTimeout == 0 {
		s.TransactionTimeout = time.Minute
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
	return s
}
func textValid(s string, max int, empty bool) bool {
	return (empty || s != "") && len(s) <= max && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
func validate(s settings) error {
	ip, err := netip.ParseAddr(s.Address)
	validAddress := s.Network == "tcp" && err == nil && !ip.IsUnspecified() && !ip.Unmap().IsUnspecified() && ip.Zone() == "" && s.Port != 0 ||
		s.Network == "unix" && filepath.IsAbs(s.Address) && textValid(s.Address, 4096, false) && s.Port == 0
	if !validAddress ||
		!textValid(s.Database, 256, true) || !textValid(s.User, 256, false) || !textValid(s.Password, 4096, true) ||
		s.MaxConnections < 1 || s.MaxConnections > 32 || s.QueuedCalls < 0 || s.QueuedCalls > 64 ||
		s.MaxIdleConnections < -1 || s.MaxIdleConnections > s.MaxConnections || s.MaxIdleTime < 0 || s.MaxIdleTime > 24*time.Hour || s.MaxLifetime < 0 || s.MaxLifetime > 24*time.Hour ||
		s.MaxRows < 1 || s.MaxRows > 65536 || s.MaxResultBytes < 1024 || s.MaxResultBytes > 16<<20 ||
		s.MaxPacketBytes < 1024 || s.MaxPacketBytes > 4<<20 || s.MaxResponseBytes < 1024 || s.MaxResponseBytes > 32<<20 {
		return failure(ErrInput, "options")
	}
	for _, d := range []time.Duration{s.Timeout, s.ReadTimeout, s.WriteTimeout, s.CloseTimeout, s.TransactionTimeout} {
		if d < time.Millisecond || d > time.Minute {
			return failure(ErrInput, "duration")
		}
	}
	if s.Authentication != "caching_sha2_password" && s.Authentication != "mysql_native_password" {
		return failure(ErrUnsupported, "authentication")
	}
	_, err = trust(s)
	return err
}
func trust(s settings) (*tls.Config, error) {
	if s.Plaintext {
		if s.RootCAPEM != "" || s.ServerName != "" || s.ServerCertificateSHA256 != "" {
			return nil, failure(ErrInput, "tls")
		}
		return nil, nil
	}
	if len(s.RootCAPEM) == 0 || len(s.RootCAPEM) > 64<<10 || (s.ServerName == "") == (s.ServerCertificateSHA256 == "") || !textValid(s.ServerName, 256, true) {
		return nil, failure(ErrInput, "tls")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(s.RootCAPEM)) {
		return nil, failure(ErrInput, "roots")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: s.ServerName}
	if s.ServerCertificateSHA256 != "" {
		pin, err := hex.DecodeString(s.ServerCertificateSHA256)
		if err != nil || len(pin) != sha256.Size {
			return nil, failure(ErrInput, "certificate-pin")
		}
		// Pin-only identities have no DNS name to pass to the standard verifier.
		// Replace that check with BOTH chain verification and an exact leaf pin.
		config.InsecureSkipVerify = true
		config.VerifyConnection = func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return errors.New("mysql: missing peer certificate")
			}
			leaf := state.PeerCertificates[0]
			digest := sha256.Sum256(leaf.Raw)
			if string(digest[:]) != string(pin) {
				return errors.New("mysql: server certificate pin mismatch")
			}
			intermediates := x509.NewCertPool()
			for _, cert := range state.PeerCertificates[1:] {
				intermediates.AddCert(cert)
			}
			_, err := leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
			return err
		}
	}
	return config, nil
}
func nativeConfig(s settings) *sdk.Config {
	cfg := sdk.NewConfig()
	cfg.User, cfg.Passwd, cfg.DBName = s.User, s.Password, s.Database
	cfg.Net, cfg.Addr = s.Network, s.address()
	cfg.Logger = &sdk.NopLogger{}
	cfg.AllowNativePasswords = s.Authentication == "mysql_native_password"
	cfg.CheckConnLiveness = false
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = s.Timeout, s.ReadTimeout, s.WriteTimeout
	cfg.MaxAllowedPacket = 2 << 20
	cfg.ParseTime, cfg.ClientFoundRows, cfg.ColumnsWithAlias = s.ParseTime, s.ClientFoundRows, s.ColumnsWithAlias
	cfg.Params = map[string]string{"autocommit": "1", "time_zone": "'+00:00'"}
	_ = cfg.Apply(sdk.Charset("utf8mb4", "utf8mb4_0900_ai_ci"))
	return cfg
}
func (s settings) address() string {
	if s.Network == "unix" {
		return s.Address
	}
	return net.JoinHostPort(s.Address, strconv.Itoa(int(s.Port)))
}
func (s settings) reservation() int64 {
	return int64(2*MaxArgumentBytes+MaxSQLBytes+3*s.MaxPacketBytes+s.MaxResultBytes) + int64(s.MaxRows)*(MaxColumns*24+96) + int64(MaxPreparedStatements)*(int64(MaxSQLBytes)+int64(s.MaxResponseBytes)) + 128<<10
}
func (s settings) evidenceReservation() int64 {
	return int64(s.MaxResultBytes+3*s.MaxPacketBytes) + int64(s.MaxRows)*(MaxColumns*24+48) + 128<<10
}
func (s settings) limits() resource.Limits {
	return resource.Limits{Active: s.MaxConnections, Queued: s.QueuedCalls, Bytes: int64(s.MaxConnections) * s.reservation(), QueuedBytes: int64(s.QueuedCalls) * s.reservation(), MaxLeases: MaxPreparedStatements + 4}
}

// LimitsV1 recommends admission for defaulted, unoverridden options. If layers
// change bounds, composition must select a corresponding explicit policy.
func LimitsV1(v OptionsV1) resource.Limits { return defaults(v).limits() }

// Profile copies non-secret effective settings; declarations are not service evidence.
func (db *Database) Profile() compatibility.Profile {
	if db == nil || db.owner == nil {
		return compatibility.Profile{}
	}
	s := db.owner.settings
	identity := "server-name-and-chain"
	if s.Plaintext {
		identity = "isolated-plaintext"
	} else if s.ServerCertificateSHA256 != "" {
		identity = "leaf-pin-and-chain"
	}
	options := []compatibility.Option{{Name: "authentication-policy", Value: s.Authentication}, {Name: "plaintext", Value: strconv.FormatBool(s.Plaintext)}, {Name: "tls-identity", Value: identity},
		{Name: "local-infile", Value: "denied-before-dispatch"}, {Name: "query-mode", Value: "native-no-retry"}, {Name: "parse-time", Value: strconv.FormatBool(s.ParseTime)},
		{Name: "network", Value: s.Network}, {Name: "client-found-rows", Value: strconv.FormatBool(s.ClientFoundRows)}, {Name: "columns-with-alias", Value: strconv.FormatBool(s.ColumnsWithAlias)},
		{Name: "max-idle-connections", Value: strconv.Itoa(s.MaxIdleConnections)}, {Name: "max-idle-time-ns", Value: strconv.FormatInt(int64(s.MaxIdleTime), 10)}, {Name: "max-lifetime-ns", Value: strconv.FormatInt(int64(s.MaxLifetime), 10)},
		{Name: "read-timeout-ns", Value: strconv.FormatInt(int64(s.ReadTimeout), 10)}, {Name: "write-timeout-ns", Value: strconv.FormatInt(int64(s.WriteTimeout), 10)},
		{Name: "autocommit", Value: "1"}, {Name: "time-zone", Value: "UTC"}, {Name: "charset", Value: "utf8mb4"},
		{Name: "max-connections", Value: strconv.Itoa(s.MaxConnections)}, {Name: "queued-calls", Value: strconv.Itoa(s.QueuedCalls)},
		{Name: "timeout-ns", Value: strconv.FormatInt(int64(s.Timeout), 10)}, {Name: "close-timeout-ns", Value: strconv.FormatInt(int64(s.CloseTimeout), 10)},
		{Name: "transaction-timeout-ns", Value: strconv.FormatInt(int64(s.TransactionTimeout), 10)},
		{Name: "max-rows", Value: strconv.Itoa(s.MaxRows)}, {Name: "max-result-bytes", Value: strconv.Itoa(s.MaxResultBytes)},
		{Name: "max-packet-bytes", Value: strconv.Itoa(s.MaxPacketBytes)}, {Name: "max-response-bytes", Value: strconv.Itoa(s.MaxResponseBytes)}}
	return compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "sql-pinned-framed",
		ServiceMode: compatibility.Fact{Kind: compatibility.Declared, Value: "mysql-innodb-single-server"},
		Protocol:    compatibility.Fact{Kind: compatibility.Declared, Value: "mysql-41"}, Native: compatibility.Fact{Kind: compatibility.NotApplicable}, Options: options}
}
