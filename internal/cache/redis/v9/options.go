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
	"crypto/tls"
	"crypto/x509"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/resource"
)

// OptionsV1 is borrowed during Select only. Version zero selects format 1.
// Modes are standalone, sentinel, cluster, ring and universal. UniversalMode
// must explicitly select standalone, sentinel or cluster; address count does
// not silently change topology. Addrs contains seeds; AllowedAddrs is the complete
// network authority (defaults to seeds/shards). Discovery never expands it.
//
// Commands and AdminCommands are exact uppercase command or COMMAND|SUBCOMMAND
// grants; no commands are granted by default. AdminCommands explicitly grants
// administrative or module/raw commands. Server ACLs remain the security boundary.
// Session/protocol-control commands cannot be granted through the raw route.
//
// Plaintext requires explicit opt-in; otherwise RootCAPEM supplies the complete
// trust set. No URL/environment/default localhost discovery occurs.
// Durations are nanoseconds. Ordinary command retries are disabled. MaxRedirects
// also permits Cluster network retries and therefore defaults to zero.
//
// Defaults: RESP3, 4 active calls, no waiting queue, 16 total sockets, 32 commands,
// 4096 arguments, 1 MiB requests/replies, 32768 reply elements, 5 s timeout,
// 5 s cleanup, 5 min idle expiry, no lifetime expiry. Experimental capabilities
// are opt-in. Cache defaults: 1024 entries, 8 MiB estimated bytes, 1 s staleness.
// Declared reservations are not a heap/RSS or distributed quota guarantee.
type OptionsV1 struct {
	private
	Name                     string
	Version                  uint32
	Mode                     string
	UniversalMode            string
	Addrs                    []string
	AllowedAddrs             []string
	Shards                   map[string]string
	MasterName               string
	DB                       int
	Protocol                 int
	Username                 string
	Password                 string
	SentinelUsername         string
	SentinelPassword         string
	Plaintext                bool
	RootCAPEM                string
	ServerName               string
	CertificatePEM           string
	PrivateKeyPEM            string
	ReadOnly                 bool
	MaxActive                int
	QueuedCalls              int
	MaxConnections           int
	MaxCommands              int
	MaxArgs                  int
	MaxRequestBytes          int
	MaxReplyBytes            int
	MaxReplyElements         int
	Timeout                  time.Duration
	CloseTimeout             time.Duration
	MaxIdleTime              time.Duration
	MaxLifetime              time.Duration
	MaxRedirects             int
	Commands                 []string
	AdminCommands            []string
	ExperimentalCache        bool
	CacheEntries             int
	CacheBytes               int64
	CacheMaxStaleness        time.Duration
	ExperimentalAutoPipeline bool
	AllowSessions            bool
	AllowSubscriptions       bool
	MaintenanceMode          string
}

type settings struct {
	Mode                     string            `json:"mode"`
	UniversalMode            string            `json:"universal_mode"`
	Addrs                    []string          `json:"addrs"`
	AllowedAddrs             []string          `json:"allowed_addrs"`
	Shards                   map[string]string `json:"shards"`
	MasterName               string            `json:"master_name"`
	DB                       int               `json:"db"`
	Protocol                 int               `json:"protocol"`
	Username                 string            `json:"username"`
	Password                 string            `json:"password"`
	SentinelUsername         string            `json:"sentinel_username"`
	SentinelPassword         string            `json:"sentinel_password"`
	Plaintext                bool              `json:"plaintext"`
	RootCAPEM                string            `json:"root_ca_pem"`
	ServerName               string            `json:"server_name"`
	CertificatePEM           string            `json:"certificate_pem"`
	PrivateKeyPEM            string            `json:"private_key_pem"`
	ReadOnly                 bool              `json:"read_only"`
	MaxActive                int               `json:"max_active"`
	QueuedCalls              int               `json:"queued_calls"`
	MaxConnections           int               `json:"max_connections"`
	MaxCommands              int               `json:"max_commands"`
	MaxArgs                  int               `json:"max_args"`
	MaxRequestBytes          int               `json:"max_request_bytes"`
	MaxReplyBytes            int               `json:"max_reply_bytes"`
	MaxReplyElements         int               `json:"max_reply_elements"`
	Timeout                  time.Duration     `json:"timeout_ns"`
	CloseTimeout             time.Duration     `json:"close_timeout_ns"`
	MaxIdleTime              time.Duration     `json:"max_idle_time_ns"`
	MaxLifetime              time.Duration     `json:"max_lifetime_ns"`
	MaxRedirects             int               `json:"max_redirects"`
	Commands                 []string          `json:"commands"`
	AdminCommands            []string          `json:"admin_commands"`
	ExperimentalCache        bool              `json:"experimental_cache"`
	CacheEntries             int               `json:"cache_entries"`
	CacheBytes               int64             `json:"cache_bytes"`
	CacheMaxStaleness        time.Duration     `json:"cache_max_staleness_ns"`
	ExperimentalAutoPipeline bool              `json:"experimental_auto_pipeline"`
	AllowSessions            bool              `json:"allow_sessions"`
	AllowSubscriptions       bool              `json:"allow_subscriptions"`
	MaintenanceMode          string            `json:"maintenance_mode"`
}

func defaults(input OptionsV1) settings {
	value := settings{
		Mode:                     input.Mode,
		UniversalMode:            input.UniversalMode,
		Addrs:                    input.Addrs,
		AllowedAddrs:             input.AllowedAddrs,
		Shards:                   input.Shards,
		MasterName:               input.MasterName,
		DB:                       input.DB,
		Protocol:                 input.Protocol,
		Username:                 input.Username,
		Password:                 input.Password,
		SentinelUsername:         input.SentinelUsername,
		SentinelPassword:         input.SentinelPassword,
		Plaintext:                input.Plaintext,
		RootCAPEM:                input.RootCAPEM,
		ServerName:               input.ServerName,
		CertificatePEM:           input.CertificatePEM,
		PrivateKeyPEM:            input.PrivateKeyPEM,
		ReadOnly:                 input.ReadOnly,
		MaxActive:                input.MaxActive,
		QueuedCalls:              input.QueuedCalls,
		MaxConnections:           input.MaxConnections,
		MaxCommands:              input.MaxCommands,
		MaxArgs:                  input.MaxArgs,
		MaxRequestBytes:          input.MaxRequestBytes,
		MaxReplyBytes:            input.MaxReplyBytes,
		MaxReplyElements:         input.MaxReplyElements,
		Timeout:                  input.Timeout,
		CloseTimeout:             input.CloseTimeout,
		MaxIdleTime:              input.MaxIdleTime,
		MaxLifetime:              input.MaxLifetime,
		MaxRedirects:             input.MaxRedirects,
		Commands:                 input.Commands,
		AdminCommands:            input.AdminCommands,
		ExperimentalCache:        input.ExperimentalCache,
		CacheEntries:             input.CacheEntries,
		CacheBytes:               input.CacheBytes,
		CacheMaxStaleness:        input.CacheMaxStaleness,
		ExperimentalAutoPipeline: input.ExperimentalAutoPipeline,
		AllowSessions:            input.AllowSessions,
		AllowSubscriptions:       input.AllowSubscriptions,
		MaintenanceMode:          input.MaintenanceMode,
	}
	if value.Protocol == 0 {
		value.Protocol = 3
	}
	if value.MaintenanceMode == "" {
		value.MaintenanceMode = "disabled"
	}
	if value.MaxActive == 0 {
		value.MaxActive = 4
	}
	if value.MaxConnections == 0 {
		value.MaxConnections = 16
	}
	if value.MaxCommands == 0 {
		value.MaxCommands = 32
	}
	if value.MaxArgs == 0 {
		value.MaxArgs = 4096
	}
	if value.MaxRequestBytes == 0 {
		value.MaxRequestBytes = 1 << 20
	}
	if value.MaxReplyBytes == 0 {
		value.MaxReplyBytes = 1 << 20
	}
	if value.MaxReplyElements == 0 {
		value.MaxReplyElements = 32768
	}
	if value.Timeout == 0 {
		value.Timeout = 5 * time.Second
	}
	if value.CloseTimeout == 0 {
		value.CloseTimeout = 5 * time.Second
	}
	if value.MaxIdleTime == 0 {
		value.MaxIdleTime = 5 * time.Minute
	}
	if value.CacheEntries == 0 {
		value.CacheEntries = 1024
	}
	if value.CacheBytes == 0 {
		value.CacheBytes = 8 << 20
	}
	if value.CacheMaxStaleness == 0 {
		value.CacheMaxStaleness = time.Second
	}
	if len(value.AllowedAddrs) == 0 {
		value.AllowedAddrs = append([]string(nil), value.Addrs...)
		for _, addr := range value.Shards {
			value.AllowedAddrs = append(value.AllowedAddrs, addr)
		}
	}
	return value
}

func (value settings) mode() string {
	if value.Mode == "universal" {
		return value.UniversalMode
	}
	return value.Mode
}
func addressValid(address string) bool {
	host, port, err := net.SplitHostPort(address)
	number, parseErr := strconv.Atoi(port)
	return err == nil && parseErr == nil && number > 0 && number <= 65535 && host != "" &&
		len(address) <= 320 && !strings.ContainsAny(address, "\r\n\t /@") && !strings.ContainsRune(address, 0)
}
func (value settings) allowed(address string) bool {
	for _, allowed := range value.AllowedAddrs {
		if address == allowed {
			return true
		}
	}
	return false
}
func validate(value settings) error {
	mode := value.mode()
	if value.MaintenanceMode != "disabled" && value.MaintenanceMode != "auto" && value.MaintenanceMode != "enabled" {
		return failure(ErrInput, "maintenance")
	}
	if value.MaintenanceMode != "disabled" && (value.Protocol != 3 || mode == "sentinel") {
		return failure(ErrUnsupported, "maintenance")
	}
	if mode != "standalone" && mode != "sentinel" && mode != "cluster" && mode != "ring" ||
		value.Mode == "universal" && mode == "ring" || value.Mode != "universal" && value.UniversalMode != "" ||
		value.Protocol != 2 && value.Protocol != 3 || value.DB < 0 || value.DB > 65535 ||
		value.MaxActive < 1 || value.MaxActive > 64 || value.QueuedCalls < 0 || value.QueuedCalls > 128 ||
		value.MaxConnections < 1 || value.MaxConnections > 256 || value.MaxActive > value.MaxConnections ||
		value.MaxCommands < 1 || value.MaxCommands > 256 || value.MaxArgs < 1 || value.MaxArgs > 65536 ||
		value.MaxRequestBytes < 1024 || value.MaxRequestBytes > 16<<20 ||
		value.MaxReplyBytes < 1024 || value.MaxReplyBytes > 16<<20 ||
		value.MaxReplyElements < 16 || value.MaxReplyElements > 65536 ||
		value.Timeout < time.Millisecond || value.Timeout > time.Minute ||
		value.CloseTimeout < time.Millisecond || value.CloseTimeout > time.Minute ||
		value.MaxIdleTime < 0 || value.MaxLifetime < 0 || value.MaxRedirects < 0 || value.MaxRedirects > 8 ||
		mode != "cluster" && value.MaxRedirects != 0 ||
		len(value.AllowedAddrs) < 1 || len(value.AllowedAddrs) > 64 ||
		len(value.Addrs) > 32 || len(value.Shards) > 32 ||
		len(value.Username) > 256 || len(value.Password) > 4096 ||
		len(value.SentinelUsername) > 256 || len(value.SentinelPassword) > 4096 ||
		len(value.MasterName) > 256 {
		return failure(ErrInput, "options")
	}
	if mode == "ring" && (len(value.Shards) == 0 || len(value.Addrs) != 0) ||
		mode != "ring" && (len(value.Addrs) == 0 || len(value.Shards) != 0) ||
		mode == "standalone" && len(value.Addrs) != 1 ||
		mode == "cluster" && value.DB != 0 ||
		mode == "sentinel" && value.MasterName == "" ||
		mode != "sentinel" && (value.MasterName != "" || value.SentinelPassword != "" || value.SentinelUsername != "") ||
		value.ReadOnly && mode != "cluster" && mode != "sentinel" {
		return failure(ErrInput, "topology")
	}
	for _, addr := range value.AllowedAddrs {
		if !addressValid(addr) {
			return failure(ErrInput, "addresses")
		}
	}
	for _, addr := range value.Addrs {
		if !value.allowed(addr) {
			return failure(ErrAuthority, "addresses")
		}
	}
	for name, addr := range value.Shards {
		if name == "" || len(name) > 64 || !value.allowed(addr) {
			return failure(ErrInput, "shards")
		}
	}
	if value.MaxConnections < value.poolCount()+value.socketReserve() {
		return failure(ErrInput, "connection-headroom")
	}
	if len(value.Commands)+len(value.AdminCommands) > 1024 {
		return failure(ErrLimit, "grants")
	}
	seen := map[string]bool{}
	for _, grants := range [][]string{value.Commands, value.AdminCommands} {
		for _, grant := range grants {
			if !validGrant(grant) || seen[grant] || forbidden(strings.Split(grant, "|")[0]) {
				return failure(ErrInput, "grants")
			}
			seen[grant] = true
		}
	}
	for _, grant := range value.Commands {
		if !ordinary(strings.Split(grant, "|")[0]) {
			return failure(ErrAuthority, "grants")
		}
	}
	if value.ExperimentalCache && (mode != "standalone" || value.DB != 0 || value.Protocol != 3) ||
		value.ExperimentalAutoPipeline && mode == "ring" {
		return failure(ErrUnsupported, "experimental")
	}
	if value.CacheEntries < 1 || value.CacheEntries > 65536 || value.CacheBytes < 1024 || value.CacheBytes > 256<<20 ||
		value.CacheMaxStaleness < time.Millisecond || value.CacheMaxStaleness > time.Minute {
		return failure(ErrInput, "cache")
	}
	_, err := value.tls()
	return err
}

func (value settings) poolCount() int {
	switch value.mode() {
	case "ring":
		return len(value.Shards)
	case "cluster", "sentinel":
		return len(value.AllowedAddrs)
	default:
		return 1
	}
}
func (value settings) socketReserve() int {
	reserve := 0
	if value.AllowSessions || value.AllowSubscriptions {
		reserve = value.MaxActive
	}
	if value.mode() == "sentinel" {
		reserve++
	}
	return reserve
}
func (value settings) poolCapacity() int {
	return (value.MaxConnections - value.socketReserve()) / max(1, value.poolCount())
}
func (value settings) tls() (*tls.Config, error) {
	if value.Plaintext {
		if value.RootCAPEM != "" || value.ServerName != "" || value.CertificatePEM != "" || value.PrivateKeyPEM != "" {
			return nil, failure(ErrInput, "tls")
		}
		return nil, nil
	}
	if len(value.RootCAPEM) == 0 || len(value.RootCAPEM) > 128<<10 || len(value.ServerName) > 253 {
		return nil, failure(ErrInput, "tls")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(value.RootCAPEM)) {
		return nil, failure(ErrInput, "tls")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: value.ServerName}
	if value.CertificatePEM != "" || value.PrivateKeyPEM != "" {
		if len(value.CertificatePEM) > 128<<10 || len(value.PrivateKeyPEM) > 128<<10 {
			return nil, failure(ErrLimit, "tls")
		}
		certificate, err := tls.X509KeyPair([]byte(value.CertificatePEM), []byte(value.PrivateKeyPEM))
		if err != nil {
			return nil, failure(ErrInput, "tls", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}
func (value settings) reservation() int64 {
	nodes := 1
	if value.mode() == "cluster" {
		nodes = len(value.AllowedAddrs)
	}
	return int64(2*value.MaxRequestBytes) + int64(max(1, nodes))*int64(value.MaxCommands)*(int64(value.MaxReplyBytes)*3+int64(value.MaxReplyElements)*1024+4096)
}
func (value settings) evidenceReservation() int64 {
	return int64(value.MaxCommands) * (int64(value.MaxReplyBytes) + int64(value.MaxReplyElements)*512 + 4096)
}
func (value settings) limits() resource.Limits {
	return resource.Limits{Active: value.MaxActive, Queued: value.QueuedCalls, Bytes: int64(value.MaxActive) * value.reservation(),
		QueuedBytes: int64(value.QueuedCalls) * value.reservation(), MaxLeases: 2}
}

// LimitsV1 returns the defaulted shared admission policy. If overlays change
// bounds, composition must attach a corresponding resolved policy instead.
func LimitsV1(options OptionsV1) resource.Limits { return defaults(options).limits() }

// Profile reports non-secret effective settings, not observed service support.
func (client *Client) Profile() compatibility.Profile {
	if client == nil || client.owner == nil {
		return compatibility.Profile{}
	}
	value := client.owner.settings
	credentialMode := "fixed"
	if client.owner.password != nil {
		credentialMode = "local-rotation"
	}
	options := []compatibility.Option{
		{Name: "topology-selection", Value: value.Mode}, {Name: "node-retries", Value: "disabled"},
		{Name: "credentials", Value: credentialMode},
		{Name: "context-deadlines", Value: "enabled"}, {Name: "maintenance-push", Value: value.MaintenanceMode},
		{Name: "plaintext", Value: strconv.FormatBool(value.Plaintext)},
		{Name: "replica-reads", Value: strconv.FormatBool(value.ReadOnly)},
		{Name: "experimental-cache", Value: strconv.FormatBool(value.ExperimentalCache)},
		{Name: "experimental-auto", Value: strconv.FormatBool(value.ExperimentalAutoPipeline)},
		{Name: "sessions", Value: strconv.FormatBool(value.AllowSessions)},
		{Name: "subscriptions", Value: strconv.FormatBool(value.AllowSubscriptions)},
		{Name: "mutual-tls", Value: strconv.FormatBool(value.CertificatePEM != "")},
	}
	for _, pair := range []struct {
		name  string
		value int64
	}{
		{"max-active", int64(value.MaxActive)}, {"max-connections", int64(value.MaxConnections)},
		{"max-commands", int64(value.MaxCommands)}, {"max-args", int64(value.MaxArgs)},
		{"request-bytes", int64(value.MaxRequestBytes)}, {"reply-bytes", int64(value.MaxReplyBytes)},
		{"reply-elements", int64(value.MaxReplyElements)}, {"timeout-ns", int64(value.Timeout)},
		{"close-timeout-ns", int64(value.CloseTimeout)}, {"redirects", int64(value.MaxRedirects)},
		{"cache-entries", int64(value.CacheEntries)}, {"cache-bytes", value.CacheBytes},
		{"cache-staleness-ns", int64(value.CacheMaxStaleness)},
		{"database", int64(value.DB)}, {"max-idle-ns", int64(value.MaxIdleTime)}, {"max-lifetime-ns", int64(value.MaxLifetime)},
	} {
		options = append(options, compatibility.Option{Name: pair.name, Value: strconv.FormatInt(pair.value, 10)})
	}
	return compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "redis-" + value.mode(),
		ServiceMode: compatibility.Fact{Kind: compatibility.Declared, Value: value.mode()},
		Protocol:    compatibility.Fact{Kind: compatibility.Declared, Value: "resp" + strconv.Itoa(value.Protocol)},
		Native:      compatibility.Fact{Kind: compatibility.NotApplicable}, Options: options}
}
