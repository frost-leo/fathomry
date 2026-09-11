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

package nacos

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	// MaxKeys bounds the distinct configuration identities selected by one client.
	MaxKeys = 16
	// MaxServers bounds the explicitly authorized members of one cluster.
	MaxServers = 8
	// MaxDocumentBytes bounds one decoded UTF-8 configuration document in bytes.
	MaxDocumentBytes = 1 << 20
	// MaxTotalBytes bounds the sum of raw content returned by one ReadAll call.
	MaxTotalBytes = 4 << 20
	// MaxWireBytes bounds an incoming gRPC message and its protocol JSON body.
	MaxWireBytes = 8 << 20
	// A session may hold a unary response and one incoming stream message together.
	reservationBytes = 2 * MaxWireBytes
)

// ServerV1 selects both channels of one authorized Nacos server. HTTPURL includes
// the server context path (usually /nacos); GRPCAddress is an explicit host:port.
type ServerV1 struct {
	private
	// HTTPURL is the authentication origin plus root or /nacos context path,
	// at most 2048 bytes. Userinfo, queries, fragments and redirects are refused.
	HTTPURL string
	// GRPCAddress is an explicit host:port, at most 512 bytes with port 1–65535.
	// No gRPC port offset or security mode is inferred from HTTPURL.
	GRPCAddress string
}

// KeyV1 identifies a preselected configuration in the client's namespace.
// An omitted group means DEFAULT_GROUP. Names are not diagnostic labels.
type KeyV1 struct {
	private
	// Group defaults to DEFAULT_GROUP. Present values use at most 128 UTF-8
	// bytes, without leading/trailing whitespace or control characters.
	Group string
	// DataID is required and uses the same 128-byte identifier bounds as Group.
	DataID string
}

// OptionsV1 is borrowed only during Open, then frozen. It is not a persisted DTO.
// Empty Namespace uses the native default-namespace form. Nonempty IDs are sent
// unchanged; server-side aliases such as "public" are version-dependent.
// Plaintext must be explicitly selected for isolated tests; both channels use
// verified TLS otherwise. Username and Password must both be present or absent.
type OptionsV1 struct {
	private
	// Name is a required 1–64 character lowercase ASCII source/scope label.
	// Composition owns its uniqueness and must exclude sensitive data.
	Name string
	// Namespace selects the native namespace form, at most 128 UTF-8 bytes.
	// Empty requests the default; nonempty IDs are not rewritten by this package.
	Namespace string
	// AppName is the bounded native application label; empty becomes fathomry.
	// It is not a business Run or Item identifier and is limited to 128 bytes.
	AppName string
	// Servers contains 1–MaxServers members of one cluster in initial failover
	// order. Open copies the slice and retained strings; it does not discover peers.
	Servers []ServerV1
	// Keys selects 1–MaxKeys distinct identities in this Namespace. ReadAll uses
	// this order and Watch observes this fixed set; Open copies retained storage.
	Keys []KeyV1
	// Username and Password select password-token authentication together.
	// Both empty select no login; Username is limited to 256 UTF-8 bytes.
	Username string
	// Password is limited to 4096 UTF-8 bytes and is never read from the environment.
	Password string
	// RootCAPEM supplies at most 64 KiB of trust roots for both transports.
	// Empty uses system trust; nonempty roots replace, rather than extend, that set.
	RootCAPEM string
	// AllowInsecure explicitly selects plaintext gRPC and permits HTTP for isolated
	// tests. False requires independently certificate-verified HTTPS and gRPC TLS.
	AllowInsecure bool
	// RequestTimeout is a cooperative phase budget including admission and native
	// request/setup/decoding work. Zero defaults to 10 s; valid values are 1 ms–1 min.
	// It does not force a blocked resolver or caller callback to terminate.
	RequestTimeout time.Duration
	// RetryDelay is the minimum registration/recovery pause, not a business retry
	// policy. Zero defaults to 100 ms; valid values are 1 ms–1 min.
	RetryDelay time.Duration
	// ReconcileInterval schedules native hash/presence comparisons when no push
	// wakes the observer. Zero defaults to 30 s; valid values are 1 s–5 min.
	ReconcileInterval time.Duration
	// ConcurrentRequests bounds admitted reads/batches and persistent subscriptions.
	// Zero defaults to 4; valid values are 2–16. The same count separately bounds
	// concurrent native dial/TLS phases; neither reservation is a process RSS limit.
	ConcurrentRequests int
	// QueuedRequests bounds FIFO admission waits, from 0–64. Zero refuses overload
	// instead of waiting; queued byte reservations are separate from active ones.
	QueuedRequests int
	// Subscriptions bounds live observers. Zero defaults to 1; the effective count
	// must be positive and less than ConcurrentRequests to leave read capacity.
	Subscriptions int
	// QueueCapacity bounds invalidations per subscription. Zero defaults to 16;
	// valid values are 1–64. Overflow drops the oldest event and requires resync.
	QueueCapacity int
}

type endpoint struct {
	HTTPURL     string `json:"http_url"`
	GRPCAddress string `json:"grpc_address"`
}
type key struct {
	Group  string `json:"group"`
	DataID string `json:"data_id"`
}

// settings is the private plain-data preparation input, separate from the guarded
// public bootstrap types. It records effective defaults, not live transports.
type settings struct {
	Name          string        `json:"name"`
	Namespace     string        `json:"namespace"`
	AppName       string        `json:"app_name"`
	Servers       []endpoint    `json:"servers"`
	Keys          []key         `json:"keys"`
	Username      string        `json:"username"`
	Password      string        `json:"password"`
	RootCAPEM     string        `json:"roots"`
	Plaintext     bool          `json:"plaintext"`
	Timeout       time.Duration `json:"timeout"`
	Retry         time.Duration `json:"retry"`
	Reconcile     time.Duration `json:"reconcile"`
	Active        int           `json:"active"`
	Queued        int           `json:"queued"`
	Subscriptions int           `json:"subscriptions"`
	Queue         int           `json:"queue"`
}

// prepareOptions validates the complete selection before construction and clones
// retained caller data. Preparing TLS trust does not establish server identity.
func prepareOptions(input OptionsV1) (settings, *tls.Config, error) {
	if len(input.Name) > 64 || len(input.Namespace) > 128 || len(input.AppName) > 128 ||
		len(input.Username) > 256 || len(input.Password) > 4096 || len(input.RootCAPEM) > 64<<10 ||
		len(input.Servers) < 1 || len(input.Servers) > MaxServers || len(input.Keys) < 1 || len(input.Keys) > MaxKeys {
		return settings{}, nil, fail(ErrInput, "options")
	}
	value := settings{Name: strings.Clone(input.Name), Namespace: strings.Clone(input.Namespace), AppName: strings.Clone(input.AppName),
		Username: strings.Clone(input.Username), Password: strings.Clone(input.Password), RootCAPEM: strings.Clone(input.RootCAPEM),
		Plaintext: input.AllowInsecure, Timeout: input.RequestTimeout, Retry: input.RetryDelay, Reconcile: input.ReconcileInterval,
		Active: input.ConcurrentRequests, Queued: input.QueuedRequests, Subscriptions: input.Subscriptions, Queue: input.QueueCapacity}
	if value.AppName == "" {
		value.AppName = "fathomry"
	}
	if value.Timeout == 0 {
		value.Timeout = 10 * time.Second
	}
	if value.Retry == 0 {
		value.Retry = 100 * time.Millisecond
	}
	if value.Reconcile == 0 {
		value.Reconcile = 30 * time.Second
	}
	if value.Active == 0 {
		value.Active = 4
	}
	if value.Subscriptions == 0 {
		value.Subscriptions = 1
	}
	if value.Queue == 0 {
		value.Queue = 16
	}
	if !sourceName(value.Name) || !identifier(value.Namespace, 128, true) || !identifier(value.AppName, 128, false) ||
		!utf8.ValidString(value.Username) || !utf8.ValidString(value.Password) ||
		(value.Username == "") != (value.Password == "") ||
		value.Timeout < time.Millisecond || value.Timeout > time.Minute ||
		value.Retry < time.Millisecond || value.Retry > time.Minute ||
		value.Reconcile < time.Second || value.Reconcile > 5*time.Minute ||
		value.Active < 2 || value.Active > 16 || value.Queued < 0 || value.Queued > 64 ||
		value.Subscriptions < 1 || value.Subscriptions >= value.Active || value.Queue < 1 || value.Queue > 64 {
		return settings{}, nil, fail(ErrInput, "options")
	}
	for _, selected := range input.Keys {
		group := selected.Group
		if group == "" {
			group = "DEFAULT_GROUP"
		}
		item := key{group, selected.DataID}
		if !identifier(group, 128, false) || !identifier(item.DataID, 128, false) || slices.Contains(value.Keys, item) {
			return settings{}, nil, fail(ErrInput, "keys")
		}
		value.Keys = append(value.Keys, key{strings.Clone(group), strings.Clone(item.DataID)})
	}
	for _, selected := range input.Servers {
		if len(selected.HTTPURL) > 2048 || len(selected.GRPCAddress) > 512 {
			return settings{}, nil, fail(ErrLimit, "servers")
		}
		parsed, err := url.Parse(selected.HTTPURL)
		if err != nil || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery ||
			parsed.Fragment != "" || parsed.Opaque != "" || parsed.RawPath != "" ||
			(parsed.Scheme != "https" && !(value.Plaintext && parsed.Scheme == "http")) {
			return settings{}, nil, fail(ErrInput, "servers")
		}
		if parsed.Path != "" && parsed.Path != "/" && parsed.Path != "/nacos" && parsed.Path != "/nacos/" {
			return settings{}, nil, fail(ErrUnsupported, "server-context")
		}
		if port := parsed.Port(); port != "" && !validPort(port) {
			return settings{}, nil, fail(ErrInput, "servers")
		}
		host, port, err := net.SplitHostPort(selected.GRPCAddress)
		if err != nil || !identifier(host, 256, false) || strings.ContainsAny(host, "/?#@") || !validPort(port) {
			return settings{}, nil, fail(ErrInput, "servers")
		}
		parsed.Path = strings.TrimRight(parsed.Path, "/")
		item := endpoint{parsed.String(), strings.Clone(selected.GRPCAddress)}
		if slices.Contains(value.Servers, item) {
			return settings{}, nil, fail(ErrInput, "servers")
		}
		value.Servers = append(value.Servers, item)
	}
	trust := &tls.Config{MinVersion: tls.VersionTLS12}
	if value.RootCAPEM != "" {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(value.RootCAPEM)) {
			return settings{}, nil, fail(ErrInput, "roots")
		}
		trust.RootCAs = roots
	}
	return value, trust, nil
}
func validPort(value string) bool {
	number, err := strconv.Atoi(value)
	return err == nil && number > 0 && number <= 65535
}
func identifier(value string, limit int, empty bool) bool {
	if len(value) > limit || !utf8.ValidString(value) || value == "" && !empty || strings.TrimSpace(value) != value {
		return false
	}
	for _, char := range value {
		if unicode.IsControl(char) {
			return false
		}
	}
	return true
}
func sourceName(value string) bool {
	if value == "" || len(value) > 64 || value == "." || value == ".." {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '-' || char == '_' || char == '.') {
			return false
		}
	}
	return true
}
func normalizeKey(value KeyV1) key {
	group := value.Group
	if group == "" {
		group = "DEFAULT_GROUP"
	}
	return key{group, value.DataID}
}
