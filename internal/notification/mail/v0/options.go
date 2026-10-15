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

package mail

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/resource"
)

// OptionsV1 is borrowed only during Select. Version zero selects configuration
// format 1, distinct from module v0 and SMTP/MIME versions. Host, Port, TLSMode
// (implicit or starttls) and Hello are explicit. Empty RootCAPEM uses system roots;
// a nonempty PEM replaces them. TLS always verifies Host with TLS 1.2 or newer.
// Hello IP inputs are encoded as SMTP address literals; DNS names are unchanged.
// Auth selects none, plain, login, xoauth2, cram-md5, ntlm, scram-sha-1,
// scram-sha-256, either SCRAM plus variant, custom, auto or preferred. Preferred
// fallback is explicit. Password may be empty, or an externally obtained OAuth token, frozen
// with Username. Rotation requires replacing the source, not a hidden HTTP client.
//
// Defaults: 2 active calls, no queue, 16 messages/batch, 100 recipients/message,
// 4 MiB input/message, 8 MiB encoded MIME/message, 256 KiB incoming wire/batch,
// 30 s per admission/execution phase and 3 s close timeout. All bytes are
// per declared call except explicitly per-message limits. Durations are ns.
// SkipNoop/SkipReset explicitly control native session commands. DSNReturn and
// DSNNotify request native RET/NOTIFY values, not delivery. Authentication uses
// at most 16 exchanges, 16 KiB challenges/responses and 100000 SCRAM iterations
// by default. Runtime dependencies are separately frozen in Native.
type OptionsV1 struct {
	private
	Name               string
	Version            uint32
	Host               string
	Port               int
	FallbackPort       int
	Hello              string
	TLSMode            string
	RootCAPEM          string
	Auth               string
	Username           string
	Password           string
	MaxActive          int
	QueuedCalls        int
	MaxMessages        int
	MaxRecipients      int
	MaxMessageBytes    int
	MaxMIMEBytes       int
	MaxReplyBytes      int
	Timeout            time.Duration
	CloseTimeout       time.Duration
	Network            string
	SocketPath         string
	AllowPlaintext     bool
	SkipNoop           bool
	SkipReset          bool
	DisableSMTPUTF8    bool
	AuthIdentity       string
	AuthDomain         string
	AuthPreferences    []string
	AuthFallback       bool
	MaxAuthBytes       int
	MaxSCRAMIterations int
	DSNReturn          string
	DSNNotify          []string
	Native             NativeOptions
}
type settings struct {
	Host               string        `json:"host"`
	Port               int           `json:"port"`
	FallbackPort       int           `json:"fallback_port"`
	Hello              string        `json:"hello"`
	TLSMode            string        `json:"tls_mode"`
	RootCAPEM          string        `json:"root_ca_pem"`
	Auth               string        `json:"auth"`
	Username           string        `json:"username"`
	Password           string        `json:"password"`
	MaxActive          int           `json:"max_active"`
	QueuedCalls        int           `json:"queued_calls"`
	MaxMessages        int           `json:"max_messages"`
	MaxRecipients      int           `json:"max_recipients"`
	MaxMessageBytes    int           `json:"max_message_bytes"`
	MaxMIMEBytes       int           `json:"max_mime_bytes"`
	MaxReplyBytes      int           `json:"max_reply_bytes"`
	Timeout            time.Duration `json:"timeout_ns"`
	CloseTimeout       time.Duration `json:"close_timeout_ns"`
	Network            string        `json:"network"`
	SocketPath         string        `json:"socket_path"`
	AllowPlaintext     bool          `json:"allow_plaintext"`
	SkipNoop           bool          `json:"skip_noop"`
	SkipReset          bool          `json:"skip_reset"`
	DisableSMTPUTF8    bool          `json:"disable_smtp_utf8"`
	AuthIdentity       string        `json:"auth_identity"`
	AuthDomain         string        `json:"auth_domain"`
	AuthPreferences    []string      `json:"auth_preferences"`
	AuthFallback       bool          `json:"auth_fallback"`
	MaxAuthBytes       int           `json:"max_auth_bytes"`
	MaxSCRAMIterations int           `json:"max_scram_iterations"`
	DSNReturn          string        `json:"dsn_return"`
	DSNNotify          []string      `json:"dsn_notify"`
}

func defaults(input OptionsV1) settings {
	value := settings{Host: input.Host, Port: input.Port, Hello: input.Hello, TLSMode: input.TLSMode,
		RootCAPEM: input.RootCAPEM, Auth: input.Auth, Username: input.Username, Password: input.Password,
		MaxActive: input.MaxActive, QueuedCalls: input.QueuedCalls, MaxMessages: input.MaxMessages,
		MaxRecipients: input.MaxRecipients, MaxMessageBytes: input.MaxMessageBytes, MaxMIMEBytes: input.MaxMIMEBytes,
		MaxReplyBytes: input.MaxReplyBytes, Timeout: input.Timeout, CloseTimeout: input.CloseTimeout}
	value.Network, value.SocketPath, value.AllowPlaintext = input.Network, input.SocketPath, input.AllowPlaintext
	value.FallbackPort = input.FallbackPort
	value.SkipNoop, value.SkipReset, value.DisableSMTPUTF8 = input.SkipNoop, input.SkipReset, input.DisableSMTPUTF8
	value.AuthIdentity, value.AuthDomain = input.AuthIdentity, input.AuthDomain
	value.AuthPreferences, value.AuthFallback = slices.Clone(input.AuthPreferences), input.AuthFallback
	value.MaxAuthBytes, value.MaxSCRAMIterations = input.MaxAuthBytes, input.MaxSCRAMIterations
	value.DSNReturn, value.DSNNotify = input.DSNReturn, slices.Clone(input.DSNNotify)
	if value.Network == "" {
		value.Network = "tcp"
	}
	if value.MaxAuthBytes == 0 {
		value.MaxAuthBytes = 16 << 10
	}
	if value.MaxSCRAMIterations == 0 {
		value.MaxSCRAMIterations = 100000
	}
	if value.MaxActive == 0 {
		value.MaxActive = 2
	}
	if value.MaxMessages == 0 {
		value.MaxMessages = 16
	}
	if value.MaxRecipients == 0 {
		value.MaxRecipients = 100
	}
	if value.MaxMessageBytes == 0 {
		value.MaxMessageBytes = 4 << 20
	}
	if value.MaxMIMEBytes == 0 {
		value.MaxMIMEBytes = 8 << 20
	}
	if value.MaxReplyBytes == 0 {
		value.MaxReplyBytes = 256 << 10
	}
	if value.Timeout == 0 {
		value.Timeout = 30 * time.Second
	}
	if value.CloseTimeout == 0 {
		value.CloseTimeout = 3 * time.Second
	}
	return value
}
func hostValid(host string) bool {
	if host == "" || len(host) > 253 {
		return false
	}
	if net.ParseIP(host) != nil {
		return true
	}
	for _, part := range strings.Split(host, ".") {
		if len(part) == 0 || len(part) > 63 || part[0] == '-' || part[len(part)-1] == '-' {
			return false
		}
		for _, char := range part {
			if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-') {
				return false
			}
		}
	}
	return true
}
func validate(value settings) error {
	if !hostValid(value.Host) || !hostValid(value.Hello) || value.Network != "unix" && (value.Port < 1 || value.Port > 65535) ||
		value.MaxActive < 1 || value.MaxActive > 16 || value.QueuedCalls < 0 || value.QueuedCalls > 32 ||
		value.MaxMessages < 1 || value.MaxMessages > 64 || value.MaxRecipients < 1 || value.MaxRecipients > 100 ||
		value.MaxMessageBytes < 1024 || value.MaxMessageBytes > 16<<20 ||
		value.MaxMIMEBytes < 1024 || value.MaxMIMEBytes > 32<<20 || value.MaxReplyBytes < 4096 || value.MaxReplyBytes > 2<<20 ||
		value.Timeout < time.Millisecond || value.Timeout > 5*time.Minute ||
		value.CloseTimeout < time.Millisecond || value.CloseTimeout > time.Minute {
		return failure(ErrInput, "options")
	}
	if value.Network != "tcp" && value.Network != "unix" || value.Network == "unix" && (!filepath.IsAbs(value.SocketPath) || len(value.SocketPath) > 1024 || strings.ContainsRune(value.SocketPath, 0)) || value.Network == "tcp" && value.SocketPath != "" {
		return failure(ErrInput, "transport")
	}
	if value.MaxAuthBytes < 1024 || value.MaxAuthBytes > 64<<10 || value.MaxSCRAMIterations < 1 || value.MaxSCRAMIterations > 1000000 {
		return failure(ErrLimit, "authentication")
	}
	if value.FallbackPort < 0 || value.FallbackPort > 65535 || value.FallbackPort != 0 && (value.Network != "tcp" || value.FallbackPort == value.Port) {
		return failure(ErrInput, "fallback-port")
	}
	if value.TLSMode != "implicit" && value.TLSMode != "starttls" && !((value.TLSMode == "none" || value.TLSMode == "opportunistic") && value.AllowPlaintext) {
		return failure(ErrUnsupported, "tls")
	}
	switch value.Auth {
	case "none":
		if value.Username != "" || value.Password != "" {
			return failure(ErrInput, "credentials")
		}
	case "plain", "login", "xoauth2", "cram-md5", "ntlm", "scram-sha-1", "scram-sha-256", "scram-sha-1-plus", "scram-sha-256-plus", "auto", "preferred":
		if value.Username == "" || len(value.Username) > 320 || len(value.Password) > 4096 {
			return failure(ErrInput, "credentials")
		}
		if value.Auth == "plain" && strings.ContainsRune(value.AuthIdentity+value.Username+value.Password, 0) || value.Auth == "xoauth2" && strings.ContainsAny(value.Username+value.Password, "\r\n\x00\x01") {
			return failure(ErrInput, "credentials")
		}
	case "custom":
		if value.Username != "" || value.Password != "" {
			return failure(ErrInput, "credentials")
		}
	default:
		return failure(ErrUnsupported, "auth")
	}
	if value.TLSMode == "none" && value.Auth != "none" {
		return failure(ErrUnsupported, "unencrypted-auth")
	}
	if !line(value.AuthIdentity, 320) || !line(value.AuthDomain, 253) || len(value.AuthPreferences) > 12 {
		return failure(ErrInput, "auth")
	}
	if value.Auth != "preferred" && (len(value.AuthPreferences) != 0 || value.AuthFallback) || value.Auth == "preferred" && len(value.AuthPreferences) == 0 {
		return failure(ErrInput, "auth-policy")
	}
	for _, mechanism := range value.AuthPreferences {
		if !authMechanism(mechanism) {
			return failure(ErrInput, "auth-policy")
		}
	}
	if value.DSNReturn != "" && value.DSNReturn != "FULL" && value.DSNReturn != "HDRS" {
		return failure(ErrInput, "dsn")
	}
	if len(value.DSNNotify) > 3 {
		return failure(ErrInput, "dsn")
	}
	seen := map[string]bool{}
	for _, notify := range value.DSNNotify {
		if seen[notify] || notify != "NEVER" && notify != "SUCCESS" && notify != "FAILURE" && notify != "DELAY" || notify == "NEVER" && len(value.DSNNotify) != 1 {
			return failure(ErrInput, "dsn")
		}
		seen[notify] = true
	}
	_, err := value.tls()
	return err
}
func (value settings) tls() (*tls.Config, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: value.Host}
	if len(value.RootCAPEM) > 128<<10 {
		return nil, failure(ErrLimit, "trust")
	}
	if value.RootCAPEM != "" {
		config.RootCAs = x509.NewCertPool()
		if !config.RootCAs.AppendCertsFromPEM([]byte(value.RootCAPEM)) {
			return nil, failure(ErrInput, "trust")
		}
	}
	return config, nil
}
func (value settings) reservation() int64 {
	// One message is composed at a time; frozen caller inputs are caller-owned.
	return int64(value.MaxMIMEBytes)*8 + int64(value.MaxMessageBytes)*12 +
		value.evidenceReservation()
}
func (value settings) evidenceReservation() int64 {
	return int64(value.MaxMessages)*(int64(value.MaxRecipients)*1024+2048) + int64(value.MaxReplyBytes)*4 + int64(value.MaxMessageBytes)*2 + int64(value.MaxMIMEBytes)*2
}
func (value settings) limits() resource.Limits {
	return resource.Limits{Active: value.MaxActive, Queued: value.QueuedCalls,
		Bytes: int64(value.MaxActive) * value.reservation(), QueuedBytes: int64(value.QueuedCalls) * value.reservation(), MaxLeases: 1}
}

// LimitsV1 returns the defaulted admission policy. Overlays require a matching
// resolved policy; Bind rejects a policy that cannot cover the working envelope.
func LimitsV1(options OptionsV1) resource.Limits { return defaults(options).limits() }

// Profile contains declarations, not proof of service/client qualification.
func (client *Client) Profile() compatibility.Profile {
	if client == nil || client.owner == nil {
		return compatibility.Profile{}
	}
	value := client.owner.settings
	options := []compatibility.Option{{Name: "tls", Value: value.TLSMode}, {Name: "auth", Value: value.Auth},
		{Name: "retry", Value: "disabled"}, {Name: "html", Value: "caller-authored"},
		{Name: "credentials", Value: "frozen"}}
	for _, pair := range []struct {
		name  string
		value int64
	}{
		{"max-active", int64(value.MaxActive)}, {"max-messages", int64(value.MaxMessages)},
		{"max-recipients", int64(value.MaxRecipients)}, {"input-bytes", int64(value.MaxMessageBytes)},
		{"mime-bytes", int64(value.MaxMIMEBytes)}, {"reply-wire-bytes", int64(value.MaxReplyBytes)},
		{"timeout-ns", int64(value.Timeout)},
		{"close-timeout-ns", int64(value.CloseTimeout)}} {
		options = append(options, compatibility.Option{Name: pair.name, Value: strconv.FormatInt(pair.value, 10)})
	}
	return compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "mail-smtp",
		ServiceMode: compatibility.Fact{Kind: compatibility.Declared, Value: "outbound-relay"},
		Protocol:    compatibility.Fact{Kind: compatibility.Declared, Value: "smtp-mime"},
		Native:      compatibility.Fact{Kind: compatibility.NotApplicable}, Options: options}
}
