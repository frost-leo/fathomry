/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package mail

import (
	"context"
	"crypto/tls"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/notification/mail/v0"
	smtp "github.com/wneessen/go-mail/smtp"
	"net"
	"time"
)

// Settings is loadable data with explicit endpoint/TLS/auth selection. Zero limits
// use Internal defaults; durations are ns. Preparation is not readiness. Credentials
// are frozen until source replacement. Explicit JSON is sensitive; fmt/slog redact.
type Settings struct {
	Name               string        `json:"name"`
	Version            uint32        `json:"version"`
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

// NativeOptions contains explicitly borrowed callbacks and TLS dependencies.
// They must be cooperative, concurrency-safe and non-panicking. Preparation never
// invokes them. DialContext transfers ownership of every non-nil returned socket.
type NativeOptions struct {
	private
	DialContext            func(context.Context, string, string) (net.Conn, error)
	Auth                   func(context.Context, tls.ConnectionState) (smtp.Auth, error)
	TLS                    *tls.Config
	ResponseHandlers       map[string]ResponseHandler
	DefaultResponseHandler ResponseHandler
	AlwaysDKIM             *DKIM
}

// Auth and AuthServerInfo are the selected SDK's SASL exchange values, not
// owning clients. Factories create one exchange at a time under admitted work.
type Auth = smtp.Auth
type AuthServerInfo = smtp.ServerInfo

// ResponseHandler is scoped recovery, not SMTP send/close authority. A nil return
// asks native SMTP to read a real replacement response; it does not invent one.
type ResponseHandler func(context.Context, string, string, *ResponseReader, error) error
type ResponseReader struct {
	private
	native *native.ResponseReader
}

func (reader *ResponseReader) ReadLine() (string, error) {
	if reader == nil || reader.native == nil {
		return "", fail(ErrState, "response-reader")
	}
	line, err := reader.native.ReadLine()
	return line, translate(err, "response-reader")
}

// Dependencies borrows public owners. Client never closes the Runtime or Inbox.
type Dependencies struct {
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Result]
	Observer *adapters.Observer
	Native   NativeOptions
}

func options(value Settings, hooks NativeOptions) native.OptionsV1 {
	handlers := make(map[string]native.ResponseHandler, len(hooks.ResponseHandlers))
	adapt := func(hook ResponseHandler) native.ResponseHandler {
		if hook == nil {
			return nil
		}
		return func(ctx context.Context, host, command string, reader *native.ResponseReader, err error) error {
			return hook(ctx, host, command, &ResponseReader{native: reader}, err)
		}
	}
	for name, hook := range hooks.ResponseHandlers {
		handlers[name] = adapt(hook)
	}
	return native.OptionsV1{
		Name:               value.Name,
		Version:            value.Version,
		Host:               value.Host,
		Port:               value.Port,
		FallbackPort:       value.FallbackPort,
		Hello:              value.Hello,
		TLSMode:            value.TLSMode,
		RootCAPEM:          value.RootCAPEM,
		Auth:               value.Auth,
		Username:           value.Username,
		Password:           value.Password,
		MaxActive:          value.MaxActive,
		QueuedCalls:        value.QueuedCalls,
		MaxMessages:        value.MaxMessages,
		MaxRecipients:      value.MaxRecipients,
		MaxMessageBytes:    value.MaxMessageBytes,
		MaxMIMEBytes:       value.MaxMIMEBytes,
		MaxReplyBytes:      value.MaxReplyBytes,
		Timeout:            value.Timeout,
		CloseTimeout:       value.CloseTimeout,
		Network:            value.Network,
		SocketPath:         value.SocketPath,
		AllowPlaintext:     value.AllowPlaintext,
		SkipNoop:           value.SkipNoop,
		SkipReset:          value.SkipReset,
		DisableSMTPUTF8:    value.DisableSMTPUTF8,
		AuthIdentity:       value.AuthIdentity,
		AuthDomain:         value.AuthDomain,
		AuthPreferences:    value.AuthPreferences,
		AuthFallback:       value.AuthFallback,
		MaxAuthBytes:       value.MaxAuthBytes,
		MaxSCRAMIterations: value.MaxSCRAMIterations,
		DSNReturn:          value.DSNReturn,
		DSNNotify:          value.DSNNotify,
		Native:             native.NativeOptions{DialContext: hooks.DialContext, Auth: hooks.Auth, TLS: hooks.TLS, ResponseHandlers: handlers, DefaultResponseHandler: adapt(hooks.DefaultResponseHandler), AlwaysDKIM: nativeDKIM(hooks.AlwaysDKIM)}}
}

// Validate checks loadable configuration only, without requiring or invoking
// runtime dependencies. Prepare additionally freezes and checks those declarations.
func Validate(value Settings) error {
	_, err := native.DataBudgetV1(options(value, NativeOptions{}))
	return translate(err, "validate")
}
