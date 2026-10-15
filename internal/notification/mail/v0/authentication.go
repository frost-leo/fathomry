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
	"crypto/x509"
	"encoding/base64"
	"errors"
	"net/textproto"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"

	smtp "github.com/wneessen/go-mail/smtp"
)

// ResponseHandler may inspect an error or consume bounded recovery lines.
// Returning nil asks native SMTP to read a new response, not to fabricate success.
// It must not issue commands or retain/use Reader after returning.
type ResponseHandler func(context.Context, string, string, *ResponseReader, error) error

// ResponseReader grants only scoped response consumption, never write/close authority.
type ResponseReader struct {
	private
	mu        sync.Mutex
	conn      *textproto.Conn
	remaining int
}

func (reader *ResponseReader) ReadLine() (string, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.conn == nil || reader.remaining <= 0 {
		return "", failure(ErrLimit, "response-reader")
	}
	reader.remaining--
	return reader.conn.ReadLine()
}

type responseHandler struct {
	ctx    context.Context
	hook   ResponseHandler
	record func(error) bool
}

func (handler responseHandler) HandleError(host, command string, conn *textproto.Conn, original error) error {
	if handler.record != nil && !handler.record(original) {
		return errors.Join(original, failure(ErrLimit, "response-recovery"))
	}
	if handler.hook == nil {
		return original
	}
	reader := &ResponseReader{conn: conn, remaining: 16}
	defer func() { reader.mu.Lock(); reader.conn = nil; reader.mu.Unlock() }()
	err := handler.hook(handler.ctx, host, command, reader, original)
	if err != nil {
		return errors.Join(original, err)
	}
	return handler.ctx.Err()
}

func freezeNative(input NativeOptions) (NativeOptions, int64, error) {
	total := int64(4096)
	if input.AlwaysDKIM != nil && input.AlwaysDKIM.key == nil {
		return NativeOptions{}, 0, failure(ErrInput, "source-dkim")
	}
	if len(input.ResponseHandlers) > 32 {
		return NativeOptions{}, 0, failure(ErrLimit, "handlers")
	}
	handlers := make(map[string]ResponseHandler, len(input.ResponseHandlers))
	for command, handler := range input.ResponseHandlers {
		switch command {
		case "ehlo", "helo", "starttls", "auth", "mail", "rcpt", "data", "rset", "noop", "vrfy", "quit":
		default:
			return NativeOptions{}, 0, failure(ErrInput, "handler-command")
		}
		if handler == nil {
			return NativeOptions{}, 0, failure(ErrInput, "handler")
		}
		handlers[command] = handler
	}
	input.ResponseHandlers = handlers
	if input.TLS != nil {
		charge, err := tlsContainerCharge(input.TLS)
		if err != nil {
			return NativeOptions{}, 0, err
		}
		total += charge
		config := input.TLS.Clone()
		if config.InsecureSkipVerify || config.MinVersion != 0 && config.MinVersion < tls.VersionTLS12 || config.MaxVersion != 0 && config.MaxVersion < tls.VersionTLS12 {
			return NativeOptions{}, 0, failure(ErrUnsupported, "tls")
		}
		if config.MinVersion == 0 {
			config.MinVersion = tls.VersionTLS12
		}
		if len(config.Certificates) > 16 || len(config.NameToCertificate) > 0 || len(config.NextProtos) > 16 || len(config.CipherSuites) > 256 || len(config.CurvePreferences) > 256 || len(config.EncryptedClientHelloConfigList) > 64<<10 {
			return NativeOptions{}, 0, failure(ErrLimit, "tls")
		}
		config.Certificates = slices.Clone(config.Certificates)
		for index := range config.Certificates {
			cert := &config.Certificates[index]
			if len(cert.Certificate) > 16 {
				return NativeOptions{}, 0, failure(ErrLimit, "certificate")
			}
			cert.Certificate = slices.Clone(cert.Certificate)
			for pos, der := range cert.Certificate {
				cert.Certificate[pos] = slices.Clone(der)
			}
			// Leaf is recomputed from owned DER by crypto/tls; keys/callbacks are borrowed.
			cert.Leaf = nil
			cert.OCSPStaple = slices.Clone(cert.OCSPStaple)
			cert.SignedCertificateTimestamps = slices.Clone(cert.SignedCertificateTimestamps)
			for pos, stamp := range cert.SignedCertificateTimestamps {
				cert.SignedCertificateTimestamps[pos] = slices.Clone(stamp)
			}
			cert.SupportedSignatureAlgorithms = slices.Clone(cert.SupportedSignatureAlgorithms)
		}
		config.NextProtos = slices.Clone(config.NextProtos)
		config.CipherSuites = slices.Clone(config.CipherSuites)
		config.CurvePreferences = slices.Clone(config.CurvePreferences)
		config.EncryptedClientHelloConfigList = slices.Clone(config.EncryptedClientHelloConfigList)
		if config.RootCAs != nil {
			config.RootCAs = config.RootCAs.Clone()
		}
		if config.ClientCAs != nil {
			config.ClientCAs = config.ClientCAs.Clone()
		}
		input.TLS = config
	}
	return input, total, nil
}

func tlsContainerCharge(config *tls.Config) (int64, error) {
	total := int64(4096)
	add := func(size int64) bool {
		if size < 0 || size > (2<<20)-total {
			return false
		}
		total += size
		return true
	}
	if len(config.Certificates) > 16 || len(config.NameToCertificate) != 0 || len(config.NextProtos) > 16 || len(config.CipherSuites) > 256 || len(config.CurvePreferences) > 256 || len(config.EncryptedClientHelloConfigList) > 64<<10 {
		return 0, failure(ErrLimit, "tls")
	}
	if !add(int64(len(config.ServerName)+len(config.EncryptedClientHelloConfigList))*2 + int64(len(config.CipherSuites)+len(config.CurvePreferences))*16) {
		return 0, failure(ErrLimit, "tls")
	}
	for _, protocol := range config.NextProtos {
		if len(protocol) > 255 || !add(int64(len(protocol))*2+32) {
			return 0, failure(ErrLimit, "tls-protocol")
		}
	}
	for _, certificate := range config.Certificates {
		if len(certificate.Certificate) > 16 || len(certificate.SignedCertificateTimestamps) > 32 || len(certificate.SupportedSignatureAlgorithms) > 256 {
			return 0, failure(ErrLimit, "tls-certificate")
		}
		if !add(int64(len(certificate.OCSPStaple))*2 + int64(len(certificate.SupportedSignatureAlgorithms))*16 + 1024) {
			return 0, failure(ErrLimit, "tls-certificate")
		}
		for _, der := range certificate.Certificate {
			if !add(int64(len(der))*4 + 32) {
				return 0, failure(ErrLimit, "tls-certificate")
			}
		}
		for _, stamp := range certificate.SignedCertificateTimestamps {
			if !add(int64(len(stamp))*2 + 32) {
				return 0, failure(ErrLimit, "tls-sct")
			}
		}
	}
	for _, pool := range []*x509.CertPool{config.RootCAs, config.ClientCAs} {
		if pool == nil {
			continue
		}
		subjects := pool.Subjects()
		if len(subjects) > 1024 {
			return 0, failure(ErrLimit, "tls-trust")
		}
		for _, subject := range subjects {
			if !add(int64(len(subject))*2 + 512) {
				return 0, failure(ErrLimit, "tls-trust")
			}
		}
	}
	return total, nil
}

func authMechanism(name string) bool {
	switch name {
	case "plain", "login", "xoauth2", "cram-md5", "ntlm", "scram-sha-1", "scram-sha-256", "scram-sha-1-plus", "scram-sha-256-plus":
		return true
	}
	return false
}
func (owned *owner) authentication(ctx context.Context, state tls.ConnectionState, advertised string) (smtp.Auth, string, error) {
	value := owned.settings
	if value.Auth == "custom" {
		auth, err := owned.hooks.Auth(ctx, state)
		if err != nil {
			return nil, "", err
		}
		if nilInterface(auth) {
			return nil, "", failure(ErrInput, "custom-auth")
		}
		return &boundedAuth{ctx: ctx, native: auth, limit: value.MaxAuthBytes, iterations: value.MaxSCRAMIterations, advertised: strings.Fields(advertised)}, "custom", nil
	}
	wanted := value.Auth
	available := strings.Fields(strings.ToLower(advertised))
	preference := []string{"scram-sha-256-plus", "scram-sha-256", "scram-sha-1-plus", "scram-sha-1", "ntlm", "cram-md5", "plain", "login"}
	if wanted == "preferred" {
		preference = value.AuthPreferences
	}
	if wanted == "auto" || wanted == "preferred" {
		wanted = ""
		for _, candidate := range preference {
			if slices.Contains(available, candidate) {
				wanted = candidate
				break
			}
		}
		if wanted == "" && value.Auth == "preferred" && value.AuthFallback {
			for _, candidate := range []string{"scram-sha-256-plus", "scram-sha-256", "scram-sha-1-plus", "scram-sha-1", "ntlm", "cram-md5", "plain", "login"} {
				if slices.Contains(available, candidate) {
					wanted = candidate
					break
				}
			}
		}
	}
	if wanted == "" || !slices.Contains(available, wanted) {
		return nil, "", failure(ErrUnsupported, "auth")
	}
	var auth smtp.Auth
	switch wanted {
	case "plain":
		auth = smtp.PlainAuth(value.AuthIdentity, value.Username, value.Password, value.Host, false)
	case "login":
		auth = smtp.LoginAuth(value.Username, value.Password, value.Host, false)
	case "xoauth2":
		auth = smtp.XOAuth2Auth(value.Username, value.Password)
	case "cram-md5":
		auth = smtp.CRAMMD5Auth(value.Username, value.Password)
	case "ntlm":
		auth = smtp.NTLMAuth(value.Username, value.Password, value.AuthDomain)
	case "scram-sha-1":
		auth = smtp.ScramSHA1Auth(value.Username, value.Password)
	case "scram-sha-256":
		auth = smtp.ScramSHA256Auth(value.Username, value.Password)
	case "scram-sha-1-plus":
		auth = smtp.ScramSHA1PlusAuth(value.Username, value.Password, &state)
	case "scram-sha-256-plus":
		auth = smtp.ScramSHA256PlusAuth(value.Username, value.Password, &state)
	}
	return &boundedAuth{ctx: ctx, native: auth, limit: value.MaxAuthBytes, iterations: value.MaxSCRAMIterations, advertised: strings.Fields(advertised)}, wanted, nil
}

type boundedAuth struct {
	ctx                                  context.Context
	native                               smtp.Auth
	limit, iterations, rounds, scramStep int
	advertised                           []string
	mechanism, nonce                     string
}

func (auth *boundedAuth) Start(server *smtp.ServerInfo) (string, []byte, error) {
	if err := auth.ctx.Err(); err != nil {
		return "", nil, err
	}
	serverCopy := *server
	serverCopy.Auth = slices.Clone(server.Auth)
	mechanism, data, err := auth.native.Start(&serverCopy)
	if err != nil {
		return "", nil, err
	}
	if !slices.Contains(auth.advertised, mechanism) || len(data) > auth.limit {
		return "", nil, failure(ErrLimit, "auth-start")
	}
	auth.mechanism = mechanism
	return mechanism, data, nil
}
func (auth *boundedAuth) Next(challenge []byte, more bool) ([]byte, error) {
	if err := auth.ctx.Err(); err != nil {
		return nil, err
	}
	auth.rounds++
	if len(challenge) > auth.limit || auth.rounds > 16 {
		return nil, failure(ErrLimit, "auth-challenge")
	}
	scram := strings.HasPrefix(auth.mechanism, "SCRAM-")
	if scram {
		switch auth.scramStep {
		case 0:
			if !more || len(challenge) != 0 {
				return nil, failure(ErrInput, "scram-initial")
			}
		case 1:
			parts := strings.Split(string(challenge), ",")
			if !more || len(parts) != 3 || !strings.HasPrefix(parts[0], "r="+auth.nonce) || len(parts[0]) <= len(auth.nonce)+2 || !strings.HasPrefix(parts[1], "s=") || !strings.HasPrefix(parts[2], "i=") {
				return nil, failure(ErrInput, "scram-challenge")
			}
			for _, char := range parts[0][2:] {
				if char < 33 || char > 126 {
					return nil, failure(ErrInput, "scram-nonce")
				}
			}
			for _, char := range parts[2][2:] {
				if char < '0' || char > '9' {
					return nil, failure(ErrInput, "scram-iterations")
				}
			}
			salt, err := base64.StdEncoding.DecodeString(parts[1][2:])
			if err != nil || len(salt) == 0 || len(salt) > 1024 {
				return nil, failure(ErrInput, "scram-salt", err)
			}
			iterations, err := strconv.Atoi(parts[2][2:])
			if err != nil || iterations < 1 || iterations > auth.iterations {
				return nil, failure(ErrLimit, "scram-iterations", err)
			}
		case 2:
			if !more || !strings.HasPrefix(string(challenge), "v=") {
				return nil, failure(ErrInput, "scram-verifier")
			}
		case 3:
			if more {
				return nil, failure(ErrInput, "scram-completion")
			}
		default:
			return nil, failure(ErrInput, "scram-state")
		}
	}
	response, err := auth.native.Next(challenge, more)
	if err != nil {
		return nil, err
	}
	if len(response) > auth.limit {
		return nil, failure(ErrLimit, "auth-response")
	}
	if scram {
		if auth.scramStep == 0 {
			_, auth.nonce, _ = strings.Cut(string(response), ",r=")
			if auth.nonce == "" {
				return nil, failure(ErrInput, "scram-nonce")
			}
		}
		auth.scramStep++
	}
	return response, nil
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	}
	return false
}
