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

package nuki

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"io"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	http "github.com/nukilabs/http"
	"github.com/nukilabs/http/http2"
	quic "github.com/nukilabs/quic-go"
	"github.com/nukilabs/quic-go/qlogwriter"
	sdk "github.com/nukilabs/tlsclient"
	"github.com/nukilabs/tlsclient/bandwidth"
	tls "github.com/nukilabs/utls"
	"golang.org/x/net/idna"
)

func copyNative(input NativeOptionsV1) (NativeOptionsV1, error) {
	if input.Profile == nil || input.Profile.ClientHelloSpec == nil {
		return NativeOptionsV1{}, failure(ErrInput, "profile")
	}
	if err := boundNativeContainers(input); err != nil {
		return NativeOptionsV1{}, err
	}
	result := input
	profile := *input.Profile
	if len(profile.PseudoHeaderOrder) > 16 {
		return result, failure(ErrInput, "profile")
	}
	profile.PseudoHeaderOrder = slices.Clone(profile.PseudoHeaderOrder)
	if profile.H2 != nil {
		if len(profile.H2.Settings) > 128 || len(profile.H2.Priorities) > 128 || profile.H2.ConnectionFlow > 1<<31-1 {
			return result, failure(ErrInput, "http2-profile")
		}
		copied := *profile.H2
		copied.Settings, copied.Priorities = slices.Clone(copied.Settings), slices.Clone(copied.Priorities)
		for _, setting := range copied.Settings {
			if err := setting.Valid(); err != nil {
				return result, failure(ErrInput, "http2-setting", err)
			}
			if setting.ID == http2.SettingHeaderTableSize && setting.Val > 64<<20 {
				return result, failure(ErrLimit, "hpack-table")
			}
		}
		profile.H2 = &copied
	}
	if profile.H3 != nil {
		if len(profile.H3.Settings) > 128 {
			return result, failure(ErrInput, "http3-profile")
		}
		copied := *profile.H3
		copied.Settings = slices.Clone(copied.Settings)
		for _, setting := range copied.Settings {
			if setting.ID == 1 && setting.Val > 64<<20 || setting.ID == 7 && setting.Val > 4096 {
				return result, failure(ErrInput, "qpack-bound")
			}
		}
		profile.H3 = &copied
	}
	result.Profile = &profile
	result.TLS = cloneTLS(input.TLS)
	if input.QUIC == nil {
		result.QUIC = &quic.Config{}
	} else {
		if input.QUIC.GetConfigForClient != nil || input.QUIC.AllowConnectionWindowIncrease != nil {
			return result, failure(ErrUnsupported, "owning-quic-callback")
		}
		result.QUIC = input.QUIC.Clone()
		result.QUIC.Versions = slices.Clone(input.QUIC.Versions)
	}
	if input.Transport == nil {
		result.Transport = &sdk.TransportOptions{}
	} else {
		copied := *input.Transport
		result.Transport = &copied
	}
	if len(input.Before) > 32 || len(input.After) > 32 {
		return result, failure(ErrInput, "hooks")
	}
	result.Before, result.After = slices.Clone(input.Before), slices.Clone(input.After)
	for _, hook := range result.Before {
		if hook == nil {
			return result, failure(ErrInput, "hook")
		}
	}
	for _, hook := range result.After {
		if hook == nil {
			return result, failure(ErrInput, "hook")
		}
	}
	if result.TLS.GetCertificate != nil || result.TLS.GetConfigForClient != nil || result.TLS.WrapSession != nil || result.TLS.UnwrapSession != nil {
		return result, failure(ErrUnsupported, "server-tls-callback")
	}
	if input.Pinner != nil {
		pins, automatic, err := input.Pinner.BoundedPinsSnapshot(256, 32)
		if err != nil {
			return result, failure(ErrInput, "pins", err)
		}
		if automatic {
			return result, failure(ErrUnsupported, "automatic-pin-probe")
		}
		if len(pins) > 256 {
			return result, failure(ErrInput, "pins")
		}
		result.Pinner = sdk.NewPinner(false)
		canonical := make(map[string][]string, len(pins))
		for host, values := range pins {
			target, err := canonicalEndpoint(host)
			if err != nil || len(values) == 0 || len(values) > 32 {
				return result, failure(ErrInput, "pins", err)
			}
			for _, value := range values {
				decoded, err := base64.StdEncoding.DecodeString(value)
				if err != nil || len(decoded) != 32 {
					return result, failure(ErrInput, "pin-value", err)
				}
			}
			if previous, exists := canonical[target]; exists && !slices.Equal(previous, values) {
				return result, failure(ErrInput, "pin-conflict")
			}
			canonical[target] = slices.Clone(values)
		}
		for host, values := range canonical {
			result.Pinner.AddPins(host, values)
		}
	}
	return result, nil
}

func cloneTLS(input *tls.Config) *tls.Config {
	if input == nil {
		return &tls.Config{}
	}
	result := input.Clone()
	result.NextProtos = slices.Clone(input.NextProtos)
	result.CipherSuites = slices.Clone(input.CipherSuites)
	result.CurvePreferences = slices.Clone(input.CurvePreferences)
	result.EncryptedClientHelloConfigList = slices.Clone(input.EncryptedClientHelloConfigList)
	result.ApplicationSettings = make(map[string][]byte, len(input.ApplicationSettings))
	for key, value := range input.ApplicationSettings {
		result.ApplicationSettings[key] = slices.Clone(value)
	}
	result.Certificates = slices.Clone(input.Certificates)
	for index := range result.Certificates {
		certificate := &result.Certificates[index]
		certificate.Certificate = cloneBytes(certificate.Certificate)
		certificate.SupportedSignatureAlgorithms = slices.Clone(certificate.SupportedSignatureAlgorithms)
		certificate.OCSPStaple = slices.Clone(certificate.OCSPStaple)
		certificate.SignedCertificateTimestamps = cloneBytes(certificate.SignedCertificateTimestamps)
	}
	result.EncryptedClientHelloKeys = slices.Clone(input.EncryptedClientHelloKeys)
	for index := range result.EncryptedClientHelloKeys {
		result.EncryptedClientHelloKeys[index].Config = slices.Clone(input.EncryptedClientHelloKeys[index].Config)
		result.EncryptedClientHelloKeys[index].PrivateKey = slices.Clone(input.EncryptedClientHelloKeys[index].PrivateKey)
	}
	if input.RootCAs != nil {
		result.RootCAs = input.RootCAs.Clone()
	}
	if input.ClientCAs != nil {
		result.ClientCAs = input.ClientCAs.Clone()
	}
	return result
}
func cloneBytes(input [][]byte) [][]byte {
	output := make([][]byte, len(input))
	for index := range input {
		output[index] = slices.Clone(input[index])
	}
	return output
}

func canonicalEndpoint(input string) (string, error) {
	host, port, err := net.SplitHostPort(input)
	if err != nil || host == "" || port == "" {
		return "", failure(ErrInput, "endpoint", err)
	}
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil || number == 0 {
		return "", failure(ErrInput, "endpoint-port", err)
	}
	host, err = canonicalHost(host)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(host, strconv.FormatUint(number, 10)), nil
}

func canonicalHost(host string) (string, error) {
	if address, err := netip.ParseAddr(host); err == nil {
		return address.Unmap().String(), nil
	}
	ascii, err := idna.Lookup.ToASCII(strings.TrimSuffix(host, "."))
	ascii = strings.TrimSuffix(strings.ToLower(ascii), ".")
	if err != nil || ascii == "" {
		return "", failure(ErrInput, "hostname", err)
	}
	return ascii, nil
}

func validateNative(value settings, native NativeOptionsV1) error {
	options := native.Transport
	if options.DisableIPV4 && options.DisableIPV6 || options.DisableHTTP3 && options.ForceHTTP3 ||
		options.HTTP3RaceDelay < 0 || options.HTTP3RaceDelay > value.Timeout ||
		options.MaxResponseHeaderBytes < 0 || options.MaxResponseHeaderBytes > value.MaxNativeHeaderBytes ||
		options.MaxCachedOrigins < 0 || options.MaxCachedOrigins > value.MaxOrigins ||
		options.IdleConnTimeout < 0 || options.IdleConnTimeout > value.IdleConnTimeout {
		return failure(ErrInput, "native-limits")
	}
	if (value.Mode == HTTP3Only || options.ForceHTTP3) && (native.Profile.H3 == nil || options.DisableHTTP3) ||
		(value.Mode == HTTP1Only || value.Mode == HTTP2Negotiated) && options.ForceHTTP3 {
		return failure(ErrInput, "protocol-options")
	}
	if native.QUIC.MaxIncomingStreams > 4096 || native.QUIC.MaxIncomingUniStreams > 4096 ||
		native.QUIC.MaxConnectionReceiveWindow > 64<<20 || native.QUIC.MaxStreamReceiveWindow > 64<<20 ||
		native.QUIC.InitialConnectionReceiveWindow > 64<<20 || native.QUIC.InitialStreamReceiveWindow > 64<<20 {
		return failure(ErrInput, "quic-windows")
	}
	return nil
}

type activity struct {
	mu     sync.Mutex
	closed bool
	work   sync.WaitGroup
}

func (activity *activity) enter() bool {
	activity.mu.Lock()
	defer activity.mu.Unlock()
	if activity.closed {
		return false
	}
	activity.work.Add(1)
	return true
}
func (activity *activity) leave() { activity.work.Done() }
func (activity *activity) stop() {
	activity.mu.Lock()
	activity.closed = true
	activity.mu.Unlock()
	activity.work.Wait()
}

func (owner *owner) protectNative(native NativeOptionsV1) NativeOptionsV1 {
	profile := native.Profile
	factory := profile.ClientHelloSpec
	profile.ClientHelloSpec = func() *tls.ClientHelloSpec {
		if !owner.callbacks.enter() {
			return nil
		}
		defer owner.callbacks.leave()
		spec := factory()
		if spec == nil {
			return nil
		}
		if owner.settings.Mode == HTTP1Only {
			for _, extension := range spec.Extensions {
				if alpn, ok := extension.(*tls.ALPNExtension); ok {
					alpn.AlpnProtocols = []string{"http/1.1"}
				}
			}
		}
		return spec
	}
	if owner.settings.Mode == HTTP1Only {
		profile.H2, profile.H3 = nil, nil
	}
	if profile.H2 != nil && profile.H2.HeaderPriority != nil {
		callback := profile.H2.HeaderPriority
		profile.H2.HeaderPriority = func(req *http.Request) http2.PriorityParam {
			if !owner.callbacks.enter() {
				return http2.PriorityParam{}
			}
			defer owner.callbacks.leave()
			return callback(metadataRequest(req))
		}
	}
	config := native.TLS
	if callback := config.VerifyPeerCertificate; callback != nil {
		config.VerifyPeerCertificate = func(raw [][]byte, chains [][]*x509.Certificate) error {
			if !owner.callbacks.enter() {
				return failure(ErrState, "tls-callback")
			}
			defer owner.callbacks.leave()
			return callback(raw, chains)
		}
	}
	if callback := config.VerifyConnection; callback != nil {
		config.VerifyConnection = func(state tls.ConnectionState) error {
			if !owner.callbacks.enter() {
				return failure(ErrState, "tls-callback")
			}
			defer owner.callbacks.leave()
			return callback(state)
		}
	}
	if callback := config.EncryptedClientHelloRejectionVerify; callback != nil {
		config.EncryptedClientHelloRejectionVerify = func(state tls.ConnectionState) error {
			if !owner.callbacks.enter() {
				return failure(ErrState, "tls-callback")
			}
			defer owner.callbacks.leave()
			return callback(state)
		}
	}
	if callback := config.GetClientCertificate; callback != nil {
		config.GetClientCertificate = func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			if !owner.callbacks.enter() {
				return nil, failure(ErrState, "tls-callback")
			}
			defer owner.callbacks.leave()
			return callback(info)
		}
	}
	if callback := config.Time; callback != nil {
		config.Time = func() time.Time {
			if !owner.callbacks.enter() {
				return time.Time{}
			}
			defer owner.callbacks.leave()
			return callback()
		}
	}
	if config.Rand != nil {
		config.Rand = guardedReader{owner, config.Rand}
	}
	if config.KeyLogWriter != nil {
		config.KeyLogWriter = guardedWriter{owner, config.KeyLogWriter}
	}
	if config.ClientSessionCache != nil {
		config.ClientSessionCache = guardedCache{owner, config.ClientSessionCache}
	}
	if native.Jar != nil {
		native.Jar = &guardedJar{owner: owner, jar: native.Jar}
	}
	if native.Tracker != nil {
		native.Tracker = guardedTracker{owner, native.Tracker}
	}
	if callback := native.AllowConnectionWindowIncrease; callback != nil {
		native.QUIC.AllowConnectionWindowIncrease = func(conn *quic.Conn, delta uint64) bool {
			if !owner.callbacks.enter() {
				return false
			}
			defer owner.callbacks.leave()
			return callback(conn.Context(), delta)
		}
	}
	if callback := native.QUIC.Tracer; callback != nil {
		native.QUIC.Tracer = func(ctx context.Context, client bool, id quic.ConnectionID) qlogwriter.Trace {
			if !owner.callbacks.enter() {
				return nil
			}
			defer owner.callbacks.leave()
			trace := callback(ctx, client, id)
			if trace == nil {
				return nil
			}
			return guardedTrace{owner, trace}
		}
	}
	return native
}

type guardedReader struct {
	owner *owner
	io.Reader
}

func (reader guardedReader) Read(data []byte) (int, error) {
	if !reader.owner.callbacks.enter() {
		return 0, failure(ErrState, "native-read")
	}
	defer reader.owner.callbacks.leave()
	return reader.Reader.Read(data)
}

type guardedWriter struct {
	owner *owner
	io.Writer
}

func (writer guardedWriter) Write(data []byte) (int, error) {
	if !writer.owner.callbacks.enter() {
		return 0, failure(ErrState, "native-write")
	}
	defer writer.owner.callbacks.leave()
	return writer.Writer.Write(data)
}

type guardedCache struct {
	owner *owner
	tls.ClientSessionCache
}

func (cache guardedCache) Get(key string) (*tls.ClientSessionState, bool) {
	if !cache.owner.callbacks.enter() {
		return nil, false
	}
	defer cache.owner.callbacks.leave()
	return cache.ClientSessionCache.Get(key)
}
func (cache guardedCache) Put(key string, state *tls.ClientSessionState) {
	if !cache.owner.callbacks.enter() {
		return
	}
	defer cache.owner.callbacks.leave()
	cache.ClientSessionCache.Put(key, state)
}

type guardedTracker struct {
	owner *owner
	bandwidth.Tracker
}

func (tracker guardedTracker) AddReadBytes(count int64) {
	if !tracker.owner.callbacks.enter() {
		return
	}
	defer tracker.owner.callbacks.leave()
	tracker.Tracker.AddReadBytes(count)
}
func (tracker guardedTracker) AddWriteBytes(count int64) {
	if !tracker.owner.callbacks.enter() {
		return
	}
	defer tracker.owner.callbacks.leave()
	tracker.Tracker.AddWriteBytes(count)
}

type guardedTrace struct {
	owner *owner
	qlogwriter.Trace
}

func (trace guardedTrace) SupportsSchemas(schema string) bool {
	if !trace.owner.callbacks.enter() {
		return false
	}
	defer trace.owner.callbacks.leave()
	return trace.Trace.SupportsSchemas(schema)
}
func (trace guardedTrace) AddProducer() qlogwriter.Recorder {
	if !trace.owner.callbacks.enter() {
		return nil
	}
	defer trace.owner.callbacks.leave()
	recorder := trace.Trace.AddProducer()
	if recorder == nil {
		return nil
	}
	guarded := &guardedRecorder{owner: trace.owner, recorder: recorder}
	trace.owner.mu.Lock()
	trace.owner.recorders[guarded] = struct{}{}
	trace.owner.mu.Unlock()
	return guarded
}

type guardedRecorder struct {
	owner    *owner
	recorder qlogwriter.Recorder
	mu       sync.Mutex
	closed   bool
	released bool
	closeErr error
}

func (recorder *guardedRecorder) RecordEvent(event qlogwriter.Event) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.closed || !recorder.owner.callbacks.enter() {
		return
	}
	defer recorder.owner.callbacks.leave()
	recorder.recorder.RecordEvent(event)
}
func (recorder *guardedRecorder) Close() error {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.released {
		return recorder.closeErr
	}
	recorder.closed = true
	recorder.closeErr = recorder.recorder.Close()
	released := recorder.closeErr == nil
	if confirmation, ok := recorder.recorder.(interface{ ReleaseConfirmed() bool }); ok {
		released = confirmation.ReleaseConfirmed()
	}
	if released {
		recorder.released = true
		recorder.owner.mu.Lock()
		delete(recorder.owner.recorders, recorder)
		recorder.owner.mu.Unlock()
	}
	if recorder.closeErr != nil {
		recorder.owner.recordCleanup(recorder.closeErr)
	}
	return recorder.closeErr
}

func callbackFailure(recovered any) error {
	if cause, ok := recovered.(error); ok {
		return failure(ErrCallback, "callback", cause)
	}
	return failure(ErrCallback, "callback")
}

func closedOnly(err error) bool {
	if err == nil || err == net.ErrClosed || err == io.ErrClosedPipe {
		return true
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		causes := wrapped.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !closedOnly(cause) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		cause := wrapped.Unwrap()
		return cause != nil && closedOnly(cause)
	}
	return false
}
