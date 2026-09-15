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

package tlsclient

import (
	"crypto"
	"crypto/x509"
	"io"
	"maps"
	"slices"
	"time"

	"github.com/bogdanfinn/fhttp/http2"
	sdk "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	tls "github.com/bogdanfinn/utls"
)

func copyNative(input NativeOptionsV1) (NativeOptionsV1, error) {
	if input.Profile == nil || nilLike(input.Jar) {
		return NativeOptionsV1{}, failure(ErrInput, "native-profile")
	}
	if !headerFits(input.DefaultHeaders, 1<<20, true) || !connectHeadersFit(input.ConnectHeaders, 1<<20) {
		return NativeOptionsV1{}, failure(ErrLimit, "native-headers")
	}
	result := input
	profile := *input.Profile
	if len(profile.GetSettings()) > 256 || len(profile.GetSettingsOrder()) > 256 || len(profile.GetPriorities()) > 256 || len(profile.GetPseudoHeaderOrder()) > 32 ||
		len(profile.GetHttp3Settings()) > 256 || len(profile.GetHttp3SettingsOrder()) > 256 || len(profile.GetHttp3PseudoHeaderOrder()) > 32 {
		return NativeOptionsV1{}, failure(ErrLimit, "profile")
	}
	for _, order := range [][]string{profile.GetPseudoHeaderOrder(), profile.GetHttp3PseudoHeaderOrder()} {
		var total int
		for _, field := range order {
			total += len(field)
			if total > 64<<10 {
				return NativeOptionsV1{}, failure(ErrLimit, "profile-order")
			}
		}
	}
	id := profile.GetClientHelloId()
	if len(id.Client) > 128 || len(id.Version) > 128 || id.SpecFactory == nil {
		return NativeOptionsV1{}, failure(ErrInput, "profile-identity")
	}
	if id.Seed != nil {
		copy := *id.Seed
		id.Seed = &copy
	}
	if id.Weights != nil {
		copy := *id.Weights
		id.Weights = &copy
	}
	priority := profile.GetHeaderPriority()
	if priority != nil {
		copy := *priority
		priority = &copy
	}
	cloned := profiles.NewClientProfile(id, maps.Clone(profile.GetSettings()), slices.Clone(profile.GetSettingsOrder()), slices.Clone(profile.GetPseudoHeaderOrder()),
		profile.GetConnectionFlow(), slices.Clone(profile.GetPriorities()), priority, profile.GetStreamID(), profile.GetAllowHTTP(),
		maps.Clone(profile.GetHttp3Settings()), slices.Clone(profile.GetHttp3SettingsOrder()), profile.GetHttp3PriorityParam(), slices.Clone(profile.GetHttp3PseudoHeaderOrder()), profile.GetHttp3SendGreaseFrames())
	result.Profile = &cloned
	if input.Transport != nil {
		transport := *input.Transport
		if transport.RootCAs != nil {
			transport.RootCAs = transport.RootCAs.Clone()
		}
		if transport.IdleConnTimeout != nil {
			duration := *transport.IdleConnTimeout
			transport.IdleConnTimeout = &duration
		}
		if nilLike(transport.KeyLogWriter) {
			return NativeOptionsV1{}, failure(ErrInput, "key-log")
		}
		if len(transport.Certificates) > 32 {
			return NativeOptionsV1{}, failure(ErrLimit, "certificates")
		}
		transport.Certificates = make([]tls.Certificate, len(input.Transport.Certificates))
		for index, cert := range input.Transport.Certificates {
			cloned, err := copyCertificate(cert)
			if err != nil {
				return NativeOptionsV1{}, err
			}
			transport.Certificates[index] = cloned
		}
		result.Transport = &transport
	}
	if len(input.PreHooks) > 32 || len(input.PostHooks) > 32 || len(input.CertificatePins) > 256 {
		return NativeOptionsV1{}, failure(ErrLimit, "native-options")
	}
	result.PreHooks = slices.Clone(input.PreHooks)
	result.PostHooks = slices.Clone(input.PostHooks)
	for _, hook := range result.PreHooks {
		if hook == nil {
			return NativeOptionsV1{}, failure(ErrInput, "pre-hook")
		}
	}
	for _, hook := range result.PostHooks {
		if hook == nil {
			return NativeOptionsV1{}, failure(ErrInput, "post-hook")
		}
	}
	result.DefaultHeaders = input.DefaultHeaders.Clone()
	result.ConnectHeaders = input.ConnectHeaders.Clone()
	result.CertificatePins = make(map[string][]string, len(input.CertificatePins))
	for host, pins := range input.CertificatePins {
		if len(host) > 8192 || !fieldValue(host) || len(pins) > 64 {
			return NativeOptionsV1{}, failure(ErrLimit, "certificate-pins")
		}
		for _, pin := range pins {
			if len(pin) > 1024 {
				return NativeOptionsV1{}, failure(ErrLimit, "certificate-pin")
			}
		}
		result.CertificatePins[host] = slices.Clone(pins)
	}
	return result, nil
}
func copyCertificate(cert tls.Certificate) (tls.Certificate, error) {
	if nilLike(cert.PrivateKey) || len(cert.Certificate) > 32 || len(cert.OCSPStaple) > 1<<20 || len(cert.SignedCertificateTimestamps) > 128 || len(cert.SupportedSignatureAlgorithms) > 256 {
		return tls.Certificate{}, failure(ErrInput, "certificate")
	}
	result := cert
	result.Certificate = make([][]byte, len(cert.Certificate))
	size := len(cert.OCSPStaple)
	for index, data := range cert.Certificate {
		size += len(data)
		if size > 1<<20 {
			return tls.Certificate{}, failure(ErrLimit, "certificate")
		}
		result.Certificate[index] = slices.Clone(data)
	}
	result.OCSPStaple = slices.Clone(cert.OCSPStaple)
	result.SignedCertificateTimestamps = make([][]byte, len(cert.SignedCertificateTimestamps))
	for index, data := range cert.SignedCertificateTimestamps {
		size += len(data)
		if size > 1<<20 {
			return tls.Certificate{}, failure(ErrLimit, "certificate")
		}
		result.SignedCertificateTimestamps[index] = slices.Clone(data)
	}
	result.SupportedSignatureAlgorithms = slices.Clone(cert.SupportedSignatureAlgorithms)
	if cert.Leaf != nil {
		if len(cert.Leaf.Raw) > 1<<20 {
			return tls.Certificate{}, failure(ErrLimit, "certificate")
		}
		leaf, err := x509.ParseCertificate(slices.Clone(cert.Leaf.Raw))
		if err != nil {
			return tls.Certificate{}, failure(ErrInput, "certificate", err)
		}
		result.Leaf = leaf
	}
	return result, nil
}
func validateNative(value settings, native NativeOptionsV1) error {
	id := native.Profile.GetClientHelloId()
	if !sdk.FathomryNativeSpecFactory(id.SpecFactory) {
		switch id.Client {
		case tls.HelloGolang.Client, tls.HelloRandomized.Client, tls.HelloRandomizedALPN.Client, tls.HelloRandomizedNoALPN.Client, tls.HelloCustom.Client:
			return failure(ErrUnsupported, "native-profile-factory")
		}
	}
	if value.Mode == HTTP1Only && sdk.FathomryNativeSpecFactory(id.SpecFactory) &&
		(id.Client == tls.HelloRandomized.Client || id.Client == tls.HelloRandomizedALPN.Client) {
		return failure(ErrUnsupported, "native-randomized-http1")
	}
	if value.Mode == HTTP3Racing && (native.DialContext != nil || native.ProxyDialerFactory != nil || len(native.CertificatePins) > 0 || value.DisableIPV4 || value.DisableIPV6 ||
		native.Transport != nil && (native.Transport.KeyLogWriter != nil || native.Transport.DisableKeepAlives)) {
		return failure(ErrUnsupported, "tcp-only-native-option")
	}
	if value.Mode != HTTP1Only {
		limit := native.Profile.GetSettings()[http2.SettingMaxHeaderListSize]
		if limit == 0xffffffff {
			return failure(ErrUnsupported, "unbounded-http2-headers")
		}
		if limit == 0 {
			limit = 10 << 20
		}
		if int64(limit) > value.MaxNativeHeaderBytes {
			return failure(ErrLimit, "native-http2-headers")
		}
	}
	if !headerFits(native.DefaultHeaders, value.MaxHeaderBytes, true) || !headerFits(native.ConnectHeaders, value.MaxHeaderBytes, true) {
		return failure(ErrLimit, "native-headers")
	}
	if value.ProxyURL != "" && (native.DialContext != nil || native.ProxyDialerFactory != nil) || native.DialContext != nil && native.ProxyDialerFactory != nil {
		return failure(ErrInput, "native-proxy-conflict")
	}
	if value.InsecureSkipVerify && len(native.CertificatePins) > 0 {
		return failure(ErrInput, "certificate-pins")
	}
	if _, err := sdk.NewCertificatePinner(native.CertificatePins); err != nil {
		return failure(ErrInput, "certificate-pins", err)
	}
	if native.Transport != nil {
		transport := native.Transport
		if transport.IdleConnTimeout != nil && (*transport.IdleConnTimeout < time.Millisecond || *transport.IdleConnTimeout > value.IdleConnTimeout) {
			return failure(ErrInput, "native-idle-timeout")
		}
		for _, size := range []int{transport.MaxIdleConns, transport.MaxIdleConnsPerHost, transport.MaxConnsPerHost, transport.WriteBufferSize, transport.ReadBufferSize} {
			if size < 0 || size > 1<<20 {
				return failure(ErrInput, "native-transport")
			}
		}
		if transport.MaxResponseHeaderBytes < 0 {
			return failure(ErrUnsupported, "unbounded-native-headers")
		}
		if transport.MaxResponseHeaderBytes > value.MaxHeaderBytes {
			return failure(ErrLimit, "native-headers")
		}
	}
	return nil
}
func (own *owner) profile() profiles.ClientProfile {
	cloned, _ := copyNative(own.native)
	profile := *cloned.Profile
	id := profile.GetClientHelloId()
	factory := id.SpecFactory
	if !sdk.FathomryNativeSpecFactory(factory) {
		id.SpecFactory = func() (tls.ClientHelloSpec, error) {
			if !own.callbacks.enter() {
				return tls.ClientHelloSpec{}, failure(ErrState, "profile-factory")
			}
			defer own.callbacks.leave()
			spec, err := factory()
			if len(spec.Extensions) > 256 || len(spec.CipherSuites) > 512 || len(spec.CompressionMethods) > 64 {
				return tls.ClientHelloSpec{}, failure(ErrLimit, "profile-spec", err)
			}
			return spec, err
		}
	}
	return profiles.NewClientProfile(id, profile.GetSettings(), profile.GetSettingsOrder(), profile.GetPseudoHeaderOrder(), profile.GetConnectionFlow(), profile.GetPriorities(), profile.GetHeaderPriority(),
		profile.GetStreamID(), profile.GetAllowHTTP(), profile.GetHttp3Settings(), profile.GetHttp3SettingsOrder(), profile.GetHttp3PriorityParam(), profile.GetHttp3PseudoHeaderOrder(), profile.GetHttp3SendGreaseFrames())
}

type keyWriter struct {
	own    *owner
	native io.Writer
}

func (writer keyWriter) Write(data []byte) (int, error) {
	if !writer.own.callbacks.enter() {
		return 0, failure(ErrState, "key-log")
	}
	defer writer.own.callbacks.leave()
	return writer.native.Write(data)
}

type guardedSigner struct {
	own    *owner
	native crypto.Signer
}

func (key guardedSigner) Public() crypto.PublicKey {
	if !key.own.callbacks.enter() {
		return nil
	}
	defer key.own.callbacks.leave()
	return key.native.Public()
}
func (key guardedSigner) Sign(random io.Reader, digest []byte, options crypto.SignerOpts) ([]byte, error) {
	if !key.own.callbacks.enter() {
		return nil, failure(ErrState, "sign")
	}
	defer key.own.callbacks.leave()
	return key.native.Sign(random, digest, options)
}

type guardedMessageSigner struct {
	guardedSigner
	message crypto.MessageSigner
}

func (key guardedMessageSigner) SignMessage(random io.Reader, message []byte, options crypto.SignerOpts) ([]byte, error) {
	if !key.own.callbacks.enter() {
		return nil, failure(ErrState, "sign-message")
	}
	defer key.own.callbacks.leave()
	return key.message.SignMessage(random, message, options)
}
func (own *owner) transportOptions() *sdk.TransportOptions {
	copied, _ := copyNative(own.native)
	value := copied.Transport
	if value == nil {
		value = &sdk.TransportOptions{}
	}
	if value.MaxResponseHeaderBytes == 0 {
		value.MaxResponseHeaderBytes = own.settings.MaxHeaderBytes
	}
	duration := own.settings.IdleConnTimeout
	if value.IdleConnTimeout == nil {
		value.IdleConnTimeout = &duration
	}
	if value.KeyLogWriter != nil {
		value.KeyLogWriter = keyWriter{own, value.KeyLogWriter}
	}
	for index := range value.Certificates {
		if signer, ok := value.Certificates[index].PrivateKey.(crypto.Signer); ok {
			guarded := guardedSigner{own, signer}
			if message, ok := signer.(crypto.MessageSigner); ok {
				value.Certificates[index].PrivateKey = guardedMessageSigner{guarded, message}
			} else {
				value.Certificates[index].PrivateKey = guarded
			}
		}
	}
	return value
}
