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

package nethttp

import (
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"io"
	"reflect"
	"slices"
	"time"
)

func copyNative(native NativeOptionsV1) (NativeOptionsV1, error) {
	if typedNil(native.Jar) {
		return NativeOptionsV1{}, failure(ErrInput, "native-jar")
	}
	var err error
	native.TLS, err = copyTLS(native.TLS)
	if err != nil {
		return NativeOptionsV1{}, err
	}
	if native.HTTP2 != nil {
		value := *native.HTTP2
		native.HTTP2 = &value
	}
	return native, nil
}

func copyTLS(original *tls.Config) (*tls.Config, error) {
	if original == nil {
		return nil, nil
	}
	config := original.Clone()
	for _, dependency := range []any{config.Rand, config.KeyLogWriter, config.ClientSessionCache} {
		if typedNil(dependency) {
			return nil, failure(ErrInput, "tls-native")
		}
	}
	if len(config.Certificates) > 64 || len(config.NameToCertificate) > 64 ||
		len(config.NextProtos) > 64 || len(config.CipherSuites) > 256 || len(config.CurvePreferences) > 256 ||
		len(config.EncryptedClientHelloConfigList) > 1<<20 || len(config.EncryptedClientHelloKeys) != 0 {
		return nil, failure(ErrInput, "tls-native")
	}
	if config.RootCAs != nil {
		config.RootCAs = config.RootCAs.Clone()
	}
	if config.ClientCAs != nil {
		config.ClientCAs = config.ClientCAs.Clone()
	}
	config.NextProtos = slices.Clone(config.NextProtos)
	for _, protocol := range config.NextProtos {
		if len(protocol) > 255 {
			return nil, failure(ErrInput, "alpn")
		}
	}
	config.CipherSuites = slices.Clone(config.CipherSuites)
	config.CurvePreferences = slices.Clone(config.CurvePreferences)
	config.EncryptedClientHelloConfigList = slices.Clone(config.EncryptedClientHelloConfigList)
	var err error
	config.Certificates = make([]tls.Certificate, len(original.Certificates))
	for index, certificate := range original.Certificates {
		config.Certificates[index], err = copyCertificate(certificate)
		if err != nil {
			return nil, err
		}
	}
	if original.NameToCertificate != nil {
		config.NameToCertificate = make(map[string]*tls.Certificate, len(original.NameToCertificate))
		for name, certificate := range original.NameToCertificate {
			if certificate == nil || len(name) > 1024 {
				return nil, failure(ErrInput, "certificate")
			}
			value, err := copyCertificate(*certificate)
			if err != nil {
				return nil, err
			}
			config.NameToCertificate[name] = &value
		}
	}
	return config, nil
}

func copyCertificate(certificate tls.Certificate) (tls.Certificate, error) {
	if typedNil(certificate.PrivateKey) {
		return tls.Certificate{}, failure(ErrInput, "private-key")
	}
	var err error
	certificate.Certificate, err = copyBlobs(certificate.Certificate)
	if err != nil {
		return tls.Certificate{}, err
	}
	certificate.SignedCertificateTimestamps, err = copyBlobs(certificate.SignedCertificateTimestamps)
	if err != nil {
		return tls.Certificate{}, err
	}
	if len(certificate.OCSPStaple) > 1<<20 || len(certificate.SupportedSignatureAlgorithms) > 256 {
		return tls.Certificate{}, failure(ErrInput, "certificate")
	}
	certificate.OCSPStaple = slices.Clone(certificate.OCSPStaple)
	certificate.SupportedSignatureAlgorithms = slices.Clone(certificate.SupportedSignatureAlgorithms)
	if certificate.Leaf != nil {
		if len(certificate.Leaf.Raw) > 1<<20 {
			return tls.Certificate{}, failure(ErrInput, "certificate")
		}
		certificate.Leaf, err = x509.ParseCertificate(slices.Clone(certificate.Leaf.Raw))
		if err != nil {
			return tls.Certificate{}, failure(ErrInput, "certificate", err)
		}
	}
	return certificate, nil
}

func typedNil(value any) bool {
	if value == nil {
		return false
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return reflected.IsNil()
	}
	return false
}
func copyBlobs(values [][]byte) ([][]byte, error) {
	if len(values) > 64 {
		return nil, failure(ErrInput, "certificate")
	}
	total := 0
	for _, value := range values {
		total += len(value)
		if total > 1<<20 {
			return nil, failure(ErrInput, "certificate")
		}
	}
	result := make([][]byte, len(values))
	for index, value := range values {
		result[index] = slices.Clone(value)
	}
	return result, nil
}

func (own *owner) guardTLS(config *tls.Config) *tls.Config {
	if callback := config.VerifyPeerCertificate; callback != nil {
		config.VerifyPeerCertificate = func(raw [][]byte, chains [][]*x509.Certificate) error {
			if !own.callbacks.enter() {
				return failure(ErrState, "tls-callback")
			}
			defer own.callbacks.leave()
			return callback(raw, chains)
		}
	}
	if callback := config.VerifyConnection; callback != nil {
		config.VerifyConnection = func(state tls.ConnectionState) error {
			if !own.callbacks.enter() {
				return failure(ErrState, "tls-callback")
			}
			defer own.callbacks.leave()
			return callback(state)
		}
	}
	if callback := config.GetClientCertificate; callback != nil {
		config.GetClientCertificate = func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			if !own.callbacks.enter() {
				return nil, failure(ErrState, "tls-callback")
			}
			defer own.callbacks.leave()
			certificate, err := callback(info)
			if err != nil {
				return nil, err
			}
			if certificate == nil {
				return nil, failure(ErrInput, "nil-certificate")
			}
			copied, err := copyCertificate(*certificate)
			if err != nil {
				return nil, err
			}
			own.guardCertificate(&copied)
			return &copied, nil
		}
	}
	if callback := config.EncryptedClientHelloRejectionVerify; callback != nil {
		config.EncryptedClientHelloRejectionVerify = func(state tls.ConnectionState) error {
			if !own.callbacks.enter() {
				return failure(ErrState, "tls-callback")
			}
			defer own.callbacks.leave()
			return callback(state)
		}
	}
	if config.Rand != nil {
		config.Rand = guardedReader{owner: own, reader: config.Rand}
	}
	if callback := config.Time; callback != nil {
		config.Time = func() time.Time {
			if !own.callbacks.enterPrompt() {
				return time.Time{}
			}
			defer own.callbacks.leave()
			return callback()
		}
	}
	if config.KeyLogWriter != nil {
		config.KeyLogWriter = guardedWriter{owner: own, writer: config.KeyLogWriter}
	}
	if config.ClientSessionCache != nil {
		config.ClientSessionCache = guardedCache{owner: own, cache: config.ClientSessionCache}
	}
	for index := range config.Certificates {
		own.guardCertificate(&config.Certificates[index])
	}
	for _, certificate := range config.NameToCertificate {
		own.guardCertificate(certificate)
	}
	return config
}

func (own *owner) guardCertificate(certificate *tls.Certificate) {
	if signer, ok := certificate.PrivateKey.(crypto.Signer); ok {
		wrapped := guardedSigner{owner: own, signer: signer}
		if full, ok := signer.(crypto.MessageSigner); ok {
			certificate.PrivateKey = guardedMessageSigner{guardedSigner: wrapped, full: full}
		} else {
			certificate.PrivateKey = wrapped
		}
	}
}

type guardedSigner struct {
	owner  *owner
	signer crypto.Signer
}

func (key guardedSigner) Public() crypto.PublicKey {
	if !key.owner.callbacks.enter() {
		return nil
	}
	defer key.owner.callbacks.leave()
	return key.signer.Public()
}
func (key guardedSigner) Sign(random io.Reader, digest []byte, options crypto.SignerOpts) ([]byte, error) {
	if !key.owner.callbacks.enter() {
		return nil, failure(ErrState, "sign")
	}
	defer key.owner.callbacks.leave()
	return key.signer.Sign(random, digest, options)
}

type guardedMessageSigner struct {
	guardedSigner
	full crypto.MessageSigner
}

func (key guardedMessageSigner) SignMessage(random io.Reader, message []byte, options crypto.SignerOpts) ([]byte, error) {
	if !key.owner.callbacks.enter() {
		return nil, failure(ErrState, "sign")
	}
	defer key.owner.callbacks.leave()
	return key.full.SignMessage(random, message, options)
}

type guardedReader struct {
	owner  *owner
	reader io.Reader
}

func (value guardedReader) Read(data []byte) (int, error) {
	if !value.owner.callbacks.enter() {
		return 0, failure(ErrState, "tls-read")
	}
	defer value.owner.callbacks.leave()
	return value.reader.Read(data)
}

type guardedWriter struct {
	owner  *owner
	writer io.Writer
}

func (value guardedWriter) Write(data []byte) (int, error) {
	if !value.owner.callbacks.enter() {
		return 0, failure(ErrState, "tls-write")
	}
	defer value.owner.callbacks.leave()
	return value.writer.Write(data)
}

type guardedCache struct {
	owner *owner
	cache tls.ClientSessionCache
}

func (value guardedCache) Get(key string) (*tls.ClientSessionState, bool) {
	if !value.owner.callbacks.enterPrompt() {
		return nil, false
	}
	defer value.owner.callbacks.leave()
	return value.cache.Get(key)
}
func (value guardedCache) Put(key string, state *tls.ClientSessionState) {
	if !value.owner.callbacks.enterPrompt() {
		return
	}
	defer value.owner.callbacks.leave()
	value.cache.Put(key, state)
}
