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

func boundNativeContainers(native NativeOptionsV1) error {
	if len(native.Before) > MaxNativeHooks || len(native.After) > MaxNativeHooks {
		return failure(ErrInput, "hooks")
	}
	if len(native.Profile.PseudoHeaderOrder) > 16 {
		return failure(ErrLimit, "pseudo-headers")
	}
	for _, name := range native.Profile.PseudoHeaderOrder {
		if len(name) > 128 || !fieldValue(name) {
			return failure(ErrInput, "pseudo-header")
		}
	}
	if native.QUIC != nil && len(native.QUIC.Versions) > 16 {
		return failure(ErrLimit, "quic-versions")
	}
	if native.Profile.H2 != nil && (len(native.Profile.H2.Settings) > 128 || len(native.Profile.H2.Priorities) > 128) {
		return failure(ErrInput, "http2-profile")
	}
	if native.Profile.H3 != nil && len(native.Profile.H3.Settings) > 128 {
		return failure(ErrInput, "http3-profile")
	}
	config := native.TLS
	if config == nil {
		return nil
	}
	if len(config.Certificates) > 64 || len(config.NextProtos) > 32 || len(config.CipherSuites) > 512 || len(config.CurvePreferences) > 64 ||
		len(config.ApplicationSettings) > 64 || len(config.EncryptedClientHelloKeys) > 16 || config.NameToCertificate != nil {
		return failure(ErrLimit, "tls-containers")
	}
	remaining := int64(8 << 20)
	take := func(length int, overhead int64) bool {
		if overhead > remaining || int64(length) > remaining-overhead {
			return false
		}
		remaining -= int64(length) + overhead
		return true
	}
	if !take(len(config.EncryptedClientHelloConfigList), 0) || !take(len(config.ServerName), 0) {
		return failure(ErrLimit, "tls-bytes")
	}
	for _, protocol := range config.NextProtos {
		if !take(len(protocol), 32) {
			return failure(ErrLimit, "tls-bytes")
		}
	}
	for name, value := range config.ApplicationSettings {
		if !take(len(name), 64) || !take(len(value), 0) {
			return failure(ErrLimit, "tls-bytes")
		}
	}
	for _, certificate := range config.Certificates {
		if len(certificate.Certificate) > 64 || len(certificate.SignedCertificateTimestamps) > 128 || len(certificate.SupportedSignatureAlgorithms) > 512 {
			return failure(ErrLimit, "certificate-containers")
		}
		if !take(len(certificate.OCSPStaple), int64(len(certificate.SupportedSignatureAlgorithms))*2) {
			return failure(ErrLimit, "tls-bytes")
		}
		for _, value := range certificate.Certificate {
			if !take(len(value), 32) {
				return failure(ErrLimit, "tls-bytes")
			}
		}
		for _, value := range certificate.SignedCertificateTimestamps {
			if !take(len(value), 32) {
				return failure(ErrLimit, "tls-bytes")
			}
		}
	}
	for _, key := range config.EncryptedClientHelloKeys {
		if !take(len(key.Config), 64) || !take(len(key.PrivateKey), 0) {
			return failure(ErrLimit, "tls-bytes")
		}
	}
	return nil
}
