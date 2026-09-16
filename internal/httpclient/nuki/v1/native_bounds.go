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
	for _, name := range native.Profile.PseudoHeaderOrder {
		if len(name) > 128 || !fieldValue(name) {
			return failure(ErrInput, "pseudo-header")
		}
	}
	if native.QUIC != nil && len(native.QUIC.Versions) > 16 {
		return failure(ErrLimit, "quic-versions")
	}
	config := native.TLS
	if config == nil {
		return nil
	}
	if len(config.Certificates) > 64 || len(config.NextProtos) > 32 || len(config.CipherSuites) > 512 || len(config.CurvePreferences) > 64 ||
		len(config.ApplicationSettings) > 64 || len(config.EncryptedClientHelloKeys) > 16 || config.NameToCertificate != nil {
		return failure(ErrLimit, "tls-containers")
	}
	bytes := int64(len(config.EncryptedClientHelloConfigList) + len(config.ServerName))
	for _, protocol := range config.NextProtos {
		bytes += int64(len(protocol)) + 32
	}
	for name, value := range config.ApplicationSettings {
		bytes += int64(len(name)+len(value)) + 64
	}
	for _, certificate := range config.Certificates {
		if len(certificate.Certificate) > 64 || len(certificate.SignedCertificateTimestamps) > 128 || len(certificate.SupportedSignatureAlgorithms) > 512 {
			return failure(ErrLimit, "certificate-containers")
		}
		bytes += int64(len(certificate.OCSPStaple)) + int64(len(certificate.SupportedSignatureAlgorithms))*2
		for _, value := range certificate.Certificate {
			bytes += int64(len(value)) + 32
		}
		for _, value := range certificate.SignedCertificateTimestamps {
			bytes += int64(len(value)) + 32
		}
	}
	for _, key := range config.EncryptedClientHelloKeys {
		bytes += int64(len(key.Config)+len(key.PrivateKey)) + 64
	}
	if bytes > 8<<20 {
		return failure(ErrLimit, "tls-bytes")
	}
	return nil
}
