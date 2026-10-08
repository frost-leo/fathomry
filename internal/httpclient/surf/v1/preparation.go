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

package surf

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"reflect"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

// Prepared freezes final data/native selection without constructing a client or
// invoking lazy profile functions. Opaque dependencies remain cooperative borrows.
type Prepared struct {
	private
	configuration resource.Prepared[settings]
	native        NativeOptionsV1
	metadata      Budget
}

// Budget contains authoritative declared envelopes, not heap/RSS or foreign
// callback memory. Lazy outputs are bounded at their owned installation boundary.
type Budget struct {
	Limits                                resource.Limits
	WorkBytes, EvidenceBytes, SourceBytes int64
	ProfileBytes, HTTP2StreamBytes        int64
	Timeout, AdmissionTimeout             time.Duration
}

func (prepared Prepared) Metadata() Budget { return prepared.metadata }
func (prepared Prepared) Description() resource.Description {
	return prepared.configuration.Description()
}

// ValidateDataV1 checks strict layers/defaults only, not separately supplied native inputs.
func ValidateDataV1(options OptionsV1, layers ...resource.Layer) error {
	version := options.Version
	if version == 0 {
		version = 1
	}
	_, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: validate},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: version, Layers: layers})
	return err
}
func (value settings) budget(native NativeOptionsV1) Budget {
	encoded, _ := json.Marshal(value)
	config := 2*int64(len(encoded)) + nativeContainerBytes(native) + 64<<10
	work := value.reservation() + 2*value.MaxProfileBytes + int64(value.MaxReplays+1)*MaxMultipartParts*(8<<10)
	if value.Mode != HTTP1Only {
		work += value.MaxHTTP2StreamBytes
	}
	if value.Mode == PreferHTTP3 {
		work += 6 << 20
	}
	standardTLS := int64(4*(256<<10) + 2*(64<<10))
	originTLS := standardTLS
	if usesJA(native) && value.Mode != PreferHTTP3 {
		originTLS = 4*((1<<24)-1) + 2*(64<<10)
	}
	if value.Mode == H2C {
		originTLS = 0
	}
	// Buffered ordinary requests and detached pool setup remain source-resident
	// until transport shutdown, separately from retained caller response records.
	tcp := int64(128<<10) + 2*value.MaxRequestBytes + originTLS + standardTLS + 2*value.MaxProfileBytes + 4*value.MaxNativeHeaderBytes
	if value.Mode != HTTP1Only {
		tcp += (1 << 24) - 1 + 2*value.MaxNativeHeaderBytes + 4096
	}
	// HTTPS proxy HTTP/2 is independent of the origin mode: its default tunnel
	// stream has 4 MiB credit and retains its own frame/codec/header buffers.
	tcp += 4<<20 + (1 << 24) - 1 + 2*value.MaxNativeHeaderBytes + 8192
	route := config + 2*value.MaxProfileBytes + 4*value.MaxHeaderBytes + 128<<10
	if value.Mode == PreferHTTP3 {
		route += 64 << 10 // Separate H1 fallback transport; sockets share the TCP quota.
	}
	source := config + int64(value.MaxRoutes)*route + int64(value.MaxTCPConnections)*tcp + int64(value.MaxUDPSockets)*(64<<10)
	if value.Mode == PreferHTTP3 {
		// Native H3 client quota covers cached, pending and retired clients, not UDP
		// sockets. Codec buffers and peer TLS metadata outlive individual responses.
		source += int64(value.MaxHTTP3Clients) * (15<<20 + 4*value.MaxNativeHeaderBytes + standardTLS + 256<<10)
	}
	limits := value.limits()
	limits.Bytes = int64(limits.Active) * work
	limits.QueuedBytes = int64(limits.Queued) * work
	return Budget{Limits: limits, WorkBytes: work, EvidenceBytes: value.evidenceBytes(), SourceBytes: source,
		ProfileBytes: value.MaxProfileBytes, HTTP2StreamBytes: value.MaxHTTP2StreamBytes, Timeout: value.Timeout, AdmissionTimeout: value.AdmissionTimeout}
}

const maxNativeContainerBytes int64 = 1 << 30

type nativeCharge struct{ total int64 }

func (charge *nativeCharge) add(count int, width int64) {
	if charge.total > maxNativeContainerBytes {
		return
	}
	if count < 0 || int64(count) > (maxNativeContainerBytes-charge.total)/width {
		charge.total = maxNativeContainerBytes + 1
		return
	}
	charge.total += int64(count) * width
}
func (charge *nativeCharge) certificate(chain [][]byte, ocsp []byte, sct [][]byte, signatures int, leaf *x509.Certificate) {
	charge.add(1, 4096)
	charge.add(len(chain), 32)
	charge.add(len(sct), 32)
	charge.add(len(ocsp), 4)
	charge.add(signatures, 8)
	for _, data := range chain {
		charge.add(len(data), 4)
	}
	for _, data := range sct {
		charge.add(len(data), 4)
	}
	if leaf != nil {
		charge.add(len(leaf.Raw), 16)
	}
}
func (charge *nativeCharge) pool(pool *x509.CertPool) {
	if pool != nil {
		charge.add(len(pool.Subjects()), 512)
	}
}
func nativeContainerBytes(value NativeOptionsV1) int64 {
	charge := nativeCharge{total: 64 << 10}
	for key, values := range value.Headers {
		charge.add(len(key), 4)
		charge.add(len(values), 128)
		for _, item := range values {
			charge.add(len(item), 4)
		}
	}
	if value.Profile != nil && value.Profile.HelloSpec != nil && value.HelloSpecFactory == nil {
		remaining := int64(1 << 20)
		_ = boundedValue(reflect.ValueOf(*value.Profile.HelloSpec), &remaining, make(map[uintptr]bool), 0)
		charge.add(int((1<<20)-remaining), 4)
	}
	for _, config := range []*tls.Config{value.TLSConfig, value.ProxyTLSConfig} {
		if config == nil {
			continue
		}
		charge.add(len(config.ServerName), 4)
		charge.add(len(config.EncryptedClientHelloConfigList), 4)
		charge.add(len(config.NextProtos), 64)
		charge.add(len(config.CipherSuites), 8)
		charge.add(len(config.CurvePreferences), 8)
		for _, proto := range config.NextProtos {
			charge.add(len(proto), 4)
		}
		charge.pool(config.RootCAs)
		charge.pool(config.ClientCAs)
		for _, cert := range config.Certificates {
			charge.certificate(cert.Certificate, cert.OCSPStaple, cert.SignedCertificateTimestamps, len(cert.SupportedSignatureAlgorithms), cert.Leaf)
		}
		for name, cert := range config.NameToCertificate {
			charge.add(len(name), 4)
			charge.add(1, 128)
			if cert != nil {
				charge.certificate(cert.Certificate, cert.OCSPStaple, cert.SignedCertificateTimestamps, len(cert.SupportedSignatureAlgorithms), cert.Leaf)
			}
		}
	}
	if config := value.JAConfig; config != nil {
		charge.add(len(config.ServerName), 4)
		charge.add(len(config.InsecureServerNameToVerify), 4)
		charge.add(len(config.EncryptedClientHelloConfigList), 4)
		charge.add(len(config.NextProtos), 64)
		charge.add(len(config.CipherSuites), 8)
		charge.add(len(config.CurvePreferences), 8)
		for _, proto := range config.NextProtos {
			charge.add(len(proto), 4)
		}
		charge.pool(config.RootCAs)
		charge.pool(config.ClientCAs)
		for _, cert := range config.Certificates {
			charge.certificate(cert.Certificate, cert.OCSPStaple, cert.SignedCertificateTimestamps, len(cert.SupportedSignatureAlgorithms), cert.Leaf)
		}
		for name, cert := range config.NameToCertificate {
			charge.add(len(name), 4)
			charge.add(1, 128)
			if cert != nil {
				charge.certificate(cert.Certificate, cert.OCSPStaple, cert.SignedCertificateTimestamps, len(cert.SupportedSignatureAlgorithms), cert.Leaf)
			}
		}
		for name, data := range config.ApplicationSettings {
			charge.add(len(name), 4)
			charge.add(len(data), 4)
			charge.add(1, 128)
		}
	}
	return charge.total
}
