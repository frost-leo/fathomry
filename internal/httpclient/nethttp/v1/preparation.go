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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"math"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

// Prepared owns inert, frozen data/native containers. Borrowed keys, callbacks,
// jars, caches and readers retain their documented concurrent-use obligations.
type Prepared struct {
	private
	configuration resource.Prepared[settings]
	native        NativeOptionsV1
	tls           *tls.Config
	metadata      Budget
}

// Budget is authoritative for this exact prepared selection. Bytes are declared
// local envelopes, not RSS, kernel memory or arbitrary foreign callback storage.
// Root work includes the selected H2 stream receive window; connection credit is
// not allocated memory. Source residence includes copied containers and bounded
// transports/sockets. Borrowed immutable certificate payloads and runtime objects
// are not claimed as privately allocated memory.
type Budget struct {
	Limits                                resource.Limits
	WorkBytes, EvidenceBytes, SourceBytes int64
	MaxConnections, MaxExchanges          int
	Timeout, AdmissionTimeout             time.Duration
}

func (prepared Prepared) Metadata() Budget { return prepared.metadata }
func (prepared Prepared) Description() resource.Description {
	return prepared.configuration.Description()
}

func (value settings) budget(native NativeOptionsV1, config *tls.Config) Budget {
	work := value.reservation()
	perConnection := int64(128<<10) + 2*value.MaxHeaderBytes + 8192
	if value.HTTP2 || value.UnencryptedHTTP2 {
		stream, frame, decoder, encoder := int64(4<<20), int64(1<<20), int64(4096), int64(4096)
		if h2 := native.HTTP2; h2 != nil {
			// Selected Go 1.27 internal/http2/config.go defines these effective
			// fallbacks, including invalid native values that mean defaults.
			effective := func(input, minimum, maximum, fallback int64) int64 {
				if input < minimum || input > maximum {
					return fallback
				}
				return input
			}
			stream = effective(int64(h2.MaxReceiveBufferPerStream), 1, math.MaxInt32, stream)
			frame = effective(int64(h2.MaxReadFrameSize), 1<<14, (1<<24)-1, frame)
			decoder = effective(int64(h2.MaxDecoderHeaderTableSize), 1, math.MaxInt32, decoder)
			encoder = effective(int64(h2.MaxEncoderHeaderTableSize), 1, math.MaxInt32, encoder)
		}
		work += stream
		perConnection += frame + decoder + encoder
	}
	encoded, _ := json.Marshal(value)
	// Frozen serialized preparation and independently decoded owner settings
	// coexist. Textual TLS additionally owns parsed trust/certificate data.
	configuration := 2*int64(len(encoded)) + 2*value.MaxHeaderBytes + 64<<10
	if native.TLS == nil {
		configuration += 4 * int64(len(value.RootCAPEM)+len(value.ClientCertPEM)+len(value.ClientKeyPEM))
	}
	configuration += tlsContainerBytes(config)
	limits := value.limits()
	limits.Bytes = int64(limits.Active) * work
	limits.QueuedBytes = int64(limits.Queued) * work
	return Budget{Limits: limits, WorkBytes: work, EvidenceBytes: value.evidenceBytes(),
		SourceBytes:    configuration + int64(value.MaxConnections)*perConnection,
		MaxConnections: value.MaxConnections, MaxExchanges: value.MaxExchanges,
		Timeout: value.Timeout, AdmissionTimeout: value.AdmissionTimeout}
}

func tlsContainerBytes(config *tls.Config) int64 {
	total := int64(len(config.ServerName)+len(config.EncryptedClientHelloConfigList)) + 4096
	total += int64(len(config.CipherSuites)+len(config.CurvePreferences)) * 8
	for _, protocol := range config.NextProtos {
		total += int64(len(protocol)) + 32
	}
	certificate := func(value tls.Certificate) int64 {
		size := int64(len(value.OCSPStaple)) + int64(len(value.SupportedSignatureAlgorithms))*8 + 1024
		for _, blob := range value.Certificate {
			size += int64(len(blob)) + 32
		}
		for _, blob := range value.SignedCertificateTimestamps {
			size += int64(len(blob)) + 32
		}
		if value.Leaf != nil {
			size += int64(len(value.Leaf.Raw))*4 + 4096
		}
		return size
	}
	for _, value := range config.Certificates {
		total += certificate(value)
	}
	for name, value := range config.NameToCertificate {
		total += int64(len(name)) + 64 + certificate(*value)
	}
	// CertPool.Clone shares immutable certificates but copies its indices.
	for _, pool := range []*x509.CertPool{config.RootCAs, config.ClientCAs} {
		if pool != nil {
			for _, subject := range pool.Subjects() {
				total += int64(len(subject)) + 256
			}
		}
	}
	// Preparation, owner and base/direct transport containers coexist; route
	// clones share immutable certificate bytes and are charged per connection.
	return 4 * total
}
