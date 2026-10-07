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
	"encoding/json"
	"time"

	"github.com/bogdanfinn/fhttp/http2"
	sdk "github.com/bogdanfinn/tls-client"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Prepared is an inert frozen data/native selection. Custom profile factories
// are borrowed, never invoked here, and checked when native construction uses them.
// Their dynamic callbacks must honor the declared output envelope.
type Prepared struct {
	private
	configuration resource.Prepared[settings]
	native        NativeOptionsV1
	metadata      Budget
}

// Budget declares root, record and source-resident envelopes for this exact
// selection. It is not heap/RSS or arbitrary borrowed callback memory. Profile
// output, native buffers, peer TLS state and possible owned session caches are
// included even when no response is retained by an application.
type Budget struct {
	Limits                                resource.Limits
	WorkBytes, EvidenceBytes, SourceBytes int64
	ProfileBytes                          int64
	Timeout, AdmissionTimeout             time.Duration
}

func (prepared Prepared) Metadata() Budget { return prepared.metadata }
func (prepared Prepared) Description() resource.Description {
	return prepared.configuration.Description()
}

// ValidateDataV1 validates only strict data/default/layer semantics. Exact native
// validation and recommendations require PrepareV1 with an explicit profile.
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
	configuration := 2*int64(len(encoded)) + nativeContainerBytes(native) + 64<<10
	profileBytes := value.MaxProfileBytes
	if sdk.FathomryNativeSpecFactory(native.Profile.GetClientHelloId().SpecFactory) {
		// Native Go/randomized/preset construction does not call a supplied
		// factory. Use the protocol's 24-bit hello envelope, not its declaration.
		profileBytes = (1 << 24) - 1
	}
	work := value.reservation() + 2*profileBytes
	// Selected uTLS compressed certificates carry a 24-bit decoded length and
	// bypass the ordinary 256 KiB certificate-message path. Origin peer/cache
	// residence must cover that selected capability; proxy std TLS is separate.
	originTLS := int64(4*((1<<24)-1) + 2*(64<<10))
	proxyTLS := int64(4*(256<<10) + 2*(64<<10))
	tcp := int64(128<<10) + 2*value.MaxHeaderBytes + originTLS + proxyTLS + 2*profileBytes
	if transport := native.Transport; transport != nil {
		tcp += int64(max(transport.ReadBufferSize, 4096) + max(transport.WriteBufferSize, 4096))
	} else {
		tcp += 8192
	}
	if value.Mode != HTTP1Only {
		settings := native.Profile.GetSettings()
		window := int64(settings[http2.SettingInitialWindowSize])
		if window == 0 {
			window = 65535
		}
		table := int64(4096)
		if selected, ok := settings[http2.SettingHeaderTableSize]; ok {
			table = int64(selected)
		}
		if settings == nil {
			window = 6 << 20
			table = 65536
		}
		work += window
		// Selected fhttp's Framer retains the native 24-bit maximum, not the
		// profile's advertised frame size. HPACK saveBuf uses the header limit.
		tcp += (1 << 24) - 1 + 2*(value.MaxNativeHeaderBytes+8) + table + 4096
	}
	if value.Mode != HTTP3Racing {
		// A built-in HTTPS proxy can select HTTP/2 even for an HTTP/1 origin.
		// Runtime explicit routing is admitted, so include its independent H2
		// tunnel receive window, Framer and HPACK state regardless of ProxyURL.
		tcp += 65535 + (1 << 24) - 1 + 2*(value.MaxHeaderBytes+8) + 8192
	}
	binding := configuration + 128<<10
	if !value.DisableSessionTickets {
		// PSK support can depend on a lazy factory. Reserve the SDK's possible
		// 32-entry owned cache without calling that factory during preparation.
		binding += 32 * originTLS
	}
	source := configuration + int64(value.MaxBindings)*binding + int64(value.MaxTCPConnections)*tcp
	if value.Mode == HTTP3Racing {
		work += 6 << 20
		// Selected QUIC receive maxima: 6 MiB per stream and 15 MiB per
		// connection. QPACK has no dynamic table in this selected decoder.
		source += int64(value.MaxHTTP3Transports) * (15<<20 + 2*value.MaxNativeHeaderBytes + originTLS + 256<<10)
	}
	limits := value.limits()
	limits.Bytes = int64(limits.Active) * work
	limits.QueuedBytes = int64(limits.Queued) * work
	return Budget{Limits: limits, WorkBytes: work, EvidenceBytes: value.evidenceBytes(), SourceBytes: source,
		ProfileBytes: profileBytes, Timeout: value.Timeout, AdmissionTimeout: value.AdmissionTimeout}
}

func nativeContainerBytes(value NativeOptionsV1) int64 {
	total := int64(16 << 10)
	for _, header := range []map[string][]string{value.DefaultHeaders, value.ConnectHeaders} {
		for key, values := range header {
			total += int64(len(key)) + 64
			for _, item := range values {
				total += int64(len(item)) + 32
			}
		}
	}
	for host, pins := range value.CertificatePins {
		total += int64(len(host)) + 64
		for _, pin := range pins {
			total += int64(len(pin)) + 32
		}
	}
	profile := value.Profile
	total += int64(len(profile.GetSettings())+len(profile.GetHttp3Settings())+len(profile.GetSettingsOrder())+len(profile.GetHttp3SettingsOrder())+len(profile.GetPriorities())) * 64
	for _, fields := range [][]string{profile.GetPseudoHeaderOrder(), profile.GetHttp3PseudoHeaderOrder()} {
		for _, field := range fields {
			total += int64(len(field)) + 32
		}
	}
	total += int64(len(value.PreHooks)+len(value.PostHooks)) * 32
	if transport := value.Transport; transport != nil {
		if transport.RootCAs != nil {
			for _, subject := range transport.RootCAs.Subjects() {
				total += int64(len(subject)) + 256
			}
		}
		for _, cert := range transport.Certificates {
			total += int64(len(cert.OCSPStaple)+len(cert.SupportedSignatureAlgorithms)*8) + 4096
			for _, blob := range cert.Certificate {
				total += int64(len(blob)) + 32
			}
			for _, blob := range cert.SignedCertificateTimestamps {
				total += int64(len(blob)) + 32
			}
			if cert.Leaf != nil {
				total += 4 * int64(len(cert.Leaf.Raw))
			}
		}
	}
	return 4 * total
}
