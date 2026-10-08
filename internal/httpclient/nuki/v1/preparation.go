// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package nuki

import (
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/klauspost/compress/zstd"
	http2 "github.com/nukilabs/http/http2"
	quic "github.com/nukilabs/quic-go"
	http3 "github.com/nukilabs/quic-go/http3"
	sdk "github.com/nukilabs/tlsclient"
	nativeproxy "github.com/nukilabs/tlsclient/proxy"
	tls "github.com/nukilabs/utls"
)

// Prepared owns frozen data containers, not the borrowed callbacks or live sockets.
type Prepared struct {
	private
	configuration resource.Prepared[settings]
	native        NativeOptionsV1
	metadata      Budget
}

// Budget separates one root, its record and the whole source residence declaration.
type Budget struct {
	Limits                                 resource.Limits
	WorkBytes, EvidenceBytes, SourceBytes  int64
	Timeout, AdmissionTimeout              time.Duration
	RequestPolicy                          RequestPolicyV1
	MaxProfileBytes                        int64
	MaxProxyTunnels                        int
	ProxyTunnelBytes                       int64
	TCPGraphBytes, H3GraphBytes            int64
	ProxyTCPGraphBytes                     int64
	ProxyH3GraphBytes, RetainedNativeBytes int64
	H2FrameBytes                           int64
}

func (prepared Prepared) Metadata() Budget { return prepared.metadata }
func (prepared Prepared) Description() resource.Description {
	return prepared.configuration.Description()
}

// ValidateDataV1 validates strict data without requiring or invoking a native profile.
func ValidateDataV1(options OptionsV1, layers ...resource.Layer) error {
	if err := validateDataEnvelope(options); err != nil {
		return err
	}
	version := options.Version
	if version == 0 {
		version = 1
	}
	_, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: validate},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: version, Layers: layers})
	return err
}

func validateDataEnvelope(options OptionsV1) error {
	if len(options.Mode) > 64 || len(options.ProxyURL) > 8192 {
		return failure(ErrLimit, "configuration-strings")
	}
	return nil
}

func (value settings) budget(native NativeOptionsV1) Budget {
	work := value.reservation()
	table, stream := int64(4096), int64(4<<20)
	var frameSize uint32
	if native.Profile.H2 != nil {
		if native.Profile.H2.Settings != nil {
			stream = 65535
		}
		for _, setting := range native.Profile.H2.Settings {
			switch setting.ID {
			case http2.SettingHeaderTableSize:
				table = int64(setting.Val)
			case http2.SettingInitialWindowSize:
				stream = int64(setting.Val)
			case http2.SettingMaxFrameSize:
				frameSize = setting.Val
			}
		}
	}
	tcpAllowed := value.Mode != HTTP3Only && !native.Transport.ForceHTTP3
	var frameBytes int64
	if tcpAllowed && value.Mode != HTTP1Only && native.Profile.H2 != nil {
		work += stream
		frameBytes = int64((&http2.Transport{MaxReadFrameSize: frameSize}).FathomryMaxReadFrameSize())
	} else {
		table = 0
	}
	config := int64(64<<10) + nativeDataBytes(native)
	cache := int64(0)
	if native.TLS.ClientSessionCache == nil {
		certificate := int64(4 * ((1 << 24) - 1))
		if value.Mode == HTTP3Only || native.Transport.ForceHTTP3 {
			certificate = 4 * (256 << 10)
		}
		cache = int64(sdk.FathomrySessionCacheCapacity) * (certificate + 128<<10)
	}
	// HPACK retains a field vector and two growing name indexes in addition to strings.
	tcpGraph := 128<<10 + 4*value.MaxNativeHeaderBytes + 2*value.MaxProfileBytes + 4*((1<<24)-1) + 32*table + 2*frameBytes
	// The selected proxy uses TLS.Client, not the origin's custom UClient profile,
	// and constructs an HTTP2 transport with default table/frame selections.
	proxyH2 := &http2.Transport{MaxHeaderListSize: uint32(value.MaxNativeHeaderBytes)}
	proxyTCPGraph := 128<<10 + 4*value.MaxNativeHeaderBytes + 4*(256<<10) +
		32*int64(proxyH2.FathomryMaxDecoderHeaderTableSize()) + 2*int64(proxyH2.FathomryMaxReadFrameSize())
	tcp := tcpGraph
	var retainedGraph int64
	if tcpAllowed {
		retainedGraph = tcpGraph + proxyTCPGraph
		// Selected uTLS permits a uint24 expanded certificate and uses zstd's
		// default streaming window with up to four block decoders. bytes.Reader
		// does not activate zstd's separate buffered DecodeAll shortcut.
		tcp += int64(zstd.MaxWindowSize) + 8<<20
	}
	origins := int64(0)
	var h3Graph, proxyH3Graph int64
	if native.Profile.H3 != nil && value.Mode != HTTP1Only && value.Mode != HTTP2Negotiated && !native.Transport.DisableHTTP3 {
		_, connection := quic.FathomryReceiveBounds(native.QUIC)
		table := uint64(0)
		for _, setting := range native.Profile.H3.Settings {
			if setting.ID == 1 {
				table = setting.Val
			}
		}
		// QPACK's bounded vector/string storage and encoder-parser scratch belong to the connection.
		controls := int64(http3.FathomryMaxControlWorkers) * (8 << 10)
		h3Graph = 128<<20 + 8*int64(table) + 8*value.MaxNativeHeaderBytes + 2*int64(connection) + 256<<10 + controls
		_, proxyConnection := quic.FathomryReceiveBounds(nil)
		proxyH3Graph = 128<<20 + 8*value.MaxNativeHeaderBytes + 2*int64(proxyConnection) + 256<<10 + controls
		origins = int64(value.MaxOrigins)*h3Graph + int64(value.MaxRoutes)*proxyH3Graph
		retainedGraph = max(retainedGraph, h3Graph+proxyH3Graph+nativeproxy.FathomryTunnelIngressBytes)
	}
	// Redirect response bodies retain their closed native connection graphs until
	// the whole operation ends; released sockets/cache slots do not reclaim them.
	retainedNative := int64(value.MaxExchanges) * retainedGraph
	work += retainedNative
	// Structured-field parameters retain descriptors/maps as well as wire bytes.
	proxyTunnel := tcp + 4<<20 + 66*value.MaxNativeHeaderBytes + nativeproxy.FathomryTunnelIngressBytes
	source := config + int64(value.MaxRoutes)*(config+cache+2*value.MaxProfileBytes+256<<10) +
		int64(value.MaxConnections)*tcp + origins + int64(value.MaxProxyTunnels)*proxyTunnel + int64(value.MaxRoutes)*proxyTCPGraph
	limits := value.limits()
	limits.Bytes, limits.QueuedBytes = int64(limits.Active)*work, int64(limits.Queued)*work
	return Budget{Limits: limits, WorkBytes: work, EvidenceBytes: value.evidenceBytes(), SourceBytes: source,
		Timeout: value.Timeout, AdmissionTimeout: value.AdmissionTimeout, RequestPolicy: value.requestPolicy(),
		MaxProfileBytes: value.MaxProfileBytes, MaxProxyTunnels: value.MaxProxyTunnels, ProxyTunnelBytes: proxyTunnel,
		TCPGraphBytes: tcpGraph, ProxyTCPGraphBytes: proxyTCPGraph, H3GraphBytes: h3Graph, ProxyH3GraphBytes: proxyH3Graph,
		RetainedNativeBytes: retainedNative, H2FrameBytes: frameBytes}
}

func nativeDataBytes(native NativeOptionsV1) int64 {
	total := int64(64 << 10)
	for _, config := range []*tls.Config{native.TLS, native.ProxyTLS} {
		if config == nil {
			continue
		}
		total += int64(len(config.ServerName)+len(config.EncryptedClientHelloConfigList)) + int64(len(config.CipherSuites))*2 + int64(len(config.CurvePreferences))*2
		for _, item := range config.NextProtos {
			total += int64(len(item)) + 32
		}
		for key, item := range config.ApplicationSettings {
			total += int64(len(key)+len(item)) + 64
		}
		for _, certificate := range config.Certificates {
			total += int64(len(certificate.OCSPStaple)) + int64(len(certificate.SupportedSignatureAlgorithms))*2 + 256
			for _, item := range certificate.Certificate {
				total += int64(len(item)) + 32
			}
			for _, item := range certificate.SignedCertificateTimestamps {
				total += int64(len(item)) + 32
			}
		}
		for _, key := range config.EncryptedClientHelloKeys {
			total += int64(len(key.Config)+len(key.PrivateKey)) + 64
		}
	}
	if native.Profile.H2 != nil {
		total += int64(len(native.Profile.H2.Settings))*32 + int64(len(native.Profile.H2.Priorities))*64
	}
	if native.Profile.H3 != nil {
		total += int64(len(native.Profile.H3.Settings)) * 32
	}
	for _, item := range native.Profile.PseudoHeaderOrder {
		total += int64(len(item)) + 32
	}
	if native.Pinner != nil {
		pins, _, _ := native.Pinner.BoundedPinsSnapshot(256, 32)
		for name, values := range pins {
			total += int64(len(name)) + 128
			for _, pin := range values {
				total += int64(len(pin)) + 64
			}
		}
	}
	return total
}
