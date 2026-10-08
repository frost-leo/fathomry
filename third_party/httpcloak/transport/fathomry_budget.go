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

package transport

import (
	"errors"

	"github.com/sardanioss/httpcloak/fingerprint"
	"github.com/sardanioss/httpcloak/internal/h2build"
	"github.com/sardanioss/httpcloak/internal/h3build"
	"github.com/sardanioss/quic-go/quicvarint"
	utls "github.com/sardanioss/utls"
)

// FathomryBudget describes the actual managed preset's declared native state.
// It excludes arbitrary borrowed callback storage and kernel socket buffers.
type FathomryBudget struct {
	TLSStateBytes, TLSCompressionBytes                               int64
	WorkBytes, BindingBytes, QUICBytes                               int64
	MASQUEParserBytes                                                int64
	H3RetainedBytes                                                  int64
	H2DecoderBytes, H2EncoderBytes, H2FrameBytes, H2StreamBytes      uint64
	H2ConnectionBytes                                                uint64
	H3TableBytes, H3BlockedStreams, H3StreamBytes, H3ConnectionBytes uint64
	H3IncomingStreams, H3IncomingUniStreams                          int64
}

// FathomryNativeBudget is inert and uses the same native builders as construction.
func FathomryNativeBudget(preset *fingerprint.Preset, config *TransportConfig, protocol Protocol, maxHeaderBytes int64) (FathomryBudget, error) {
	if preset == nil || config == nil || maxHeaderBytes < 1024 || maxHeaderBytes > 1<<20 {
		return FathomryBudget{}, errors.New("httpcloak: missing prepared native configuration")
	}
	tlsState, tlsScratch, err := fathomryTLSBudget(preset, config, protocol)
	if err != nil {
		return FathomryBudget{}, err
	}
	result := FathomryBudget{TLSStateBytes: tlsState, TLSCompressionBytes: tlsScratch,
		BindingBytes: 2*tlsState + tlsScratch + 4*(256<<10) + 128<<10}
	switch protocol {
	case ProtocolHTTP1:
		result.WorkBytes = 64 << 10
	case ProtocolHTTP2:
		h2 := h2build.Transport(h2build.Options{Preset: preset, TLSOnly: config.TLSOnly, PseudoHeaderOrder: config.CustomPseudoOrder})
		receiveWindow, err := h2.FathomryReceiveConnectionWindow()
		if err != nil {
			return FathomryBudget{}, err
		}
		result.H2ConnectionBytes = uint64(receiveWindow)
		result.H2DecoderBytes = uint64(h2.MaxDecoderHeaderTableSize)
		result.H2EncoderBytes = uint64(h2.MaxEncoderHeaderTableSize)
		if result.H2EncoderBytes == 0 {
			result.H2EncoderBytes = 4096
		}
		result.H2FrameBytes = uint64(h2.MaxReadFrameSize)
		if result.H2FrameBytes < 1<<14 {
			result.H2FrameBytes = 1 << 14
		} else if result.H2FrameBytes > (1<<24)-1 {
			result.H2FrameBytes = (1 << 24) - 1
		}
		result.H2StreamBytes = uint64(preset.HTTP2Settings.InitialWindowSize)
		if result.H2DecoderBytes > 64<<20 || result.H2EncoderBytes > 64<<20 || result.H2StreamBytes > 1<<30 {
			return FathomryBudget{}, errors.New("httpcloak: native H2 resource declaration exceeds bound")
		}
		result.WorkBytes = int64(result.H2StreamBytes)
		result.BindingBytes += int64(8*(result.H2DecoderBytes+result.H2EncoderBytes) + 2*result.H2FrameBytes + result.H2ConnectionBytes)
	case ProtocolHTTP3:
		result.MASQUEParserBytes = 64 * maxHeaderBytes
		quic := h3build.QUICConfig(h3build.QUICOptions{Preset: preset, IdleTimeout: config.QuicIdleTimeout})
		for identifier, value := range h3build.Settings(preset, 0x21, 0) {
			if identifier > quicvarint.Max || value > quicvarint.Max {
				return FathomryBudget{}, errors.New("httpcloak: H3 setting exceeds QUIC integer bound")
			}
		}
		if quic.MaxDatagramFrameSize > quicvarint.Max || preset.H3QUICConnectionIDLength() < 0 || preset.H3QUICConnectionIDLength() > 20 {
			return FathomryBudget{}, errors.New("httpcloak: invalid QUIC wire parameters")
		}
		result.H3TableBytes, result.H3BlockedStreams = preset.H3QPACKMaxTableCapacity(), preset.H3QPACKBlockedStreams()
		result.H3IncomingStreams, result.H3IncomingUniStreams = quic.MaxIncomingStreams, quic.MaxIncomingUniStreams
		if result.H3IncomingStreams == 0 {
			result.H3IncomingStreams = 100
		} else if result.H3IncomingStreams < 0 {
			result.H3IncomingStreams = 0
		}
		if result.H3IncomingUniStreams == 0 {
			result.H3IncomingUniStreams = 100
		} else if result.H3IncomingUniStreams < 0 {
			result.H3IncomingUniStreams = 0
		}
		if result.H3TableBytes > 64<<20 || result.H3BlockedStreams > 1024 || result.H3IncomingStreams > 1024 || result.H3IncomingUniStreams > 1024 || quic.InitialStreamReceiveWindow > 1<<30 || quic.InitialConnectionReceiveWindow > 1<<30 {
			return FathomryBudget{}, errors.New("httpcloak: native H3 resource declaration exceeds bound")
		}
		result.H3StreamBytes = max(quic.InitialStreamReceiveWindow, 16<<20)
		result.H3ConnectionBytes = max(quic.InitialConnectionReceiveWindow, 24<<20)
		result.WorkBytes = int64(result.H3StreamBytes)
		result.QUICBytes = int64(result.H3ConnectionBytes) + 8192*(result.H3IncomingStreams+result.H3IncomingUniStreams+3) + (32+128)*65536 + tlsState + tlsScratch + 2<<20
		// A closed QUIC context releases a dialing slot before the binding and
		// its HTTP/3 decoder/control graph necessarily retire. Exclusive authority
		// bindings retain one inner client and, for MASQUE, one outer client;
		// retry removes and joins the old inner client before constructing another.
		headerState := int64(32*result.H3TableBytes+256*result.H3BlockedStreams) + 2*maxHeaderBytes
		result.H3RetainedBytes = 2*(result.QUICBytes+headerState) + result.MASQUEParserBytes + 132*65527 + 256<<10
		result.BindingBytes += result.H3RetainedBytes
	default:
		return FathomryBudget{}, ErrProtocol
	}
	return result, nil
}

func fathomryTLSBudget(preset *fingerprint.Preset, config *TransportConfig, protocol Protocol) (int64, int64, error) {
	certificate, scratch := int64(256<<10), int64(128<<10)
	for _, resumed := range []bool{false, true} {
		var spec *utls.ClientHelloSpec
		var err error
		if protocol == ProtocolHTTP3 {
			spec, _, err = fingerprint.ResolveQUICClientHelloSpec(preset, resumed, 0)
		} else {
			spec, _, err = fingerprint.ResolveClientHelloSpec(preset, config.CustomJA3, config.CustomJA3Extras, resumed, 0)
		}
		if err != nil {
			return 0, 0, err
		}
		for _, extension := range spec.Extensions {
			compressed, ok := extension.(*utls.UtlsCompressCertExtension)
			if !ok {
				continue
			}
			for _, algorithm := range compressed.Algorithms {
				switch algorithm {
				case utls.CertCompressionBrotli:
					certificate, scratch = (1<<24)-1, max(scratch, 16<<20)
				case utls.CertCompressionZlib:
					certificate = (1 << 24) - 1
				case utls.CertCompressionZstd:
					certificate, scratch = (1<<24)-1, max(scratch, (512<<20)+(16<<20))
				}
			}
		}
	}
	// uTLS decompressCert admits a uint24 expanded certificate, independently of
	// its small wire-message limit. The selected streaming zstd reader allows a
	// 512 MiB window and up to four concurrent block decoders, independently of
	// the expected certificate length. Its 64 GiB decoded limit is not this window.
	return 4*certificate + 2*(64<<10), scratch, nil
}
