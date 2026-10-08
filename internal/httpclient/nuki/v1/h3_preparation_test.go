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
	"net"
	"testing"

	http2 "github.com/nukilabs/http/http2"
	quic "github.com/nukilabs/quic-go"
	http3 "github.com/nukilabs/quic-go/http3"
	nativeproxy "github.com/nukilabs/tlsclient/proxy"
	tls "github.com/nukilabs/utls"
)

func TestH3SettingsWireBoundsBeforeEffects(t *testing.T) {
	for _, test := range []struct {
		name     string
		settings []http3.Setting
		valid    bool
	}{
		{"oversized-id", []http3.Setting{{ID: 1 << 62, Val: 0}}, false},
		{"oversized-value", []http3.Setting{{ID: 0x41, Val: 1 << 62}}, false},
		{"oversized-grease", []http3.Setting{{ID: http3.SettingGrease, Val: 1 << 62}}, false},
		{"duplicate", []http3.Setting{{ID: 0x41, Val: 0}, {ID: 0x41, Val: 1}}, false},
		{"connect-bool", []http3.Setting{{ID: http3.SettingExtendedConnect, Val: 2}}, false},
		{"datagram-bool", []http3.Setting{{ID: http3.SettingH3Datagram, Val: 2}}, false},
		{"h2-reserved", []http3.Setting{{ID: 2, Val: 0}}, false},
		{"unknown-boundary", []http3.Setting{{ID: (1 << 62) - 1, Val: (1 << 62) - 1}}, true},
		{"grease-repeated", []http3.Setting{{ID: http3.SettingGrease, Val: 0}, {ID: http3.SettingGrease, Val: 1}}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := providerOptions()
			options.Mode = HTTP3Only
			profile := *options.Native.Profile.H3
			profile.Settings = test.settings
			options.Native.Profile.H3 = &profile
			var effects int
			options.Native.Profile.ClientHelloSpec = func() *tls.ClientHelloSpec { effects++; return nil }
			options.Native.DialContext = func(context.Context, string, string) (net.Conn, error) { effects++; return nil, nil }
			_, err := PrepareV1(options)
			if (err == nil) != test.valid {
				t.Fatalf("settings validity=%v want%v: %v", err == nil, test.valid, err)
			}
			if effects != 0 {
				t.Fatal("offline settings check invoked native work")
			}
		})
	}
}

func TestPreparedQUICBudgetFreezesActualInitialWindow(t *testing.T) {
	options := providerOptions()
	options.Mode = HTTP3Only
	baseline, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	options.Native.QUIC = &quic.Config{InitialConnectionReceiveWindow: 64 << 20, InitialStreamReceiveWindow: 64 << 20}
	larger, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	original := larger.Metadata()
	if original.SourceBytes <= baseline.Metadata().SourceBytes {
		t.Fatal("larger actual QUIC receive allocation disappeared from source budget")
	}
	if original.WorkBytes <= baseline.Metadata().WorkBytes || original.ProxyH3GraphBytes != baseline.Metadata().ProxyH3GraphBytes {
		t.Fatal("retained inner graph ignored its window or changed the proxy's native default")
	}
	options.Native.QUIC.InitialConnectionReceiveWindow = 1 << 20
	if larger.Metadata().SourceBytes != original.SourceBytes {
		t.Fatal("prepared QUIC budget changed after caller mutation")
	}
}

func TestPreparedRetainedNativeGraphCardinality(t *testing.T) {
	for _, mode := range []ProtocolMode{HTTP1Only, HTTP2Negotiated, HTTP3Only} {
		t.Run(string(mode), func(t *testing.T) {
			options := providerOptions()
			options.Mode, options.MaxExchanges, options.MaxActive, options.MaxOrigins = mode, 1, 1, 1
			first, err := PrepareV1(options)
			if err != nil {
				t.Fatal(err)
			}
			before := first.Metadata()
			unit := before.TCPGraphBytes + before.ProxyTCPGraphBytes
			if mode == HTTP3Only {
				unit = before.H3GraphBytes + before.ProxyH3GraphBytes + nativeproxy.FathomryTunnelIngressBytes
			} else if before.H3GraphBytes != 0 || before.ProxyH3GraphBytes != 0 {
				t.Fatal("TCP-only mode acquired H3 graph reservations")
			}
			if unit <= 0 || before.RetainedNativeBytes != unit {
				t.Fatal("first exchange omitted its retained native graph")
			}
			baseWork := defaults(options).reservation()
			options.MaxExchanges = 3
			three, err := PrepareV1(options)
			if err != nil {
				t.Fatal(err)
			}
			more := three.Metadata()
			if more.RetainedNativeBytes != 3*unit || more.SourceBytes != before.SourceBytes ||
				more.WorkBytes-before.WorkBytes != 2*unit+defaults(options).reservation()-baseWork {
				t.Fatal("retained response graphs were confused with cached source slots")
			}
			options.MaxActive = 2
			active, err := PrepareV1(options)
			if err != nil {
				t.Fatal(err)
			}
			if active.Metadata().WorkBytes != more.WorkBytes || active.Metadata().Limits.Bytes != 2*more.WorkBytes {
				t.Fatal("concurrent operations did not reserve their own retired graphs")
			}
			options.MaxOrigins = 2
			cached, err := PrepareV1(options)
			if err != nil {
				t.Fatal(err)
			}
			if cached.Metadata().WorkBytes != more.WorkBytes ||
				cached.Metadata().SourceBytes-active.Metadata().SourceBytes != more.H3GraphBytes {
				t.Fatal("cached origin cardinality changed per-call ownership")
			}
			if first.Metadata() != before {
				t.Fatal("later preparations changed the old frozen selection")
			}
		})
	}
}

func TestPreparedH2FrameAndTableGraphUsesSelectedProtocol(t *testing.T) {
	options := providerOptions()
	options.Mode, options.MaxExchanges = HTTP2Negotiated, 3
	profile := *options.Native.Profile.H2
	profile.Settings = []http2.Setting{{ID: http2.SettingHeaderTableSize, Val: 65536},
		{ID: http2.SettingInitialWindowSize, Val: 65535}, {ID: http2.SettingMaxFrameSize, Val: 16384}}
	options.Native.Profile.H2 = &profile
	first, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	profile.Settings[2].Val = 1 << 20
	large, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	before, after := first.Metadata(), large.Metadata()
	delta := int64((1 << 20) - 16384)
	if before.H2FrameBytes != 16384 || after.H2FrameBytes != 1<<20 ||
		after.TCPGraphBytes-before.TCPGraphBytes != 2*delta ||
		after.WorkBytes-before.WorkBytes != int64(options.MaxExchanges)*2*delta || after.SourceBytes <= before.SourceBytes ||
		after.ProxyTCPGraphBytes != before.ProxyTCPGraphBytes {
		t.Fatal("actual frame-reader storage did not enter live and retained budgets")
	}
	profile.Settings[0].Val = 1 << 20
	largeTable, err := PrepareV1(options)
	if err != nil || largeTable.Metadata().TCPGraphBytes <= after.TCPGraphBytes {
		t.Fatal("selected HPACK table disappeared from retained graph", err)
	}
	options.Mode = HTTP1Only
	h1, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	profile.Settings[0].Val, profile.Settings[2].Val = 4096, 16384
	h1Small, err := PrepareV1(options)
	if err != nil || h1.Metadata().H2FrameBytes != 0 || h1.Metadata().TCPGraphBytes != h1Small.Metadata().TCPGraphBytes {
		t.Fatal("H1-only construction acquired dormant H2 native allocations", err)
	}
}

func TestPreparedProxyTCPGraphUsesNativeDefaultOuterSelection(t *testing.T) {
	options := providerOptions()
	options.Mode, options.MaxRoutes, options.MaxExchanges = HTTP2Negotiated, 1, 1
	prepared, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	before := prepared.Metadata()
	proxy := &http2.Transport{MaxHeaderListSize: uint32(defaults(options).MaxNativeHeaderBytes)}
	minimum := 4*(256<<10) + 32*int64(proxy.FathomryMaxDecoderHeaderTableSize()) + 2*int64(proxy.FathomryMaxReadFrameSize())
	if before.ProxyTCPGraphBytes < minimum || before.RetainedNativeBytes != before.TCPGraphBytes+before.ProxyTCPGraphBytes {
		t.Fatal("retained CONNECT response omitted the outer HTTP2/TLS graph")
	}
	options.MaxRoutes = 2
	more, err := PrepareV1(options)
	if err != nil || more.Metadata().SourceBytes <= before.SourceBytes || more.Metadata().WorkBytes != before.WorkBytes {
		t.Fatal("cached proxy routes and retained response graphs were conflated", err)
	}
	zero := &http2.Transport{Settings: []http2.Setting{{ID: http2.SettingHeaderTableSize, Val: 0}}}
	if zero.FathomryMaxDecoderHeaderTableSize() != 0 || proxy.FathomryMaxDecoderHeaderTableSize() != 4096 {
		t.Fatal("native getter lost explicit-zero versus default table selection")
	}
}

func TestPreparedProxyIngressAndParserBudget(t *testing.T) {
	options := providerOptions()
	options.MaxProxyTunnels = 1
	first, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	options.MaxProxyTunnels = 2
	second, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	unit := first.Metadata().ProxyTunnelBytes
	if unit < nativeproxy.FathomryTunnelIngressBytes+64*defaults(options).MaxNativeHeaderBytes ||
		second.Metadata().SourceBytes-first.Metadata().SourceBytes != unit || second.Metadata().ProxyTunnelBytes != unit {
		t.Fatal("tunnel generation envelope omitted native ingress or structured-field containers")
	}
}
