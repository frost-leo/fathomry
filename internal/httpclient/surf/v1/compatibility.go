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
	"slices"
	"strconv"
	"strings"

	http2 "github.com/enetx/http2"
	http3 "github.com/enetx/http3"
	sdk "github.com/enetx/surf"
	"github.com/frost-leo/fathomry/internal/compatibility"
)

// Build inspects the actual executable's SDK selections and replacements.
// Local revision markers are declarations, not source or deployment attestation.
func Build() (compatibility.Build, error) {
	return compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{
		"github.com/enetx/surf", "github.com/enetx/http", "github.com/enetx/http2", "github.com/enetx/http3",
		"github.com/enetx/g", "github.com/refraction-networking/utls", "github.com/quic-go/quic-go", "github.com/wzshiming/socks5",
	}})
}

// Profile reports effective technical choices without route URLs, credentials,
// native labels or callback identities. It is not a blanket support certificate.
func (client *Client) Profile() compatibility.Profile {
	if client == nil || client.owner == nil {
		return compatibility.Profile{}
	}
	value, native := client.owner.settings, client.owner.native
	options := []compatibility.Option{
		{Name: "mode", Value: string(value.Mode)},
		{Name: "configuration-format", Value: "1"},
		{Name: "runtime-input-format", Value: "1"},
		{Name: "surf-compatibility", Value: sdk.FathomryCompatibilityRevision},
		{Name: "http2-compatibility", Value: http2.FathomryCompatibilityRevision},
		{Name: "http3-compatibility", Value: http3.FathomryCompatibilityRevision},
		{Name: "h3-ja-fingerprinting", Value: "false"},
		{Name: "shared-h2-proxy-tunnels", Value: "false"},
	}
	codes := append([]int(nil), value.RetryCodes...)
	if value.NativeRetries == 0 {
		codes = nil
	} else if len(codes) == 0 {
		codes = []int{500, 429, 503}
	}
	slices.Sort(codes)
	codes = slices.Compact(codes)
	values := make([]string, len(codes))
	for index, code := range codes {
		values[index] = strconv.Itoa(code)
	}
	if len(values) == 0 {
		values = []string{"none"}
	}
	for offset := 0; offset < len(values); offset += 32 {
		options = append(options, compatibility.Option{Name: "native-retry-codes-" + strconv.Itoa(offset/32),
			Value: strings.Join(values[offset:min(offset+32, len(values))], "+")})
	}
	for _, entry := range []struct {
		name  string
		value bool
	}{
		{"native-profile", native.Profile != nil}, {"hello-factory", native.HelloSpecFactory != nil},
		{"native-jar", native.Jar != nil}, {"native-middleware", len(native.RequestMiddleware)+len(native.ResponseMiddleware) > 0},
		{"custom-tcp-dial", native.DialContext != nil}, {"custom-packet-listen", native.ListenPacket != nil},
		{"routing-locked", value.RoutingLocked}, {"disable-compression", value.DisableCompression},
		{"origin-verification-disabled", native.TLSConfig != nil && native.TLSConfig.InsecureSkipVerify},
		{"ja-verification-disabled", native.JAConfig != nil && native.JAConfig.InsecureSkipVerify},
		{"proxy-verification-disabled", native.ProxyTLSConfig != nil && native.ProxyTLSConfig.InsecureSkipVerify},
	} {
		options = append(options, compatibility.Option{Name: entry.name, Value: strconv.FormatBool(entry.value)})
	}
	for _, entry := range []struct {
		name  string
		value int64
	}{
		{"max-active", int64(value.MaxActive)}, {"queued-calls", int64(value.QueuedCalls)}, {"max-routes", int64(value.MaxRoutes)},
		{"max-tcp-connections", int64(value.MaxTCPConnections)}, {"max-udp-sockets", int64(value.MaxUDPSockets)},
		{"max-request-bytes", value.MaxRequestBytes}, {"max-response-bytes", value.MaxResponseBytes},
		{"max-header-bytes", value.MaxHeaderBytes}, {"max-native-header-bytes", value.MaxNativeHeaderBytes},
		{"max-round-trips", int64(value.MaxRoundTrips)}, {"max-replays", int64(value.MaxReplays)},
		{"native-retries", int64(value.NativeRetries)}, {"native-retry-delay-ns", int64(value.RetryDelay)},
		{"admission-timeout-ns", int64(value.AdmissionTimeout)}, {"timeout-ns", int64(value.Timeout)}, {"idle-timeout-ns", int64(value.IdleConnTimeout)},
	} {
		options = append(options, compatibility.Option{Name: entry.name, Value: strconv.FormatInt(entry.value, 10)})
	}
	return compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "surf-v1",
		Protocol: compatibility.Fact{Kind: compatibility.Declared, Value: "configured-http"}, Native: compatibility.Fact{Kind: compatibility.NotApplicable}, Options: options}
}
