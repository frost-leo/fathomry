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
	"strconv"

	"github.com/bogdanfinn/fhttp/http2"
	sdk "github.com/bogdanfinn/tls-client"
	"github.com/frost-leo/fathomry/internal/compatibility"
)

// Build inspects the actual consuming executable, including replacements.
// Missing module facts stay unknown; the local patch marker is not attestation.
func Build() (compatibility.Build, error) {
	return compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{
		"github.com/bogdanfinn/tls-client", "github.com/bogdanfinn/fhttp",
		"github.com/bogdanfinn/utls", "github.com/bogdanfinn/quic-go-utls",
	}})
}

// Profile reports non-secret effective choices, not tested service support or
// browser fingerprint equivalence. Caller profile labels and native values are
// intentionally not disclosed. Native factory identities remain unknown.
func (client *Client) Profile() compatibility.Profile {
	if client == nil || client.owner == nil {
		return compatibility.Profile{}
	}
	value, native := client.owner.settings, client.owner.native
	options := []compatibility.Option{
		{Name: "mode", Value: string(value.Mode)},
		{Name: "local-compatibility-revision", Value: sdk.FathomryCompatibilityRevision},
		{Name: "builtin-h2-proxy-session-sharing", Value: "false"},
	}
	for _, entry := range []struct {
		name  string
		value bool
	}{
		{"routing-locked", value.RoutingLocked}, {"configured-proxy", value.ProxyURL != ""},
		{"insecure-skip-verify", value.InsecureSkipVerify}, {"configured-server-name", value.ServerName != ""},
		{"native-dial", native.DialContext != nil}, {"native-proxy-factory", native.ProxyDialerFactory != nil},
		{"native-jar", native.Jar != nil}, {"native-redirect", native.CheckRedirect != nil},
		{"native-pins", len(native.CertificatePins) > 0}, {"native-hooks", len(native.PreHooks)+len(native.PostHooks) > 0},
		{"random-tcp-tls-extension-order", value.RandomTLSExtensionOrder}, {"disable-session-tickets", value.DisableSessionTickets},
		{"disable-ipv4", value.DisableIPV4}, {"disable-ipv6", value.DisableIPV6},
	} {
		options = append(options, compatibility.Option{Name: entry.name, Value: strconv.FormatBool(entry.value)})
	}
	h2HeaderLimit := int64(native.Profile.GetSettings()[http2.SettingMaxHeaderListSize])
	if h2HeaderLimit == 0 {
		h2HeaderLimit = 10 << 20
	}
	if value.Mode == HTTP1Only {
		h2HeaderLimit = 0
	}
	headers, idle := value.MaxHeaderBytes, value.IdleConnTimeout
	if native.Transport != nil {
		if native.Transport.MaxResponseHeaderBytes > 0 {
			headers = native.Transport.MaxResponseHeaderBytes
		}
		if native.Transport.IdleConnTimeout != nil {
			idle = *native.Transport.IdleConnTimeout
		}
		options = append(options, compatibility.Option{Name: "disable-compression", Value: strconv.FormatBool(native.Transport.DisableCompression)},
			compatibility.Option{Name: "disable-keep-alives", Value: strconv.FormatBool(native.Transport.DisableKeepAlives)})
	}
	for _, entry := range []struct {
		name  string
		value int64
	}{
		{"max-active", int64(value.MaxActive)}, {"queued-calls", int64(value.QueuedCalls)}, {"max-bindings", int64(value.MaxBindings)},
		{"max-tcp-handles", int64(value.MaxTCPConnections)}, {"max-http3-transports", int64(value.MaxHTTP3Transports)},
		{"max-request-bytes", value.MaxRequestBytes}, {"max-response-bytes", value.MaxResponseBytes}, {"max-header-bytes", value.MaxHeaderBytes},
		{"max-native-header-bytes", value.MaxNativeHeaderBytes}, {"native-http2-header-limit", h2HeaderLimit}, {"native-h1-h3-header-limit", headers},
		{"max-exchanges", int64(value.MaxExchanges)}, {"max-replays", int64(value.MaxReplays)},
		{"admission-timeout-ns", int64(value.AdmissionTimeout)}, {"timeout-ns", int64(value.Timeout)}, {"tcp-idle-timeout-ns", int64(idle)},
	} {
		options = append(options, compatibility.Option{Name: entry.name, Value: strconv.FormatInt(entry.value, 10)})
	}
	return compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "tlsclient-v1",
		Protocol: compatibility.Fact{Kind: compatibility.Declared, Value: "configured-http"},
		Native:   compatibility.Fact{Kind: compatibility.NotApplicable}, Options: options}
}
