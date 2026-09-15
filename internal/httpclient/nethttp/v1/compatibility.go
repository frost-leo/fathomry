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
	"strconv"

	"github.com/frost-leo/fathomry/internal/compatibility"
)

// Build inspects the actual consuming executable. net/http is part of its Go
// toolchain, not a separately versioned SDK module. Missing facts stay unknown.
func Build() (compatibility.Build, error) {
	return compatibility.Inspect(compatibility.BuildRequest{})
}

// Profile reports effective non-secret local choices, not tested compatibility.
// A multi-origin client has no single observed service version or negotiated
// protocol. Native extension identities/behavior cannot be inferred from function
// pointers or labels, and this profile does not certify arbitrary callbacks.
func (client *Client) Profile() compatibility.Profile {
	if client == nil || client.owner == nil {
		return compatibility.Profile{}
	}
	value, native := client.owner.settings, client.owner.native
	options := []compatibility.Option{}
	for _, entry := range []struct {
		name  string
		value bool
	}{
		{"http1", value.HTTP1}, {"http2", value.HTTP2}, {"unencrypted-http2", value.UnencryptedHTTP2},
		{"disable-compression", value.DisableCompression}, {"disable-keep-alives", value.DisableKeepAlives},
		{"native-tls", native.TLS != nil}, {"native-http2", native.HTTP2 != nil},
		{"native-dial", native.DialContext != nil}, {"native-proxy", native.Proxy != nil},
		{"native-redirect", native.CheckRedirect != nil}, {"native-jar", native.Jar != nil},
		{"configured-proxy", value.ProxyURL != ""}, {"configured-roots", value.RootCAPEM != ""},
		{"routing-locked", value.RoutingLocked},
		{"configured-client-certificate", value.ClientCertPEM != ""}, {"configured-server-name", value.ServerName != ""},
	} {
		options = append(options, compatibility.Option{Name: entry.name, Value: strconv.FormatBool(entry.value)})
	}
	for _, entry := range []struct {
		name  string
		value int64
	}{
		{"max-active", int64(value.MaxActive)}, {"queued-calls", int64(value.QueuedCalls)}, {"max-connections", int64(value.MaxConnections)},
		{"max-route-transports", int64(value.MaxConnections)},
		{"callback-admission-ceiling", int64(4 * value.MaxConnections)},
		{"max-request-bytes", value.MaxRequestBytes}, {"max-response-bytes", value.MaxResponseBytes},
		{"max-header-bytes", value.MaxHeaderBytes}, {"max-exchanges", int64(value.MaxExchanges)},
		{"admission-timeout-ns", int64(value.AdmissionTimeout)}, {"timeout-ns", int64(value.Timeout)},
		{"dial-timeout-ns", int64(value.DialTimeout)}, {"tls-handshake-timeout-ns", int64(value.TLSHandshakeTimeout)},
		{"response-header-timeout-ns", int64(value.ResponseHeaderTimeout)}, {"idle-conn-timeout-ns", int64(value.IdleConnTimeout)},
	} {
		options = append(options, compatibility.Option{Name: entry.name, Value: strconv.FormatInt(entry.value, 10)})
	}
	return compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "nethttp-v1",
		Protocol: compatibility.Fact{Kind: compatibility.Declared, Value: "configured-http"},
		Native:   compatibility.Fact{Kind: compatibility.NotApplicable},
		Options:  options}
}
