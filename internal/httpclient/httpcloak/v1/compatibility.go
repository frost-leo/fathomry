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

package httpcloak

import (
	"strconv"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/sardanioss/httpcloak/transport"
)

// Build inspects the consuming executable. Local replacements are reported, not
// attested from repository pins or promoted to tested compatibility.
func Build() (compatibility.Build, error) {
	return compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{
		"github.com/sardanioss/httpcloak", "github.com/sardanioss/http", "github.com/sardanioss/net",
		"github.com/sardanioss/quic-go", "github.com/sardanioss/udpbara", "github.com/sardanioss/utls",
	}})
}

// Profile derives non-secret execution choices from the actual frozen source.
// Service identity, deployment and unobserved native callback identities remain
// unknown. No compatibility catalog or experimental preset allowlist is installed.
func (client *Client) Profile() compatibility.Profile {
	if client == nil || client.owner == nil {
		return compatibility.Profile{}
	}
	value, native := client.owner.settings, client.owner.native
	options := []compatibility.Option{
		{Name: "protocol", Value: string(value.Protocol)},
		{Name: "local-compatibility-revision", Value: transport.FathomryCompatibilityRevision},
		{Name: "checkout", Value: "exclusive"},
		{Name: "automatic-protocol-racing", Value: "false"},
		{Name: "udp-proxy", Value: "unsupported"},
	}
	for _, item := range []struct {
		name  string
		value bool
	}{
		{"configured-proxy", value.ProxyURL != ""}, {"routing-locked", value.RoutingLocked},
		{"insecure-skip-verify", value.InsecureSkipVerify}, {"disable-ech", value.DisableECH},
		{"native-jar", native.Jar != nil}, {"native-redirect-veto", native.CheckRedirect != nil},
		{"native-key-log", native.Transport.KeyLogWriter != nil},
		{"custom-ja3", native.Transport.CustomJA3 != ""}, {"custom-h2-settings", native.Transport.CustomH2Settings != nil},
		{"configured-connect-map", len(native.Transport.ConnectTo) > 0}, {"configured-ech", len(native.Transport.ECHConfig) > 0 || native.Transport.ECHConfigDomain != ""},
	} {
		options = append(options, compatibility.Option{Name: item.name, Value: strconv.FormatBool(item.value)})
	}
	for _, item := range []struct {
		name  string
		value int64
	}{
		{"max-active", int64(value.MaxActive)}, {"queued-calls", int64(value.QueuedCalls)}, {"max-bindings", int64(value.MaxBindings)},
		{"max-sockets", int64(value.MaxConnections)}, {"max-input-bytes", value.MaxRequestBytes}, {"max-wire-bytes", value.MaxWireBytes},
		{"max-decoded-bytes", value.MaxResponseBytes}, {"max-header-bytes", value.MaxHeaderBytes}, {"max-exchanges", int64(value.MaxExchanges)},
		{"max-replays", int64(value.MaxReplays)}, {"timeout-ns", int64(value.Timeout)}, {"admission-timeout-ns", int64(value.AdmissionTimeout)},
	} {
		options = append(options, compatibility.Option{Name: item.name, Value: strconv.FormatInt(item.value, 10)})
	}
	return compatibility.Profile{ImplementationModule: compatibility.FrameworkModule, SDKMode: "httpcloak-v1", Protocol: compatibility.Fact{Kind: compatibility.Declared, Value: string(value.Protocol)}, Native: compatibility.Fact{Kind: compatibility.NotApplicable}, Options: options}
}
