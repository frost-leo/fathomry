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
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"testing"

	"github.com/frost-leo/fathomry/internal/resource"
)

func TestConfigurationVersionsAndPreflight(t *testing.T) {
	options := OptionsV1{Name: "first"}
	for _, version := range []uint32{0, 1} {
		options.Version = version
		selected, err := Select(options)
		if err != nil || selectedDescription(t, options, selected).Format != 1 {
			t.Fatal("valid format rejected", err)
		}
	}
	options.Version = 2
	if _, err := Select(options); !errors.Is(err, resource.ErrConfiguration) {
		t.Fatal("unknown format accepted", err)
	}
	options.Version = 1
	for _, input := range []string{
		"unknown: true", "max_active: 0", "timeout_ns: -1", "max_response_bytes: null",
		"http1: false\nhttp2: false", "http1: true\nunencrypted_http2: true",
		"proxy_url: ftp://invalid.example", "max_exchanges: 129",
	} {
		if _, err := Select(options, resource.Layer{Kind: resource.Base, Content: []byte(input)}); !errors.Is(err, resource.ErrConfiguration) {
			t.Fatalf("invalid layer accepted: %q", input)
		}
	}
	calls := 0
	options.Native.DialContext = func(context.Context, string, string) (net.Conn, error) { calls++; return nil, errors.New("no network") }
	if _, err := Select(options, resource.Layer{Kind: resource.Base, Content: []byte("max_connections: 0")}); err == nil || calls != 0 {
		t.Fatal("invalid configuration performed native work")
	}
}
func selectedDescription(t *testing.T, options OptionsV1, selected resource.Selection[Source]) resource.Description {
	t.Helper()
	limits, _ := LimitsV1(options)
	selected = resource.WithLimits(selected, limits)
	assembly, err := resource.Assemble(deadline(t), deadline(t), "config", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(deadline(t))
	_, info, err := resource.Bind(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	return info.Configuration
}
func TestLayeredSettingsAndNativeSnapshot(t *testing.T) {
	config := &tls.Config{RootCAs: x509.NewCertPool(), NextProtos: []string{"http/1.1"}}
	options := OptionsV1{Name: "configured", Native: NativeOptionsV1{TLS: config}}
	selected, err := Select(options, resource.Layer{Kind: resource.Local, Content: []byte("max_response_bytes: 12345")})
	if err != nil {
		t.Fatal(err)
	}
	config.NextProtos[0] = "changed"
	config.ServerName = "changed.invalid"
	limits, _ := LimitsV1(options)
	selected = resource.WithLimits(selected, limits)
	assembly, err := resource.Assemble(deadline(t), deadline(t), "snapshot", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(deadline(t))
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	if source.owner.settings.MaxResponseBytes != 12345 || source.owner.settings.MaxRequestBytes != 8<<20 ||
		source.owner.transport.TLSClientConfig.ServerName != "" {
		t.Fatal("effective settings or native snapshot changed")
	}
	for _, protocol := range source.owner.transport.TLSClientConfig.NextProtos {
		if protocol == "changed" {
			t.Fatal("ALPN slice was aliased")
		}
	}
}
func TestNativeAndPlainTLSDoNotSilentlyOverride(t *testing.T) {
	options := OptionsV1{Name: "conflict", ServerName: "synthetic.invalid", Native: NativeOptionsV1{TLS: &tls.Config{}}}
	if _, err := Select(options); !errors.Is(err, resource.ErrConfiguration) {
		t.Fatal("ambiguous TLS options accepted", err)
	}
}
