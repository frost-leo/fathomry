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
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	sdk "github.com/bogdanfinn/tls-client"
)

func TestNativeBandwidthScopeAndRetirement(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			peers := make([]*httptest.Server, 2)
			roots := x509.NewCertPool()
			for index := range peers {
				peers[index] = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "x") }))
				defer peers[index].Close()
				roots.AddCert(peers[index].Certificate())
			}
			options := providerOptions()
			options.Mode = HTTP1Only
			options.Bandwidth = enabled
			options.MaxBindings = 1
			options.Native.Transport = &sdk.TransportOptions{RootCAs: roots}
			fixture := bindProvider(t, options, 1)
			var previousRead, previousWrite int64
			for index, peer := range peers {
				receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID([]string{"first", "retired"}[index]), providerRequest(t, "GET", peer.URL, nil))
				if err != nil {
					t.Fatal(err)
				}
				result := settleProvider(t, fixture, receipt)
				value := fixture.client.Bandwidth()
				read, readKnown := value.ReadBytes()
				written, writeKnown := value.WriteBytes()
				if value.Enabled() != enabled || value.Scope() != "origin-tls-over-tcp" || readKnown != enabled || writeKnown != enabled {
					t.Fatal("native bandwidth availability/scope changed")
				}
				if enabled && (read <= previousRead || written <= previousWrite || read <= result.Outcome.Value.BytesRead()) {
					t.Fatal("native TLS counters did not include real transport traffic or lost retired binding observations")
				}
				previousRead, previousWrite = read, written
			}
		})
	}
}
func TestNativeBandwidthDoesNotInventCleartextTraffic(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "cleartext") }))
	defer peer.Close()
	options := providerOptions()
	options.Mode = HTTP1Only
	options.Bandwidth = true
	fixture := bindProvider(t, options, 1)
	receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("plain"), providerRequest(t, "GET", peer.URL, nil))
	if err != nil {
		t.Fatal(err)
	}
	result := settleProvider(t, fixture, receipt)
	if result.Outcome.Value.BytesRead() != 9 {
		t.Fatal("cleartext body control failed")
	}
	read, known := fixture.client.Bandwidth().ReadBytes()
	written, writeKnown := fixture.client.Bandwidth().WriteBytes()
	if !known || !writeKnown || read != 0 || written != 0 || fixture.client.Bandwidth().Scope() != "origin-tls-over-tcp" {
		t.Fatal("application bytes became native TLS traffic")
	}
}
