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
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	sdk "github.com/bogdanfinn/tls-client"
	"github.com/tam7t/hpkp"
)

func TestProviderCertificatePinsCoverEquivalentHosts(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		t.Run(strconv.FormatBool(h2), func(t *testing.T) {
			var reached atomic.Int64
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				reached.Add(1)
				_, _ = io.WriteString(writer, "pinned")
			}))
			server.EnableHTTP2 = h2
			server.StartTLS()
			t.Cleanup(server.Close)
			roots := x509.NewCertPool()
			roots.AddCert(server.Certificate())
			target := server.Listener.Addr().String()
			_, port, err := net.SplitHostPort(target)
			if err != nil {
				t.Fatal(err)
			}
			for _, good := range []bool{false, true} {
				options := providerOptions()
				options.Mode = HTTP1Only
				if h2 {
					options.Mode = Negotiated
				}
				options.Native.Transport = &sdk.TransportOptions{RootCAs: roots}
				options.Native.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, network, target)
				}
				pin := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
				if good {
					pin = hpkp.Fingerprint(server.Certificate())
				}
				options.Native.CertificatePins = map[string][]string{
					"EXAMPLE.COM.":    {pin},
					"127.0.0.1":       {pin},
					"0:0:0:0:0:0:0:1": {pin},
				}
				fixture := bindProvider(t, options, 1)
				for _, host := range []string{"example.com", "EXAMPLE.COM", "example.com.", "127.0.0.1", "::ffff:127.0.0.1", "::1", "0:0:0:0:0:0:0:1"} {
					receipt, callErr := fixture.client.Do(testContext(t), testContext(t), providerID("pin"), providerRequest(t, "GET", "https://"+net.JoinHostPort(host, port)+"/", nil))
					result := settleProvider(t, fixture, receipt)
					if good {
						if callErr != nil || result.Err() != nil || string(result.Outcome.Value.DataCopy()) != "pinned" {
							t.Fatalf("valid pin rejected for %s: %v", host, callErr)
						}
					} else if !errors.Is(callErr, sdk.ErrBadPinDetected) || !errors.Is(result.Err(), sdk.ErrBadPinDetected) || reached.Load() != 0 {
						t.Fatalf("bad pin did not block equivalent host %s: %v", host, callErr)
					}
				}
			}
		})
	}
}

func TestProviderConflictingCanonicalPinsFailBeforeConstruction(t *testing.T) {
	options := providerOptions()
	options.Native.CertificatePins = map[string][]string{"example.com": {"first"}, "EXAMPLE.COM.": {"second"}}
	if _, err := Select(options); !errors.Is(err, ErrInput) {
		t.Fatal("conflicting pin configuration accepted", err)
	}
}
