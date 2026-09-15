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
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	sdk "github.com/bogdanfinn/tls-client"
)

func gzipPayload(t *testing.T, plain []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	encoder := gzip.NewWriter(&buffer)
	if _, err := encoder.Write(plain); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}
func TestProviderEncodedFramingBeforeDecompression(t *testing.T) {
	providerProtocols(t, func(t *testing.T, mode ProtocolMode) {
		for _, extra := range []int{0, 7} {
			t.Run(strconv.Itoa(extra), func(t *testing.T) {
				plain := []byte("encoded integrity")
				encoded := gzipPayload(t, plain)
				endpoint, options := providerPeer(t, mode, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Encoding", "gzip")
					w.Header().Set("Content-Length", strconv.Itoa(len(encoded)+extra))
					_, _ = w.Write(encoded)
				})
				fixture := bindProvider(t, options, 1)
				receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("framed"), providerRequest(t, "GET", endpoint, nil))
				result := settleProvider(t, fixture, receipt)
				value := result.Outcome.Value
				if mode == HTTP3Racing && value.Metadata().Protocol() != "HTTP/3.0" {
					t.Fatal("H3 integrity path not exercised")
				}
				if extra == 0 {
					if err != nil || result.Err() != nil || !value.Complete() || !bytes.Equal(value.DataCopy(), plain) {
						t.Fatal("valid gzip framing refused", err)
					}
				} else if err == nil || !errors.Is(result.Err(), io.ErrUnexpectedEOF) || value.Complete() {
					t.Fatal("short encoded body certified after decoding", result.Err())
				}
			})
		}
	})
}
func TestProviderNativeCompressionSelection(t *testing.T) {
	providerProtocols(t, func(t *testing.T, mode ProtocolMode) {
		for _, disabled := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				t.Run(strconv.FormatBool(disabled)+"/"+strconv.FormatBool(explicit), func(t *testing.T) {
					plain := []byte("native compression")
					encoded := gzipPayload(t, plain)
					endpoint, options := providerPeer(t, mode, func(w http.ResponseWriter, r *http.Request) {
						if got := r.Header.Get("Accept-Encoding"); strings.Contains(got, "gzip") {
							w.Header().Set("Content-Encoding", "gzip")
							w.Header().Set("Content-Length", strconv.Itoa(len(encoded)))
							_, _ = w.Write(encoded)
						} else {
							if got != "" {
								t.Error("unexpected encoding input", got)
							}
							_, _ = w.Write(plain)
						}
					})
					if options.Native.Transport == nil {
						options.Native.Transport = &sdk.TransportOptions{}
					}
					options.Native.Transport.DisableCompression = disabled
					fixture := bindProvider(t, options, 1)
					request := providerRequest(t, "GET", endpoint, nil)
					if explicit {
						request.Header.Set("Accept-Encoding", "gzip")
					}
					receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("compression"), request)
					if err != nil {
						t.Fatal(err)
					}
					result := settleProvider(t, fixture, receipt)
					data := result.Outcome.Value
					want := plain
					decoded := !disabled && (mode != HTTP3Racing || !explicit)
					if explicit && !decoded {
						want = encoded
					}
					if !data.Complete() || !bytes.Equal(data.DataCopy(), want) || data.Metadata().Uncompressed() != decoded {
						t.Fatal("native explicit/automatic compression semantics changed")
					}
				})
			}
		}
	})
}
