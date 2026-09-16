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
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"crypto/tls"
	"io"
	stdhttp "net/http"
	"strconv"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
)

func encoded(t *testing.T, kind string, body []byte) []byte {
	t.Helper()
	var output bytes.Buffer
	var writer io.WriteCloser
	if kind == "gzip" {
		writer = gzip.NewWriter(&output)
	} else {
		writer = zlib.NewWriter(&output)
	}
	if _, err := writer.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}
func TestWireAndDecodedCompletenessAreBothRequired(t *testing.T) {
	for _, protocol := range []ProtocolMode{HTTP1Only, HTTP2Only, PreferHTTP3} {
		t.Run(string(protocol), func(t *testing.T) {
			for _, name := range []string{"plain-short", "gzip-short", "gzip-bad", "stacked", "limit", "empty"} {
				t.Run(name, func(t *testing.T) {
					payload := []byte("exact-body")
					wire := payload
					encoding := ""
					length := -1
					switch name {
					case "plain-short":
						length = len(wire) + 5
					case "gzip-short":
						wire = encoded(t, "gzip", payload)
						encoding = "gzip"
						length = len(wire) + 5
					case "gzip-bad":
						wire = []byte("not gzip")
						encoding = "gzip"
					case "stacked":
						wire = encoded(t, "deflate", encoded(t, "gzip", payload))
						encoding = "gzip, deflate"
					case "empty":
						wire = nil
						length = 0
					}
					peer := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
						if encoding != "" {
							w.Header().Set("Content-Encoding", encoding)
						}
						if length >= 0 {
							w.Header().Set("Content-Length", strconv.Itoa(length))
						}
						_, _ = w.Write(wire)
					}, protocol == PreferHTTP3)
					options := OptionsV1{Name: "integrity", Mode: protocol, Native: NativeOptionsV1{TLSConfig: &tls.Config{RootCAs: peer.roots}}}
					if name == "limit" {
						options.MaxResponseBytes = 4
					}
					fix := newFixture(t, options, 1)
					receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "body"}, request(t, "GET", peer.tcp.URL, ""))
					value := settle(t, fix, receipt)
					good := name == "stacked" || name == "empty"
					if good {
						if err != nil || value.Err() != nil || !value.Outcome.Value.Complete() {
							t.Fatal("complete response rejected", err, value.Err())
						}
						expected := payload
						if name == "empty" {
							expected = []byte{}
						}
						if !bytes.Equal(value.Outcome.Value.DataCopy(), expected) {
							t.Fatal("decoded content differs")
						}
					} else {
						if err == nil || value.Err() == nil || value.Outcome.Value.Complete() {
							t.Fatal("incomplete response certified", name)
						}
						if name == "plain-short" && string(value.Outcome.Value.DataCopy()) != string(payload) {
							t.Fatal("received prefix lost")
						}
						if name == "limit" && len(value.Outcome.Value.DataCopy()) > 4 {
							t.Fatal("retained body exceeded bound")
						}
					}
					if !value.Outcome.Present || !value.Released {
						t.Fatal("headers, integrity and local release conflated")
					}
					if protocol == PreferHTTP3 && value.Outcome.Value.Metadata().Protocol() != "HTTP/3.0" {
						t.Fatal("H3 test silently used fallback")
					}
				})
			}
		})
	}
}
