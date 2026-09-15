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
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
)

func TestEncodedFramingAndDecodedLimit(t *testing.T) {
	var buffer bytes.Buffer
	encoder := gzip.NewWriter(&buffer)
	_, _ = io.WriteString(encoder, strings.Repeat("payload", 100))
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	encoded := bytes.Clone(buffer.Bytes())
	for _, mode := range []ProtocolMode{HTTP1, HTTP2, HTTP3} {
		t.Run(string(mode), func(t *testing.T) {
			address, options := peer(t, mode, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Encoding", "gzip")
				length := len(encoded)
				if r.URL.Path == "/short" {
					length += 10
				}
				w.Header().Set("Content-Length", strconv.Itoa(length))
				if r.URL.Path == "/bad" {
					_, _ = io.WriteString(w, strings.Repeat("x", len(encoded)))
					return
				}
				_, _ = w.Write(encoded)
			})
			options.MaxResponseBytes = 32
			fixture := bindFixture(t, options, 1)
			for _, path := range []string{"/short", "/bad", "/limit"} {
				receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "encoded"}, request(t, "GET", address+path, nil))
				if err == nil {
					t.Fatal("invalid or oversized encoded response accepted", path)
				}
				got := settle(t, fixture, receipt)
				if got.Outcome.Value.Complete() {
					t.Fatal("failed codec response certified complete", path)
				}
				if path == "/limit" && !errors.Is(got.Err(), ErrLimit) {
					t.Fatal("decoded limit lost")
				}
				if len(got.Outcome.Value.DataCopy()) > 32 {
					t.Fatal("retained decoded response exceeded declared bound")
				}
			}
		})
	}
}
func TestEncodedLengthBeforeDecoding(t *testing.T) {
	var buffer bytes.Buffer
	encoder := gzip.NewWriter(&buffer)
	_, _ = io.WriteString(encoder, "complete")
	_ = encoder.Close()
	for _, mode := range []ProtocolMode{HTTP1, HTTP2, HTTP3} {
		t.Run(string(mode), func(t *testing.T) {
			address, options := peer(t, mode, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Encoding", "gzip")
				length := buffer.Len()
				if r.URL.Path == "/short" {
					length += 9
				}
				w.Header().Set("Content-Length", strconv.Itoa(length))
				_, _ = w.Write(buffer.Bytes())
			})
			fixture := bindFixture(t, options, 1)
			for _, path := range []string{"/complete", "/short"} {
				receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "encoding"}, request(t, "GET", address+path, nil))
				got := settle(t, fixture, receipt)
				if path == "/complete" {
					if err != nil || !got.Outcome.Value.Complete() || string(got.Outcome.Value.DataCopy()) != "complete" {
						t.Fatal("encoded positive failed", err)
					}
				} else if err == nil || !errors.Is(got.Err(), io.ErrUnexpectedEOF) || got.Outcome.Value.Complete() {
					t.Fatal("valid compressed payload hid short wire framing", err)
				}
			}
		})
	}
}
