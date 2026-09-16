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

package nuki

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/klauspost/compress/zstd"
	nativetls "github.com/nukilabs/utls"
	"github.com/quic-go/quic-go/http3"
)

func encodeBody(t *testing.T, encoding string, input []byte) []byte {
	t.Helper()
	var output bytes.Buffer
	var writer io.WriteCloser
	switch encoding {
	case "gzip":
		writer = gzip.NewWriter(&output)
	case "deflate":
		writer = zlib.NewWriter(&output)
	case "raw-deflate":
		var err error
		writer, err = flate.NewWriter(&output, flate.DefaultCompression)
		if err != nil {
			t.Fatal(err)
		}
	case "br":
		writer = brotli.NewWriter(&output)
	case "zstd":
		var err error
		writer, err = zstd.NewWriter(&output, zstd.WithEncoderConcurrency(1))
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := writer.Write(input); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func TestProviderEncodedDecodedLengthsAndLimits(t *testing.T) {
	plain := bytes.Repeat([]byte("payload-"), 64)
	for _, encoding := range []string{"gzip", "deflate", "raw-deflate", "br", "zstd"} {
		t.Run(encoding, func(t *testing.T) {
			encoded := encodeBody(t, encoding, plain)
			peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				header := encoding
				if header == "raw-deflate" {
					header = "deflate"
				}
				writer.Header().Set("Content-Encoding", header)
				writer.Header().Set("Content-Length", strconv.Itoa(len(encoded)))
				_, _ = writer.Write(encoded)
			}))
			defer peer.Close()
			for _, kind := range []string{"complete", "decoded-limit", "encoded-limit", "raw"} {
				t.Run(kind, func(t *testing.T) {
					options := providerOptions()
					options.MaxResponseBytes = int64(len(plain))
					options.MaxEncodedBytes = int64(len(encoded))
					if kind == "decoded-limit" {
						options.MaxResponseBytes--
					}
					if kind == "encoded-limit" {
						options.MaxEncodedBytes--
					}
					if kind == "raw" {
						options.DisableDecompression = true
					}
					fixture := bindProvider(t, options)
					receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "encoding"}, nativeRequest(t, "GET", peer.URL, nil))
					if err != nil {
						t.Fatal(err)
					}
					result := outcome(t, fixture, receipt)
					value := result.Outcome.Value
					if kind == "complete" {
						if result.Err() != nil || !value.Complete() || !bytes.Equal(value.DataCopy(), plain) || value.EncodedBytesRead() != int64(len(encoded)) ||
							value.DecodedBytesRead() != int64(len(plain)) || value.Metadata().EncodedContentLength() != int64(len(encoded)) ||
							value.Metadata().ContentLength() != -1 || !value.Metadata().Uncompressed() {
							t.Fatal("encoded/decoded completeness mismatch", result.Err())
						}
					} else if kind == "raw" {
						if result.Err() != nil || !value.Complete() || !bytes.Equal(value.DataCopy(), encoded) || value.Metadata().Uncompressed() {
							t.Fatal("raw native semantics changed", result.Err())
						}
					} else {
						if !errors.Is(result.Err(), ErrLimit) || value.Complete() {
							t.Fatal("byte limit was not enforced", result.Err())
						}
						if int64(len(value.DataCopy())) > options.MaxResponseBytes || value.EncodedBytesRead() > options.MaxEncodedBytes+1 || value.DecodedBytesRead() > options.MaxResponseBytes+1 {
							t.Fatal("read or retention budget amplified")
						}
					}
				})
			}
		})
	}
}

func TestProviderH3TruncationBeforeTransparentDecode(t *testing.T) {
	plain := []byte("bounded-result")
	for _, encoding := range []string{"identity", "gzip", "br", "deflate"} {
		t.Run(encoding, func(t *testing.T) {
			encoded := plain
			if encoding != "identity" {
				encoded = encodeBody(t, encoding, plain)
			}
			peer := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
			defer peer.Close()
			packet, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := &http3.Server{TLSConfig: &tls.Config{Certificates: peer.TLS.Certificates},
				Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					writer.Header().Set("Content-Length", strconv.Itoa(len(encoded)+10))
					if encoding != "identity" {
						writer.Header().Set("Content-Encoding", encoding)
					}
					_, _ = writer.Write(encoded)
				})}
			done := make(chan error, 1)
			go func() { done <- server.Serve(packet) }()
			defer func() { _ = server.Close(); _ = packet.Close(); <-done }()
			options := providerOptions()
			options.Mode = HTTP3Only
			options.Native.TLS = &nativetls.Config{InsecureSkipVerify: true}
			fixture := bindProvider(t, options)
			receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "truncated"}, nativeRequest(t, "GET", "https://"+packet.LocalAddr().String(), nil))
			if err != nil {
				t.Fatal(err)
			}
			result := outcome(t, fixture, receipt)
			if result.Outcome.Value.Complete() || !errors.Is(result.Err(), ErrIntegrity) || !errors.Is(result.Err(), io.ErrUnexpectedEOF) {
				t.Fatal("short encoded response certified complete", result.Err())
			}
		})
	}
}
