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
	"errors"
	"io"
	stdhttp "net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/enetx/g"
	sdk "github.com/enetx/surf"
	"github.com/enetx/surf/profiles"
	"github.com/frost-leo/fathomry/internal/fault"
)

func TestNativeHeaderLimitDiffersFromRetainedMetadataLimit(t *testing.T) {
	const name = "X-Native-Header-Accounting-Duplicate-Field"
	const fields = 24
	headers := stdhttp.Header{
		"Date":           {"Mon, 01 Jan 2024 00:00:00 GMT"},
		"Content-Type":   {"text/plain"},
		"Content-Length": {"2"},
	}
	for range fields {
		headers.Add(name, "v")
	}
	for _, protocol := range []string{"h1", "h2", "ja-h1", "ja-h2", "h2c", "h3"} {
		for _, allowed := range []bool{false, true} {
			selection := "reject-one-native-byte-over"
			if allowed {
				selection = "accept-exact-native-boundary"
			}
			t.Run(protocol+"/"+selection, func(t *testing.T) {
				var calls, observed atomic.Int64
				url, options, peer := reviewBehaviorPeer(t, protocol, func(writer stdhttp.ResponseWriter, request *stdhttp.Request) {
					calls.Add(1)
					if request.Proto != reviewBehaviorProtocol(protocol) {
						t.Error("native header fixture silently used another protocol")
					}
					for key, values := range headers {
						writer.Header()[key] = append([]string(nil), values...)
					}
					_, _ = io.WriteString(writer, "ok")
				})
				var sockets reviewBehaviorSockets
				options.MaxHeaderBytes = 1024
				options.MaxNativeHeaderBytes = reviewNativeHeaderBoundary(protocol, headers)
				if !allowed {
					options.MaxNativeHeaderBytes--
				}
				if options.Native.Profile != nil {
					options.Native.Profile.ConfigureH2 = func(profiles.H2Config) {}
					options.Native.Profile.BuildHeaders = func(profiles.OSKey) *g.MapOrd[g.String, g.String] {
						empty := g.NewMapOrd[g.String, g.String]()
						return &empty
					}
				}
				t.Logf("native ceiling=%d; unchanged retained metadata ceiling=%d", options.MaxNativeHeaderBytes, options.MaxHeaderBytes)
				options.Native.DialContext = sockets.dial
				options.Native.ResponseMiddleware = []func(*sdk.Response) error{func(*sdk.Response) error {
					observed.Add(1)
					return nil
				}}
				fix := newFixture(t, options, 1)
				receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "native-header-limit"}, request(t, "GET", url, ""))
				if receipt == nil {
					t.Fatal("native header fixture refused before admission", err)
				}
				result := settle(t, fix, receipt)
				value := result.Outcome.Value
				if allowed {
					headers := value.Metadata().HeadersCopy()
					if err != nil || result.Err() != nil || !value.Complete() || string(value.DataCopy()) != "ok" || observed.Load() != 1 ||
						len(headers.Values(name)) != fields || strings.Join(headers.Values(name), "") != strings.Repeat("v", fields) ||
						!headerFits(headers, 1024, false) || value.Metadata().Protocol() != reviewBehaviorProtocol(protocol) {
						t.Fatal("larger native-only limit did not preserve the same admitted retained metadata", err)
					}
				} else if err == nil || result.Outcome.Primary == nil || errors.Is(result.Err(), ErrLimit) ||
					value.Metadata().StatusCode() != 0 || value.Complete() || observed.Load() != 0 {
					t.Fatal("native limit ignored, or result rejected only by retained metadata accounting", err)
				}
				if calls.Load() != 1 || value.RoundTrips() != 1 || result.Outcome.Cleanup != nil {
					t.Fatal("native header bound caused fallback/retry or lost cleanup")
				}
				reviewBehaviorReleased(t, fix, &sockets)
				if protocol == "h3" {
					reviewBehaviorQUICReleased(t, peer)
				}
			})
		}
	}
}

func reviewNativeHeaderBoundary(protocol string, headers stdhttp.Header) int64 {
	var size int64
	if protocol == "h1" || protocol == "ja-h1" {
		size = int64(len("HTTP/1.1 200 OK\r\n") + len("\r\n"))
		for name, values := range headers {
			for _, value := range values {
				size += int64(len(name) + len(": ") + len(value) + len("\r\n"))
			}
		}
		return size
	}
	size = int64(len(":status") + len("200") + 32)
	for name, values := range headers {
		for _, value := range values {
			size += int64(len(name) + len(value) + 32)
		}
	}
	if protocol == "h2" {
		// The selected enetx/http H2 transport adds ten 32-byte fields when
		// converting its HTTP/1-style MaxResponseHeaderBytes setting.
		size -= 10 * 32
	}
	return size
}
