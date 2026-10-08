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
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	nativehttp "github.com/sardanioss/http"
)

type fixtureCookieJar struct {
	read  func(*url.URL) []*nativehttp.Cookie
	write func(*url.URL, []*nativehttp.Cookie)
}

func (jar fixtureCookieJar) Cookies(address *url.URL) []*nativehttp.Cookie {
	return jar.read(address)
}
func (jar fixtureCookieJar) SetCookies(address *url.URL, values []*nativehttp.Cookie) {
	if jar.write != nil {
		jar.write(address, values)
	}
}

func TestJarOutputIsBoundedBeforeRequestCopyOrNetwork(t *testing.T) {
	for _, cookies := range [][]*nativehttp.Cookie{
		{{Name: "large", Value: strings.Repeat("x", 1<<16)}},
		{{Name: "containers", Unparsed: make([]string, 1<<16)}},
		make([]*nativehttp.Cookie, 1<<16),
	} {
		var calls atomic.Int32
		address, options := peer(t, HTTP1, func(writer http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			writer.WriteHeader(http.StatusNoContent)
		})
		options.Native.Jar = fixtureCookieJar{read: func(*url.URL) []*nativehttp.Cookie { return cookies }}
		fixture := bindFixture(t, options, 1)
		receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "cookie-bound"}, request(t, "GET", address, nil))
		if err == nil {
			t.Fatal("oversized jar output was admitted")
		}
		result := settle(t, fixture, receipt)
		if !errors.Is(result.Outcome.Primary, ErrLimit) || calls.Load() != 0 || result.Outcome.Value.Exchanges() != 0 {
			t.Fatal("jar bound was applied after a network exchange", result.Outcome.Primary, calls.Load())
		}
	}
}

func TestJarCannotRewriteTargetAndRetainsResponseCookieScope(t *testing.T) {
	address, options := peer(t, HTTP1, func(writer http.ResponseWriter, input *http.Request) {
		if input.Header.Get("Cookie") != "jar=original" {
			t.Error("native cookie value changed", input.Header.Get("Cookie"))
		}
		writer.Header().Set("Set-Cookie", "result=retained; Path=/; SameSite=Strict; Unknown=value")
		_, _ = io.WriteString(writer, "origin")
	})
	var cookiesWritten atomic.Int32
	options.Native.Jar = fixtureCookieJar{
		read: func(location *url.URL) []*nativehttp.Cookie {
			location.Host = "127.0.0.1:1"
			return []*nativehttp.Cookie{{Name: "jar", Value: "original", Path: "/", Unparsed: []string{"native=kept"}}}
		},
		write: func(location *url.URL, cookies []*nativehttp.Cookie) {
			if location.String() != address || len(cookies) != 1 || cookies[0].Name != "result" || cookies[0].Value != "retained" || cookies[0].Path != "/" || cookies[0].SameSite != nativehttp.SameSiteStrictMode || len(cookies[0].Unparsed) != 1 || cookies[0].Unparsed[0] != "Unknown=value" {
				t.Error("response cookie lost target or native fields")
			}
			location.Host = "127.0.0.1:1"
			cookiesWritten.Add(1)
		},
	}
	fixture := bindFixture(t, options, 1)
	receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "jar-scope"}, request(t, "GET", address, nil))
	if err != nil {
		t.Fatal(err)
	}
	if result := settle(t, fixture, receipt); !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "origin" || cookiesWritten.Load() != 1 {
		t.Fatal("jar callbacks changed request ownership or result")
	}
	original := &nativehttp.Cookie{Name: "copy", Value: "value", Unparsed: []string{"original"}}
	copied, err := copyCookies([]*nativehttp.Cookie{original}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	copied[0].Unparsed[0] = "changed"
	if original.Unparsed[0] != "original" {
		t.Fatal("jar snapshot retained mutable native container")
	}
}
