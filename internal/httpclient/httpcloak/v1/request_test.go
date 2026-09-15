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
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	nativehttp "github.com/sardanioss/http"
	"github.com/sardanioss/httpcloak/fingerprint"
)

type replayReader struct {
	io.Reader
	cause  error
	closed bool
}

func (reader *replayReader) Close() error { reader.closed = true; return reader.cause }
func TestRedirectReplayAndFailedFactoryCleanup(t *testing.T) {
	for _, mode := range []ProtocolMode{HTTP1, HTTP2, HTTP3} {
		t.Run(string(mode), func(t *testing.T) {
			address, options := peer(t, mode, func(w http.ResponseWriter, r *http.Request) {
				data, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				if string(data) != "payload" {
					t.Error("replay input changed")
				}
				if r.URL.Path == "/first" {
					w.Header().Set("Location", "/last")
					w.WriteHeader(307)
					return
				}
				_, _ = io.WriteString(w, "complete")
			})
			fixture := bindFixture(t, options, 1)
			receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "replay"}, request(t, "POST", address+"/first", strings.NewReader("payload")), RequestOptionsV1{FollowRedirects: true})
			if err != nil {
				t.Fatal(err)
			}
			got := settle(t, fixture, receipt)
			if got.Outcome.Value.Replays() != 1 || got.Outcome.Value.Exchanges() != 2 || !got.Outcome.Value.InputComplete() || !got.Outcome.Value.Complete() {
				t.Fatal("replay evidence changed")
			}
			factoryErr := errors.New("test factory")
			closeErr := errors.New("test replay close")
			reader := &replayReader{Reader: strings.NewReader("payload"), cause: closeErr}
			input := request(t, "POST", address+"/first", strings.NewReader("payload"))
			input.GetBody = func() (io.ReadCloser, error) { return reader, factoryErr }
			receipt, _ = fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "failed-replay"}, input, RequestOptionsV1{FollowRedirects: true})
			failed := settle(t, fixture, receipt)
			if !errors.Is(failed.Outcome.Primary, factoryErr) || !errors.Is(failed.Outcome.Cleanup, closeErr) || !reader.closed || !failed.Outcome.Present || failed.Outcome.Value.Complete() {
				t.Fatal("response, factory error or failed replay cleanup lost")
			}
		})
	}
}
func TestExactHeadersAndMalformedNativeInput(t *testing.T) {
	for _, mode := range []ProtocolMode{HTTP1, HTTP2, HTTP3} {
		t.Run(string(mode), func(t *testing.T) {
			address, options := peer(t, mode, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("User-Agent") != "" || r.Header.Get("Sec-Ch-Ua") != "" || r.Header.Get("Cookie") != "a=1; b=2" || r.Header.Get("X-Exact") != "yes" {
					t.Errorf("exact native header request was rewritten: ua=%q hints=%q cookie=%q exact=%q", r.Header.Get("User-Agent"), r.Header.Get("Sec-Ch-Ua"), r.Header.Get("Cookie"), r.Header.Get("X-Exact"))
				}
				_, _ = io.WriteString(w, "ok")
			})
			fixture := bindFixture(t, options, 1)
			receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "exact"}, request(t, "GET", address, nil), RequestOptionsV1{ExactHeaders: []fingerprint.HeaderPair{{Key: "Cookie", Value: "a=1; b=2"}, {Key: "X-Exact", Value: "yes"}}})
			if err != nil {
				t.Fatal(err)
			}
			if !settle(t, fixture, receipt).Outcome.Value.Complete() {
				t.Fatal("exact request incomplete")
			}
			malformed := request(t, "POST", address, strings.NewReader("abc"))
			malformed.Header = nativehttp.Header{"content-length": {"7"}}
			if receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "invalid"}, malformed); receipt != nil || err == nil {
				t.Fatal("inconsistent request framing entered native code")
			}
		})
	}
}
