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
	"errors"
	"net/url"
	"testing"

	nativehttp "github.com/bogdanfinn/fhttp"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/invocation"
)

func TestProviderPrivateErrorsAndRuntimeContracts(t *testing.T) {
	secret := "synthetic-secret-canary"
	cause := &url.Error{Op: "GET", URL: "https://invalid.example/?token=" + secret, Err: context.Canceled}
	cleanup := errors.New("synthetic-cleanup-canary")
	result := invocation.Result[Result]{Outcome: invocation.Outcome[Result]{Primary: failure(ErrTransport, "request", cause), Cleanup: failure(ErrCleanup, "body", cleanup)}}
	if !errors.Is(result.Err(), ErrTransport) || !errors.Is(result.Err(), context.Canceled) || !errors.Is(result.Err(), cleanup) {
		t.Fatal("native cause identity lost")
	}
	conformance.Cause[*url.Error](t, result.Err(), func(value *url.Error) bool { return value == cause })
	conformance.Private(t, result.Err(), secret, "synthetic-cleanup-canary")
	data := Result{data: &resultData{metadata: Metadata{present: true, url: secret, headers: nativehttp.Header{"Cookie": {secret}}}, body: []byte(secret), retained: true, warnings: []error{cause}}}
	conformance.Runtime(t, data, new(Result), secret)
	conformance.Runtime(t, data.Metadata(), new(Metadata), secret)
	conformance.Runtime(t, OptionsV1{Name: secret, ProxyURL: "http://user:" + secret + "@localhost"}, new(OptionsV1), secret)
	conformance.Runtime(t, NativeOptionsV1{DefaultHeaders: nativehttp.Header{"Cookie": {secret}}}, new(NativeOptionsV1), secret)
	conformance.Runtime(t, RequestOptionsV1{Proxy: ProxyAddress, ProxyURL: secret}, new(RequestOptionsV1), secret)
	conformance.Runtime(t, Source{}, new(Source), secret)
	conformance.Runtime(t, Client{}, new(Client), secret)
	conformance.Runtime(t, Stream{}, new(Stream), secret)
}
