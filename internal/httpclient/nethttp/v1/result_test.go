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

package nethttp

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
)

func TestResultCopiesAndRuntimePrivacy(t *testing.T) {
	secret := "synthetic-secret"
	result := Result{data: &resultData{metadata: Metadata{headers: http.Header{"Authorization": {secret}}, url: "https://invalid.example/?token=" + secret},
		body: []byte(secret), retained: true, trailers: http.Header{"X-Secret": {secret}}, complete: true}}
	body := result.DataCopy()
	body[0] = 'x'
	header := result.Metadata().HeadersCopy()
	header.Set("Authorization", "changed")
	trailer := result.TrailersCopy()
	trailer.Set("X-Secret", "changed")
	if string(result.DataCopy()) != secret || result.Metadata().HeadersCopy().Get("Authorization") != secret || result.TrailersCopy().Get("X-Secret") != secret {
		t.Fatal("result aliases mutable caller data")
	}
	conformance.Runtime(t, result, new(Result), secret)
	conformance.Runtime(t, result.Metadata(), new(Metadata), secret)
	conformance.Runtime(t, OptionsV1{Name: "private", ProxyURL: "http://user:" + secret + "@invalid.example", ClientKeyPEM: secret}, new(OptionsV1), secret)
	conformance.Runtime(t, NativeOptionsV1{}, new(NativeOptionsV1), secret)
}

func TestRetainedBodyFitsDeclaredEvidence(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		for _, maximum := range []int64{1 << 20, 8 << 20} {
			payload := bytes.Repeat([]byte("x"), int(maximum))
			server, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) { _, _ = writer.Write(payload) })
			options.MaxResponseBytes, options.MaxHeaderBytes = maximum, 1024
			f := bindFixture(t, options, 1)
			receipt, err := f.client.Do(deadline(t), deadline(t), correlation("envelope"), newRequest(t, "GET", server.URL, nil))
			if err != nil {
				t.Fatal(err)
			}
			usage := f.inbox.Usage()
			if usage.Outstanding != 1 || usage.ReservedBytes != f.client.EvidenceBytes() {
				t.Fatal("retained output lost its independent reservation")
			}
			result := settle(t, f, receipt)
			stored := result.Outcome.Value.data.body
			if !bytes.Equal(stored, payload) || int64(cap(stored)) > maximum || !result.Outcome.Value.Complete() {
				t.Fatal("retained backing allocation exceeded its envelope or changed output")
			}
			if f.inbox.Usage().ReservedBytes != 0 {
				t.Fatal("released delivery retained accounting")
			}
		}
	})
}

func TestNativeZeroStatusIsAnObservedResponse(t *testing.T) {
	server, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) {
		connection, _, err := writer.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer connection.Close()
		_, _ = io.WriteString(connection, "HTTP/1.1 000 Synthetic\r\nContent-Length: 2\r\n\r\nok")
	})
	f := bindFixture(t, options, 1)
	receipt, err := f.client.Do(deadline(t), deadline(t), correlation("zero-status"), newRequest(t, "GET", server.URL, nil))
	if err != nil {
		t.Fatal(err)
	}
	result := settle(t, f, receipt)
	data := result.Outcome.Value
	if !result.Outcome.Present || !data.Complete() || data.Metadata().StatusCode() != 0 || data.Metadata().ContentLength() != 2 || string(data.DataCopy()) != "ok" {
		t.Fatal("native status was conflated with absent response data")
	}
	if (Metadata{}).ContentLength() != -1 {
		t.Fatal("missing length became observed zero")
	}
}
