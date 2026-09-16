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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestNativeContainersAreFrozenBeforeAssembly(t *testing.T) {
	const secret = "private-header-canary"
	peer := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if r.Header.Get("X-Frozen") != secret {
			t.Error("selected headers borrowed mutable storage")
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer peer.Close()
	options := OptionsV1{Name: "frozen", Mode: HTTP1Only, Native: NativeOptionsV1{Headers: map[string][]string{"X-Frozen": {secret}}}}
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	limits, err := LimitsV1(options)
	if err != nil {
		t.Fatal(err)
	}
	options.Native.Headers["X-Frozen"][0] = "mutated"
	selected = resource.WithLimits(selected, limits)
	assembly, err := resource.Assemble(testContext(t), testContext(t), "frozen", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(testContext(t))
	inbox, err := invocation.NewInbox[Result](1, defaults(options).evidenceBytes())
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := client.Do(testContext(t), testContext(t), fault.Correlation{Call: "frozen"}, request(t, "GET", peer.URL, ""))
	if err != nil {
		t.Fatal(err)
	}
	fix := &fixture{client: client, assembly: assembly, selected: selected, inbox: inbox}
	value := settle(t, fix, receipt)
	if !value.Outcome.Value.Complete() {
		t.Fatal("frozen request incomplete")
	}
	conformance.Private(t, client, secret)
	conformance.Private(t, client.Profile(), secret)
}
func TestTLSConfigAndRootPoolDoNotAliasCaller(t *testing.T) {
	peer := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		t.Error("mutated trust configuration reached peer")
	}, false)
	config := &tls.Config{RootCAs: x509.NewCertPool()}
	options := OptionsV1{Name: "trust", Mode: HTTP1Only, Native: NativeOptionsV1{TLSConfig: config}}
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	limits, err := LimitsV1(options)
	if err != nil {
		t.Fatal(err)
	}
	config.InsecureSkipVerify = true
	config.RootCAs.AddCert(peer.tcp.Certificate())
	selected = resource.WithLimits(selected, limits)
	assembly, err := resource.Assemble(testContext(t), testContext(t), "trust", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(testContext(t))
	inbox, err := invocation.NewInbox[Result](1, defaults(options).evidenceBytes())
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := client.Do(testContext(t), testContext(t), fault.Correlation{Call: "trust"}, request(t, "GET", peer.tcp.URL, ""))
	if err == nil {
		t.Fatal("caller mutation changed selected trust")
	}
	value := settle(t, &fixture{client: client, assembly: assembly, selected: selected, inbox: inbox}, receipt)
	var unknown x509.UnknownAuthorityError
	if !errors.As(value.Err(), &unknown) {
		t.Fatal("native trust cause lost")
	}
}
func TestRuntimeValuesRefusePersistenceAndRedactNativeValues(t *testing.T) {
	const secret = "runtime-secret-canary"
	options := OptionsV1{Name: "private", ProxyURL: "http://owner:" + secret + "@127.0.0.1:1"}
	native := NativeOptionsV1{Headers: map[string][]string{"X-Secret": {secret}}}
	runtime := RequestOptionsV1{ProxyURL: &options.ProxyURL}
	for _, value := range []any{options, &options, native, &native, runtime, &runtime} {
		for _, format := range []string{"%v", "%+v", "%#v", "%q"} {
			conformance.Private(t, fmt.Sprintf(format, value), secret)
		}
	}
	conformance.Runtime(t, options, &OptionsV1{}, secret)
	conformance.Runtime(t, native, &NativeOptionsV1{}, secret)
	conformance.Runtime(t, runtime, &RequestOptionsV1{}, secret)
}
