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

package tls_client

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	nativehttp "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/tls-client/profiles"
)

func TestFathomrySetupIgnoresOrphanedCachedKind(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, "complete") }))
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	client, err := NewHttpClient(NewNoopLogger(), WithClientProfile(profiles.Chrome_150), WithDisableHttp3(), WithForceHttp1(), WithTransportOptions(&TransportOptions{RootCAs: roots}))
	if err != nil {
		t.Fatal(err)
	}
	defer Close(client)
	rt := client.(*httpClient).Transport.(*roundTripper)
	address := server.Listener.Addr().String()
	rt.cachedKinds[address] = transportHTTP1
	request, _ := nativehttp.NewRequest("GET", server.URL, nil)
	func() {
		rt.cachedTransportsLck.Lock()
		defer rt.cachedTransportsLck.Unlock()
		if err := rt.getTransport(request, address); err != nil {
			t.Fatal(err)
		}
	}()
	response, err := client.Do(request)
	if err != nil {
		t.Fatal("transport setup failed in half-dropped cache state", err)
	}
	data, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || string(data) != "complete" {
		t.Fatal("response changed", readErr, closeErr)
	}
}

func TestFathomryCloseJoinsNativeHook(t *testing.T) {
	entered, canceled, resume := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	defer unblock()
	client, err := NewHttpClient(NewNoopLogger(), WithClientProfile(profiles.Chrome_150), WithDisableHttp3(), WithPreHook(func(request *nativehttp.Request) error {
		close(entered)
		<-request.Context().Done()
		close(canceled)
		<-resume
		return request.Context().Err()
	}))
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	request, _ := nativehttp.NewRequest("GET", "https://not-contacted.invalid/", nil)
	go func() { _, err := client.Do(request); result <- err }()
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- Close(client) }()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel native hook context")
	}
	select {
	case <-closed:
		t.Fatal("Close released an active native hook")
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("native hook did not finish")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("native hook cleanup did not join")
	}
}
