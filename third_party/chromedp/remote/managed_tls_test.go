/*
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

package remote

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/chromedp"
	"github.com/chromedp/chromedp/remote/internal/wstest"
)

func TestManagedControlTLSRequiresExplicitTrustAndPeerName(t *testing.T) {
	server := httptest.NewTLSServer(wstest.Handler(t, func(_ *http.Request, conn *wstest.Conn) {
		mustWrite(t, conn, wstest.OpText, []byte(`{"id":7,"result":{}}`))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if conn, err := DialContext(ctx, wstest.URL(server)); err == nil {
		conn.Close()
		t.Fatal("untrusted control certificate was accepted")
	} else {
		var authority x509.UnknownAuthorityError
		if !errors.As(err, &authority) {
			t.Fatal("fixture did not reach certificate validation", err)
		}
	}
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	configuration := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	conn, err := DialContext(ctx, wstest.URL(server), WithConnTLSConfig(configuration))
	if err != nil {
		t.Fatal("explicit trust did not reach the native dialer", err)
	}
	defer conn.Close()
	var message cdproto.Message
	if err := conn.Read(ctx, &message); err != nil || message.ID != 7 {
		t.Fatal("trusted control connection did not carry CDP", err)
	}
	wrongName := configuration.Clone()
	wrongName.ServerName = "not-the-fixture.invalid"
	if conn, err := DialContext(ctx, wstest.URL(server), WithConnTLSConfig(wrongName)); err == nil {
		conn.Close()
		t.Fatal("explicit trust disabled peer-name checks")
	}
	if configuration.ServerName != "" || configuration.InsecureSkipVerify {
		t.Fatal("dial changed caller TLS policy")
	}
}

func TestManagedAllocatorPassesControlTLS(t *testing.T) {
	server := httptest.NewTLSServer(wstest.Handler(t, func(_ *http.Request, conn *wstest.Conn) {
		for {
			frame, err := conn.ReadFrame()
			if err != nil || frame.Opcode == wstest.OpClose {
				return
			}
			var command cdproto.Message
			if err := jsonv2.Unmarshal(frame.Payload, &command); err != nil {
				t.Error(err)
				return
			}
			result := jsonv2.Value(`{}`)
			if string(command.Method) == browser.GetVersion.Method {
				result = jsonv2.Value(`{"protocolVersion":"1.3","product":"fixture","revision":"fixture","userAgent":"fixture","jsVersion":"fixture"}`)
			}
			data, err := jsonv2.Marshal(cdproto.Message{ID: command.ID, Result: result})
			if err != nil {
				t.Error(err)
				return
			}
			if err := conn.WriteFrame(wstest.OpText, data); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	allocator, stopAllocator := NewAllocator(ctx, wstest.URL(server), NoModifyURL, WithDialTLSConfig(&tls.Config{RootCAs: roots}))
	defer stopAllocator()
	work, stop := chromedp.NewContext(allocator)
	defer stop()
	version, err := chromedp.CallBrowser(work, browser.GetVersion, cdp.Empty{})
	if err != nil || version.Product != "fixture" {
		t.Fatal("allocator TLS configuration did not reach its connection", err)
	}
}
