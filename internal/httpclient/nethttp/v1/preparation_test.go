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
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestPreparedResolvedNativeBudgetAndCopy(t *testing.T) {
	var calls atomic.Int64
	headers := http.Header{"X-Proxy": {"original"}}
	config := &tls.Config{ServerName: "original.invalid", NextProtos: []string{"h2"}}
	options := OptionsV1{Name: "prepared", ProxyConnectHeader: headers,
		Native: NativeOptionsV1{TLS: config, DialContext: func(context.Context, string, string) (net.Conn, error) {
			calls.Add(1)
			return nil, errors.New("unexpected dial")
		}}}
	original, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareV1(options, resource.Layer{Kind: resource.Local, Content: []byte("max_response_bytes: 12345\nexpect_continue_timeout_ns: 0\nhttp2: false\n")})
	if err != nil {
		t.Fatal(err)
	}
	meta := prepared.Metadata()
	if calls.Load() != 0 || meta.WorkBytes < 1 || meta.SourceBytes < 1 || meta.EvidenceBytes != 12345+4*(64<<10)+4096 ||
		meta.WorkBytes == original.Metadata().WorkBytes || meta.Limits.Bytes != int64(meta.Limits.Active)*meta.WorkBytes {
		t.Fatal("resolved budgets do not describe final native selection")
	}
	headers.Set("X-Proxy", "changed")
	config.ServerName = "changed.invalid"
	config.NextProtos[0] = "changed"
	selected := resource.WithLimits(prepared.Select(), meta.Limits)
	assembly, err := resource.Assemble(deadline(t), deadline(t), "prepared", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(deadline(t))
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	if source.owner.settings.ExpectContinueTimeout != 0 || source.owner.transport.ExpectContinueTimeout != 0 ||
		source.owner.settings.HTTP2 || source.owner.settings.ProxyConnectHeader.Get("X-Proxy") != "original" ||
		source.owner.native.TLS.ServerName != "original.invalid" || source.owner.budget != meta || calls.Load() != 0 {
		t.Fatal("construction changed preparation")
	}
	inbox, _ := invocation.NewInbox[Result](1, meta.EvidenceBytes)
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil || client.EvidenceBytes() != meta.EvidenceBytes {
		t.Fatal("prepared bind budget differs", err)
	}
}
func TestPreparedValidationAndNativeH2Accounting(t *testing.T) {
	base, err := PrepareV1(OptionsV1{Name: "native"})
	if err != nil {
		t.Fatal(err)
	}
	larger, err := PrepareV1(OptionsV1{Name: "native", Native: NativeOptionsV1{HTTP2: &http.HTTP2Config{
		MaxReceiveBufferPerStream: 8 << 20, MaxReadFrameSize: 2 << 20, MaxDecoderHeaderTableSize: 8192}}})
	if err != nil {
		t.Fatal(err)
	}
	if larger.Metadata().WorkBytes-base.Metadata().WorkBytes != 4<<20 || larger.Metadata().SourceBytes <= base.Metadata().SourceBytes {
		t.Fatal("selected native H2 buffers absent from accounting")
	}
	for _, layer := range []string{"timeout_ns: 0", "max_response_bytes: 0", "http1: false\nhttp2: false", "expect_continue_timeout_ns: -1"} {
		if _, err := PrepareV1(OptionsV1{Name: "invalid"}, resource.Layer{Kind: resource.Local, Content: []byte(layer)}); err == nil {
			t.Fatal("invalid resolved input defaulted", layer)
		}
	}
	if _, err := PrepareV1(OptionsV1{Name: "conflict", ServerName: "conflict", Native: NativeOptionsV1{TLS: &tls.Config{}}}); err == nil {
		t.Fatal("native conflict accepted")
	}
	if _, err := PrepareV1(OptionsV1{Name: "continue", ExpectContinueTimeout: 25 * time.Hour}); err == nil {
		t.Fatal("invalid duration accepted")
	}
}
