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
	"context"
	"crypto/tls"
	"fmt"
	"io"
	stdhttp "net/http"
	"slices"
	"sync"
	"testing"

	"github.com/enetx/surf/profiles/chrome"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	utls "github.com/refraction-networking/utls"
)

func TestConcurrentNativeInputsAndIndependentConformance(t *testing.T) {
	for _, mode := range []ProtocolMode{HTTP1Only, HTTP2Only, PreferHTTP3} {
		t.Run(string(mode), func(t *testing.T) {
			var mu sync.Mutex
			seen := make(map[string]string)
			peer := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
				data, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				id := r.Header.Get("X-Call")
				if r.Header.Get("Cookie") != "value="+id {
					t.Error("runtime Cookie isolation changed")
				}
				mu.Lock()
				if _, exists := seen[id]; exists {
					t.Error("unexpected replay")
				}
				seen[id] = string(data)
				mu.Unlock()
				_, _ = w.Write(data)
			}, mode == PreferHTTP3)
			profile := chrome.Desktop
			options := OptionsV1{Name: "concurrent", Mode: mode, MaxActive: 4, QueuedCalls: 32,
				Native: NativeOptionsV1{Profile: &profile, TLSConfig: &tls.Config{RootCAs: peer.roots}}}
			fix := newFixture(t, options, 24)
			ctx := testContext(t)
			var group sync.WaitGroup
			for index := range 24 {
				id := fmt.Sprintf("call-%02d", index)
				group.Go(func() {
					input := request(t, "POST", peer.tcp.URL, "body-"+id)
					input.Header.Set("X-Call", id)
					input.Header.Set("Cookie", "value="+id)
					receipt, err := fix.client.Do(ctx, ctx, fault.Correlation{Call: id}, input)
					if err != nil || receipt == nil {
						t.Error("native concurrent request", err)
						return
					}
					result, err := receipt.WaitReleased(ctx)
					if err != nil || result.Err() != nil || !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "body-"+id {
						t.Error("direct result mismatch", err)
					}
				})
			}
			group.Wait()
			access, err := resource.AccessFor(fix.assembly, fix.selected)
			if err != nil {
				t.Fatal(err)
			}
			expected := make([]conformance.Expected[Result], 0, 24)
			mu.Lock()
			if len(seen) != 24 {
				t.Fatal("independent peer did not observe complete workload")
			}
			for index := range 24 {
				id := fmt.Sprintf("call-%02d", index)
				if seen[id] != "body-"+id {
					t.Fatal("peer observed misattribution")
				}
				expected = append(expected, conformance.Expected[Result]{
					Context: fault.Context{Provider: ProviderID, Source: "concurrent", Scope: "test", Operation: "request", Correlation: fault.Correlation{Call: id}},
					Source:  access.Info(), Limits: access.Limits(), Shape: invocation.Finite, Present: true, Final: true, Released: true,
					Attempts: invocation.Attempts{Observed: 1},
					Value: func(t testing.TB, value Result) {
						if !value.Complete() || string(value.DataCopy()) != "body-"+id {
							t.Error("independent payload mismatch")
						}
					},
				})
			}
			mu.Unlock()
			conformance.Receive(t, ctx, fix.inbox, expected)
		})
	}
}

type customExtension struct {
	*utls.GenericExtension
	opaque *customExtension
}

func TestCustomHelloFactoryAppearsOnWireWithoutNativeHandleEscape(t *testing.T) {
	var mu sync.Mutex
	var extensions [][]uint16
	configuration := &tls.Config{GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		mu.Lock()
		extensions = append(extensions, slices.Clone(hello.Extensions))
		mu.Unlock()
		return nil, nil
	}}
	peer := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) { _, _ = io.WriteString(w, "ok") }, false, configuration)
	factory := func(context.Context) (utls.ClientHelloSpec, error) {
		spec, err := utls.UTLSIdToSpec(utls.HelloChrome_Auto)
		if err != nil {
			return spec, err
		}
		extension := &customExtension{GenericExtension: &utls.GenericExtension{Id: 0xffa5, Data: []byte{1, 2, 3}}}
		extension.opaque = extension
		spec.Extensions = append(spec.Extensions, extension)
		return spec, nil
	}
	options := OptionsV1{Name: "custom", Mode: HTTP1Only, Native: NativeOptionsV1{HelloSpecFactory: factory, TLSConfig: &tls.Config{RootCAs: peer.roots}}}
	fix := newFixture(t, options, 1)
	receipt, err := fix.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "custom"}, request(t, "GET", peer.tcp.URL, ""))
	if err != nil {
		t.Fatal(err)
	}
	value := settle(t, fix, receipt)
	if !value.Outcome.Value.Complete() {
		t.Fatal("custom spec failed")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(extensions) == 0 || !slices.Contains(extensions[0], uint16(0xffa5)) {
		t.Fatal("factory was invoked but its native extension disappeared from wire")
	}
}
