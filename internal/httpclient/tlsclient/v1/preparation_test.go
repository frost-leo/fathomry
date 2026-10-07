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
	"io"
	"net"
	"net/http"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bogdanfinn/fhttp/http2"
	"github.com/bogdanfinn/tls-client/profiles"
	tls "github.com/bogdanfinn/utls"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestFrozenPreparationMetadataAndLayerAuthority(t *testing.T) {
	options := providerOptions()
	options.Mode = HTTP1Only
	var calls atomic.Int64
	id := tls.HelloChrome_120
	id.SpecFactory = func() (tls.ClientHelloSpec, error) { calls.Add(1); return profiles.Chrome_120.GetClientHelloSpec() }
	profile := profileWithID(*options.Native.Profile, id)
	options.Native.Profile = &profile
	address := net.TCPAddr{IP: net.IPv4(127, 0, 0, 2)}
	options.Native.LocalAddr = &address
	options.Native.Dialer = &net.Dialer{Timeout: time.Hour}
	options.Native.DefaultHeaders = map[string][]string{"X-Frozen": {"original"}}
	prepared, err := PrepareV1(options, resource.Layer{Kind: resource.Local, Content: []byte("{\"timeout_ns\":1700000000,\"queued_calls\":2,\"bandwidth\":false}")})
	if err != nil {
		t.Fatal(err)
	}
	budget := prepared.Metadata()
	options.Native.DefaultHeaders["X-Frozen"][0] = "changed"
	address.IP[0] = 99
	if calls.Load() != 0 || budget.Timeout != 1700*time.Millisecond || budget.Limits.Queued != 2 || budget.Limits.Bytes != 8*budget.WorkBytes || budget.Limits.QueuedBytes != 2*budget.WorkBytes ||
		prepared.native.DefaultHeaders.Get("X-Frozen") != "original" || prepared.native.LocalAddr.IP.Equal(address.IP) || prepared.native.Dialer.Timeout != budget.Timeout {
		t.Fatal("frozen offline final selection mismatch")
	}
	selected := resource.WithLimits(prepared.Select(), budget.Limits)
	assembly, err := resource.Assemble(testContext(t), testContext(t), "prepared", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(context.Background())
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(source.owner.budget, budget) || calls.Load() != 0 {
		t.Fatal("construction changed authoritative preparation")
	}
}
func TestPreparationBudgetCoversEffectiveNativeBuffers(t *testing.T) {
	options := providerOptions()
	options.Mode = HTTP1Only
	options.DisableSessionTickets = true
	prepare := func(value OptionsV1) Budget {
		t.Helper()
		prepared, err := PrepareV1(value)
		if err != nil {
			t.Fatal(err)
		}
		return prepared.Metadata()
	}
	base := prepare(options)
	options.MaxTCPConnections = 64
	connections := prepare(options)
	if connections.SourceBytes <= base.SourceBytes || connections.WorkBytes != base.WorkBytes {
		t.Fatal("source sockets charged to wrong envelope")
	}
	options.MaxTCPConnections = 0
	options.DisableSessionTickets = false
	if cached := prepare(options); cached.SourceBytes <= base.SourceBytes+int64(16*32*(1<<24)) {
		t.Fatal("owned cache/decoded peer state omitted")
	}
	options.DisableSessionTickets = true
	options.Mode = Negotiated
	negotiated := prepare(options)
	if negotiated.SourceBytes <= base.SourceBytes || negotiated.WorkBytes <= base.WorkBytes {
		t.Fatal("native HTTP2 buffers omitted")
	}
	original := *options.Native.Profile
	settings := original.GetSettings()
	old := settings[http2.SettingInitialWindowSize]
	cloned := map[http2.SettingID]uint32{}
	for key, value := range settings {
		cloned[key] = value
	}
	cloned[http2.SettingInitialWindowSize] = old + 4096
	profile := profiles.NewClientProfile(original.GetClientHelloId(), cloned, original.GetSettingsOrder(), original.GetPseudoHeaderOrder(), original.GetConnectionFlow(), original.GetPriorities(), original.GetHeaderPriority(),
		original.GetStreamID(), original.GetAllowHTTP(), original.GetHttp3Settings(), original.GetHttp3SettingsOrder(), original.GetHttp3PriorityParam(), original.GetHttp3PseudoHeaderOrder(), original.GetHttp3SendGreaseFrames())
	options.Native.Profile = &profile
	if changed := prepare(options); changed.WorkBytes-negotiated.WorkBytes != 4096 {
		t.Fatal("effective profile stream window not authoritative")
	}
	options.Mode = HTTP3Racing
	if racing := prepare(options); racing.SourceBytes <= 0 || racing.WorkBytes <= negotiated.WorkBytes {
		t.Fatal("native racing buffers omitted")
	}
}
func TestLazyProfileDeclarationRefusesOversizedFactoryInput(t *testing.T) {
	for _, size := range []int{16, 4096} {
		t.Run(string(rune('a'+size%25)), func(t *testing.T) {
			var requests, factories atomic.Int64
			endpoint, options := providerPeer(t, HTTP1Only, func(writer http.ResponseWriter, request *http.Request) {
				requests.Add(1)
				_, _ = io.WriteString(writer, "profile")
			})
			options.MaxProfileBytes = 2048
			options.DisableSessionTickets = true
			id := tls.HelloChrome_120
			id.SpecFactory = func() (tls.ClientHelloSpec, error) {
				factories.Add(1)
				spec, err := profiles.Chrome_120.GetClientHelloSpec()
				spec.Extensions = append(spec.Extensions, &tls.GenericExtension{Id: 65000, Data: make([]byte, size)})
				return spec, err
			}
			profile := profileWithID(*options.Native.Profile, id)
			options.Native.Profile = &profile
			prepared, err := PrepareV1(options)
			if err != nil || factories.Load() != 0 {
				t.Fatal("offline factory invocation", err)
			}
			if prepared.Metadata().ProfileBytes != 2048 {
				t.Fatal("profile declaration missing")
			}
			fixture := bindProvider(t, options, 1)
			receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("profile"), providerRequest(t, "GET", endpoint, nil))
			result := settleProvider(t, fixture, receipt)
			if size > 2048 {
				if !errors.Is(err, ErrLimit) || requests.Load() != 0 || result.Outcome.Value.Complete() {
					t.Fatal("oversized factory accepted", err)
				}
			} else if err != nil || requests.Load() != 1 || !result.Outcome.Value.Complete() {
				t.Fatal("valid profile refused", err)
			}
		})
	}
}

func TestNativePlaceholderProfileHasSeparateEnvelope(t *testing.T) {
	options := providerOptions()
	options.MaxProfileBytes = 1024
	profile := profileWithID(*options.Native.Profile, tls.HelloGolang)
	options.Native.Profile = &profile
	prepared, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Metadata().ProfileBytes != (1<<24)-1 {
		t.Fatal("native placeholder used an inapplicable factory declaration")
	}
}
