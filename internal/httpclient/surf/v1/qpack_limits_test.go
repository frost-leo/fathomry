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
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"strings"
	"sync/atomic"
	"testing"

	sdk "github.com/enetx/surf"
	"github.com/enetx/surf/profiles"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func reviewQPACKFixture(t *testing.T, options OptionsV1, layers ...resource.Layer) *fixture {
	t.Helper()
	prepared, err := PrepareV1(options, layers...)
	if err != nil {
		t.Fatal(err)
	}
	selected := resource.WithLimits(prepared.Select(), prepared.Metadata().Limits)
	assembly, err := resource.Assemble(testContext(t), testContext(t), "qpack-review", selected)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](1, prepared.Metadata().EvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := &fixture{client: client, assembly: assembly, selected: selected, inbox: inbox}
	t.Cleanup(func() {
		if err := assembly.Close(testContext(t)); err != nil {
			t.Error(err)
		}
	})
	return result
}

func TestQPACKLazyCeilingsPrecedeNativeEffects(t *testing.T) {
	for _, scenario := range []struct {
		name                          string
		table, blocked                int
		advertised, advertisedBlocked uint64
		mode                          ProtocolMode
		refused                       bool
	}{
		{"exact", 64, 1, 64, 1, PreferHTTP3, false},
		{"table-one-over", 64, 1, 65, 1, PreferHTTP3, true},
		{"blocked-one-over", 64, 1, 64, 2, PreferHTTP3, true},
		{"explicit-zero-table-control", 0, 0, 0, 0, PreferHTTP3, false},
		{"explicit-zero-table-refusal", 0, 0, 1, 0, PreferHTTP3, true},
		{"explicit-zero-blocked-control", 64, 0, 64, 0, PreferHTTP3, false},
		{"explicit-zero-blocked-refusal", 64, 0, 64, 1, PreferHTTP3, true},
		{"inactive-h3-settings-h1-control", 0, 0, 65536, 100, HTTP1Only, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var profileCalls, packets, dials, requests atomic.Int64
			peer := newPeers(t, func(writer stdhttp.ResponseWriter, request *stdhttp.Request) {
				requests.Add(1)
				_, _ = io.WriteString(writer, request.Proto)
			}, true)
			profile := reviewMinimalVariant()
			profile.ConfigureH3 = func(settings profiles.H3Config) {
				profileCalls.Add(1)
				settings.QpackMaxTableCapacity(scenario.advertised).QpackBlockedStreams(scenario.advertisedBlocked)
			}
			options := OptionsV1{Name: "qpack-limit", Mode: scenario.mode, Native: NativeOptionsV1{Profile: &profile,
				TLSConfig: &tls.Config{RootCAs: peer.roots},
				DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
					dials.Add(1)
					return (&net.Dialer{}).DialContext(ctx, network, address)
				},
				ListenPacket: func(ctx context.Context, network, address string) (net.PacketConn, error) {
					packets.Add(1)
					return (&net.ListenConfig{}).ListenPacket(ctx, network, address)
				}}}
			layer := resource.Layer{Kind: resource.Local, Content: fmt.Appendf(nil, "max_http3_qpack_table_bytes: %d\nmax_http3_qpack_blocked_streams: %d\n", scenario.table, scenario.blocked)}
			fixture := reviewQPACKFixture(t, options, layer)
			if profileCalls.Load() != 0 || packets.Load() != 0 || dials.Load() != 0 {
				t.Fatal("offline QPACK declaration executed native work")
			}
			receipt, direct := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: scenario.name}, request(t, "GET", peer.tcp.URL, ""))
			result := settle(t, fixture, receipt)
			if profileCalls.Load() != 1 {
				t.Fatal("lazy profile invocation changed", profileCalls.Load())
			}
			if scenario.refused {
				if !errors.Is(direct, sdk.ErrFathomryProfileLimit) || !errors.Is(result.Err(), sdk.ErrFathomryProfileLimit) || result.Outcome.Value.Complete() || packets.Load() != 0 || dials.Load() != 0 || requests.Load() != 0 {
					t.Fatal("lazy QPACK ceiling failed before native effects", direct, result.Err(), packets.Load(), dials.Load(), requests.Load())
				}
				return
			}
			want := "HTTP/3.0"
			if scenario.mode == HTTP1Only {
				want = "HTTP/1.1"
			}
			if direct != nil || result.Err() != nil || !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != want || requests.Load() != 1 {
				t.Fatal("in-declaration QPACK protocol control failed", direct, result.Err(), result.Outcome.Value.Metadata().Protocol())
			}
		})
	}
}

func TestQPACKPreparationBudgetAxes(t *testing.T) {
	options := OptionsV1{Name: "qpack-budget", Mode: PreferHTTP3, MaxRoutes: 1, MaxActive: 1, MaxHTTP3Clients: 2,
		MaxHTTP3QPACKTableBytes: 65536, MaxHTTP3QPACKBlockedStreams: 128, MaxHeaderBytes: 1024, MaxNativeHeaderBytes: 4096}
	base := reviewPrepared(t, options).Metadata()
	options.MaxHTTP3QPACKTableBytes++
	table := reviewPrepared(t, options).Metadata()
	if table.SourceBytes-base.SourceBytes != 2*32 || table.WorkBytes != base.WorkBytes {
		t.Fatal("per-client table declaration not charged solely to source", table.SourceBytes-base.SourceBytes, table.WorkBytes-base.WorkBytes)
	}
	options.MaxHTTP3QPACKTableBytes--
	options.MaxHTTP3QPACKBlockedStreams++
	blocked := reviewPrepared(t, options).Metadata()
	if blocked.SourceBytes-base.SourceBytes != 2*(128+2*64) || blocked.WorkBytes != base.WorkBytes {
		t.Fatal("blocked descriptors/feedback declaration not source-resident", blocked.SourceBytes-base.SourceBytes, blocked.WorkBytes-base.WorkBytes)
	}
	options.MaxHTTP3QPACKBlockedStreams--
	options.MaxNativeHeaderBytes++
	root := reviewPrepared(t, options).Metadata()
	if root.WorkBytes-base.WorkBytes != 10 {
		t.Fatal("eager H3 field decode bound absent from actual root", root.WorkBytes-base.WorkBytes)
	}
	for _, layer := range []string{"max_http3_qpack_table_bytes: -1", "max_http3_qpack_table_bytes: 67108865", "max_http3_qpack_blocked_streams: -1", "max_http3_qpack_blocked_streams: 1025"} {
		if _, err := PrepareV1(options, resource.Layer{Kind: resource.Local, Content: []byte(layer)}); err == nil {
			t.Fatal("unrepresentable QPACK declaration admitted", layer)
		}
	}
}

func TestQPACKDecodedLimitKeepsRetainedSibling(t *testing.T) {
	peer := newReviewQPACKPeer(t, 4096, 1)
	first := peer.request(peer.ctx)
	connection := peer.connect()
	firstStream := peer.requestStream(connection)
	peer.instructions(connection, 2, append(reviewQPACKCapacity(4096), reviewQPACKInsert("x-large", strings.Repeat("v", 1024), false)...), false)
	peer.feedbackFor("increment", 1)
	if err := reviewQPACKFrame(firstStream, 1, []byte{0, 0, 0xd9}); err != nil {
		t.Fatal(err)
	}
	if err := reviewQPACKFrame(firstStream, 0, []byte("prefix-")); err != nil {
		t.Fatal(err)
	}
	healthy := <-first
	if healthy.err != nil || healthy.response == nil {
		t.Fatal("healthy retained stream did not start", healthy.err)
	}
	defer healthy.response.Body.Close()
	second := peer.request(peer.ctx)
	secondStream := peer.requestStream(connection)
	peer.response(secondStream, []byte{2, 0, 0xd9, 0x80, 0x80})
	refused := <-second
	if refused.err == nil {
		if refused.response != nil {
			_ = refused.response.Body.Close()
		}
		t.Fatal("decoded field-list limit was ignored")
	}
	select {
	case <-connection.Context().Done():
		t.Fatal("local field-list ceiling killed retained sibling connection", context.Cause(connection.Context()))
	default:
	}
	if err := reviewQPACKFrame(firstStream, 0, []byte("suffix")); err != nil {
		t.Fatal("healthy sibling lost write path after local limit", err)
	}
	if err := firstStream.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(healthy.response.Body)
	if err != nil || string(data) != "prefix-suffix" {
		t.Fatal("healthy sibling did not survive local field limit", err, string(data))
	}
}
