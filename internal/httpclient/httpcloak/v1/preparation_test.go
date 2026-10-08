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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/klauspost/compress/zstd"
	nativehttp "github.com/sardanioss/http"
	"github.com/sardanioss/httpcloak/fingerprint"
	"github.com/sardanioss/httpcloak/transport"
	"github.com/sardanioss/quic-go/quicvarint"
)

func preparedFixture(t *testing.T, prepared Prepared, slots int) *fixture {
	t.Helper()
	selected := resource.WithLimits(prepared.Select(), prepared.Metadata().Limits)
	assembly, err := resource.Assemble(testContext(t), testContext(t), "prepared-fixture", selected)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](slots, int64(slots)*prepared.Metadata().EvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := assembly.Close(testContext(t)); err != nil {
			t.Error(err)
		}
	})
	return &fixture{client: client, assembly: assembly, selected: selected, inbox: inbox}
}

func TestPreparedPresetRegistryAndMetadataAreFrozen(t *testing.T) {
	address, options := peer(t, HTTP2, func(w http.ResponseWriter, input *http.Request) {
		_, _ = w.Write([]byte(input.Header.Get("X-Prepared")))
	})
	const name = "fathomry-gh120-prepared"
	preset := fingerprint.GetStrict("chrome-148")
	preset.HeaderOrder = []fingerprint.HeaderPair{{Key: "X-Prepared", Value: "original"}}
	fingerprint.Register(name, preset)
	t.Cleanup(func() { fingerprint.Unregister(name) })
	options.PresetName = name
	prepared, err := PrepareV1(options, resource.Layer{Kind: resource.Local, Content: []byte("max_active: 1\nmax_connections: 3\n")})
	if err != nil {
		t.Fatal(err)
	}
	metadata := prepared.Metadata()
	if metadata.Limits.Active != 1 || metadata.MaxConnections != 3 || metadata.WorkBytes <= defaults(options).reservation() || metadata.SourceBytes <= 0 {
		t.Fatal("metadata did not resolve exact selection", metadata)
	}
	preset.HeaderOrder[0].Value = "replacement"
	fingerprint.Register(name, preset)
	fixture := preparedFixture(t, prepared, 1)
	receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "frozen"}, request(t, "GET", address, nil))
	if err != nil {
		t.Fatal(err)
	}
	if result := settle(t, fixture, receipt); string(result.Outcome.Value.DataCopy()) != "original" {
		t.Fatal("construction reread mutable registry")
	}
	if prepared.Metadata() != metadata {
		t.Fatal("metadata changed after construction")
	}
}

func TestDataInputBoundPrecedesNativeCopyAndSerialization(t *testing.T) {
	for _, change := range []func(*OptionsV1){
		func(value *OptionsV1) { value.Name = strings.Repeat("n", 65) },
		func(value *OptionsV1) { value.PresetName = strings.Repeat("n", 257) },
		func(value *OptionsV1) { value.PresetJSON = strings.Repeat(" ", (1<<20)+1) },
		func(value *OptionsV1) { value.ProxyURL = strings.Repeat("p", 8193) },
		func(value *OptionsV1) { value.ResolverAddress = strings.Repeat("r", 65) },
		func(value *OptionsV1) { value.ResolverNetwork = strings.Repeat("r", 4) },
		func(value *OptionsV1) { value.Protocol = ProtocolMode(strings.Repeat("p", 17)) },
	} {
		value := OptionsV1{Name: "data-bound", PresetName: "chrome-148", Native: NativeOptionsV1{Transport: &transport.TransportConfig{EnableSpeculativeTLS: true}}}
		change(&value)
		if err := ValidateDataInputV1(value); !errors.Is(err, ErrLimit) {
			t.Fatal("hard data input bound missing", err)
		}
		if err := ValidateDataV1(value); !errors.Is(err, ErrLimit) {
			t.Fatal("strict data serialization preceded shape check", err)
		}
		if _, err := PrepareV1(value); !errors.Is(err, ErrLimit) {
			t.Fatal("native snapshot preceded data shape check", err)
		}
	}
	if err := ValidateDataInputV1(OptionsV1{Version: 2}); !errors.Is(err, ErrInput) {
		t.Fatal("unsupported format entered serialization", err)
	}
}

func TestPreparedBorrowsImmutableCertPoolsButCopiesVerificationWrappers(t *testing.T) {
	_, roots := protocolCertificate(t)
	_, proxyRoots := protocolCertificate(t)
	options := OptionsV1{Name: "borrowed-roots", PresetName: "chrome-148", Native: NativeOptionsV1{Verify: &transport.TLSVerify{RootCAs: roots}, ProxyVerify: &transport.TLSVerify{RootCAs: proxyRoots}}}
	prepared, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.native.Verify == options.Native.Verify || prepared.native.ProxyVerify == options.Native.ProxyVerify || prepared.native.Verify.RootCAs != roots || prepared.native.ProxyVerify.RootCAs != proxyRoots {
		t.Fatal("preparation cloned opaque pool memory or borrowed the mutable wrapper")
	}
	options.Native.Verify.RootCAs = proxyRoots
	options.Native.ProxyVerify.RootCAs = roots
	fixture := preparedFixture(t, prepared, 1)
	if fixture.client.owner.native.Verify.RootCAs != roots || fixture.client.owner.native.ProxyVerify.RootCAs != proxyRoots {
		t.Fatal("construction changed the frozen borrowed pool pointers")
	}
}

func TestRegisteredPresetIsBoundedBeforeSnapshot(t *testing.T) {
	const name = "fathomry-gh120-large-registered"
	preset := fingerprint.GetStrict("chrome-148")
	preset.HeaderOrder = []fingerprint.HeaderPair{{Key: "X-Large", Value: strings.Repeat("x", 1<<20)}}
	fingerprint.Register(name, preset)
	t.Cleanup(func() { fingerprint.Unregister(name) })
	if _, err := PrepareV1(OptionsV1{Name: "registered-bound", PresetName: name}); !errors.Is(err, ErrLimit) {
		t.Fatal("oversized registered preset entered preparation", err)
	}
}

func TestPreparationApplicabilityAndExplicitZero(t *testing.T) {
	options := OptionsV1{Name: "prepared", PresetName: "chrome-148", Protocol: HTTP3}
	prepared, err := PrepareV1(options)
	if err != nil {
		t.Fatal("literal H3 requires no ambient resolver", err)
	}
	input := request(t, "GET", "https://127.0.0.1/", nil)
	if err := prepared.RequestPolicy().Validate(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if err := prepared.RequestPolicy().Validate(context.Background(), request(t, "GET", "https://target.invalid/", nil)); !errors.Is(err, ErrInput) {
		t.Fatal("unapproved direct DNS admitted", err)
	}
	if _, err := PrepareV1(options, resource.Layer{Kind: resource.Local, Content: []byte("max_address_races: 0\n")}); err == nil {
		t.Fatal("explicit zero was defaulted twice")
	}
	options.Native.Transport = &transport.TransportConfig{ECHConfigDomain: "ech.invalid"}
	if _, err := PrepareV1(options); !errors.Is(err, ErrInput) {
		t.Fatal("configured discovery lacked authority", err)
	}
	options.DisableECH = true
	if _, err := PrepareV1(options); err != nil {
		t.Fatal("disabled discovery retained sideband authority", err)
	}
	options.Native.Transport = &transport.TransportConfig{EnableSpeculativeTLS: true}
	if _, err := PrepareV1(options); !errors.Is(err, ErrUnsupported) {
		t.Fatal("inert speculative switch admitted", err)
	}
	options.Native.Transport = &transport.TransportConfig{CustomPseudoOrder: []string{":path", ":authority", ":method", ":scheme"}}
	if _, err := PrepareV1(options); err != nil {
		t.Fatal("native H3 pseudo-header order was removed", err)
	}
}

func TestPreparationQUICIdleTimeoutRequiresH3(t *testing.T) {
	for _, protocol := range []ProtocolMode{HTTP1, HTTP2, HTTP3} {
		options := OptionsV1{Name: "quic-idle", PresetName: "chrome-148", Protocol: protocol, Native: NativeOptionsV1{Transport: &transport.TransportConfig{QuicIdleTimeout: time.Second}}}
		_, err := PrepareV1(options)
		if protocol == HTTP3 {
			if err != nil {
				t.Fatal("effective H3 idle timeout was refused", err)
			}
		} else if !errors.Is(err, ErrUnsupported) {
			t.Fatal("TCP-only protocol accepted an inert QUIC timeout", protocol, err)
		}
	}
}

func TestPreparationH3WireScalarsAreValidatedBeforeConstruction(t *testing.T) {
	for _, field := range []string{"max-field-section", "max-datagram"} {
		for _, scalar := range []uint64{quicvarint.Max, quicvarint.Max + 1} {
			preset := fingerprint.GetStrict("chrome-148")
			if preset.H3Config == nil {
				preset.H3Config = &fingerprint.H3FingerprintConfig{}
			}
			if field == "max-field-section" {
				preset.H3Config.MaxFieldSectionSize = &scalar
			} else {
				preset.H3Config.QUICMaxDatagramFrameSize = &scalar
			}
			_, err := PrepareV1(OptionsV1{Name: "wire-bound", Protocol: HTTP3, Native: NativeOptionsV1{Preset: preset}})
			if scalar == quicvarint.Max && err != nil || scalar > quicvarint.Max && !errors.Is(err, ErrInput) {
				t.Fatal("H3 scalar overflow or exact-bound control changed", field, scalar, err)
			}
		}
	}
	for _, length := range []int{-1, 0, 8, 20, 21} {
		preset := fingerprint.GetStrict("chrome-148")
		if preset.H3Config == nil {
			preset.H3Config = &fingerprint.H3FingerprintConfig{}
		}
		preset.H3Config.QUICConnectionIDLength = &length
		_, err := PrepareV1(OptionsV1{Name: "connection-id-bound", Protocol: HTTP3, Native: NativeOptionsV1{Preset: preset}})
		valid := length >= 0 && length <= 20
		if valid && err != nil || !valid && !errors.Is(err, ErrInput) {
			t.Fatal("invalid connection ID reached native slice allocation", length, err)
		}
	}
}

func TestRequestHardPreflightPreservesNativeAuthorityWithoutReading(t *testing.T) {
	body := &countedBody{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input := request(t, "POST", "https://127.0.0.1/", body).WithContext(ctx)
	input.Header = nativehttp.Header{"X-Input": {"value"}}
	input.Trailer = nativehttp.Header{"X-End": nil}
	option := RequestOptionsV1{Proxy: ProxyAddress, ProxyURL: "masque://127.0.0.1:443", ExactHeaders: []fingerprint.HeaderPair{{Key: "X-Wire", Value: "one"}, {Key: "X-Wire", Value: "two"}}}
	copy, frozen, err := SnapshotRequestV1(context.Background(), input, option)
	if err != nil {
		t.Fatal(err)
	}
	input.Header["X-Input"][0] = "changed"
	option.ExactHeaders[0].Value = "changed"
	if copy.Context() != ctx || copy.Body != body || copy.Header.Get("X-Input") != "value" || frozen.ExactHeaders[0].Value != "one" {
		t.Fatal("native request snapshot lost context/body/extensions or alias isolation")
	}
	if body.reads.Load() != 0 || body.closes.Load() != 0 {
		t.Fatal("snapshot acquired input")
	}
	input.Header = make(nativehttp.Header)
	for index := 0; index < 17000; index++ {
		input.Header[fmt.Sprintf("X-%s-%d", strings.Repeat("a", 32), index)] = nil
	}
	if err := ValidateRequestV1(context.Background(), input); err == nil {
		t.Fatal("empty-valued metadata bypassed bound")
	}
	input.Header = nil
	input.RequestURI = "/server-state"
	if err := ValidateRequestV1(context.Background(), input); !errors.Is(err, ErrUnsupported) {
		t.Fatal("server authority was silently copied", err)
	}
	if body.reads.Load() != 0 || body.closes.Load() != 0 {
		t.Fatal("refused request acquired input")
	}
}

func TestPreparationNativeBudgetPreservesH2Zero(t *testing.T) {
	options := OptionsV1{Name: "budget", PresetName: "chrome-148", Protocol: HTTP2, Native: NativeOptionsV1{Transport: &transport.TransportConfig{CustomH2Settings: &fingerprint.HTTP2Settings{}}}}
	prepared, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	budget := prepared.Metadata()
	if budget.Native.H2StreamBytes != 0 || budget.Native.H2FrameBytes != 1<<14 || budget.Native.H2EncoderBytes != 16<<20 || budget.Native.H2DecoderBytes != 0 {
		t.Fatal("metadata copied incorrect native defaults", budget.Native)
	}
	options.MaxECHEntries = 65
	larger, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	if larger.Metadata().SourceBytes-budget.SourceBytes < int64(defaults(options).MaxECHConfigBytes) {
		t.Fatal("cache capacity missing from source residence")
	}
}

func TestPreparationMASQUEParserBudgetUsesResolvedHeaderAndTunnelBounds(t *testing.T) {
	options := OptionsV1{Name: "masque-parser-budget", PresetName: "chrome-148", Protocol: HTTP3, MaxHeaderBytes: 1024, MaxControlStreams: 2}
	first, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := PrepareV1(options, resource.Layer{Kind: resource.Local, Content: []byte("max_header_bytes: 2048\n")})
	if err != nil {
		t.Fatal(err)
	}
	if first.Metadata().Native.MASQUEParserBytes != 64*1024 || second.Metadata().Native.MASQUEParserBytes != 64*2048 || second.Metadata().SourceBytes-first.Metadata().SourceBytes < 2*64*1024 {
		t.Fatal("structured-field parser containers are not covered by resolved source residence")
	}
	options.Protocol = HTTP2
	tcp, err := PrepareV1(options)
	if err != nil || tcp.Metadata().Native.MASQUEParserBytes != 0 {
		t.Fatal("TCP-only source unexpectedly acquired MASQUE parser residence", err)
	}
}

func TestPreparationH3RetainedBindingsOutliveActiveQUICAllowance(t *testing.T) {
	options := OptionsV1{Name: "retained-h3", PresetName: "chrome-148", Protocol: HTTP3, MaxQUICConnections: 1, MaxBindings: 1}
	first, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := PrepareV1(options, resource.Layer{Kind: resource.Local, Content: []byte("max_bindings: 3\n")})
	if err != nil {
		t.Fatal(err)
	}
	initial, extended := first.Metadata(), second.Metadata()
	if initial.Native.H3RetainedBytes < 2*initial.Native.QUICBytes || initial.Native.BindingBytes < initial.Native.H3RetainedBytes || extended.MaxQUICConnections != 1 || extended.MaxBindings != 3 || extended.SourceBytes-initial.SourceBytes < 2*initial.Native.BindingBytes {
		t.Fatal("closed native H3 graphs were charged only to active QUIC slots", initial, extended)
	}
	if first.Metadata() != initial {
		t.Fatal("replacement preparation mutated prior-generation residence")
	}
	options.Protocol = HTTP2
	tcp, err := PrepareV1(options)
	if err != nil || tcp.Metadata().Native.H3RetainedBytes != 0 {
		t.Fatal("TCP-only selection acquired H3 retained graphs", err)
	}
}

func TestPreparedRetainedBindingGraphsFollowExchangeAndCallerBounds(t *testing.T) {
	workByProtocol := make(map[int64]bool)
	for _, protocol := range []ProtocolMode{HTTP1, HTTP2, HTTP3} {
		t.Run(string(protocol), func(t *testing.T) {
			options := OptionsV1{Name: "retained-exchanges", PresetName: "chrome-148", Protocol: protocol, MaxActive: 1, QueuedCalls: 1, MaxBindings: 1, MaxExchanges: 1, MaxHeaderBytes: 1024}
			prepare := func(layer string) Budget {
				t.Helper()
				prepared, err := PrepareV1(options, resource.Layer{Kind: resource.Local, Content: []byte(layer)})
				if err != nil {
					t.Fatal(err)
				}
				return prepared.Metadata()
			}
			first := prepare("max_exchanges: 1\n")
			more := prepare("max_exchanges: 3\n")
			workByProtocol[first.WorkBytes] = true
			if first.WorkBytes < first.Native.BindingBytes || more.WorkBytes-first.WorkBytes < 2*first.Native.BindingBytes+16*options.MaxHeaderBytes {
				t.Fatal("closed redirect bindings were reserved only in the source cache", first.WorkBytes, more.WorkBytes, first.Native.BindingBytes)
			}
			if first.RetainedBindingBytes < first.Native.BindingBytes || more.WorkBytes-first.WorkBytes != 2*first.RetainedBindingBytes+16*options.MaxHeaderBytes {
				t.Fatal("per-exchange residence was not derived from the exact frozen binding unit")
			}
			if more.EvidenceBytes != first.EvidenceBytes || more.Native != first.Native {
				t.Fatal("retained exchange graphs changed immutable native selection or final evidence")
			}
			parallel := prepare("max_exchanges: 3\nmax_active: 3\nqueued_calls: 2\n")
			if parallel.WorkBytes != more.WorkBytes || parallel.Limits.Bytes != 3*more.WorkBytes || parallel.Limits.QueuedBytes != 2*more.WorkBytes {
				t.Fatal("active/queued roots bypassed retained graph accounting")
			}
			cached := prepare("max_exchanges: 3\nmax_bindings: 3\n")
			if cached.WorkBytes != more.WorkBytes || cached.SourceBytes-more.SourceBytes < 2*more.Native.BindingBytes {
				t.Fatal("cached and per-root retained graphs were conflated")
			}
		})
	}
	if len(workByProtocol) != 3 {
		t.Fatal("H1/H2/H3 recommendations ignored their selected native graph differences")
	}
}

func TestTLSZstdStreamingWindowRemainsBounded(t *testing.T) {
	for _, concurrency := range []int{1, 2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			positive := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x20, 1, 9, 0, 0, 'a'}
			reader, err := zstd.NewReader(bytes.NewReader(positive), zstd.WithDecoderConcurrency(concurrency), zstd.WithDecoderMaxWindow(1024), zstd.WithDecoderMaxMemory(2<<20))
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(reader)
			reader.Close()
			if err != nil || string(data) != "a" {
				t.Fatal("normal streaming single-segment frame failed", err)
			}
			oversized := []byte{0x28, 0xb5, 0x2f, 0xfd, 0xe0}
			oversized = binary.LittleEndian.AppendUint64(oversized, 1<<20)
			oversized = append(oversized, 9, 0, 0, 'a')
			reader, err = zstd.NewReader(bytes.NewReader(oversized), zstd.WithDecoderConcurrency(concurrency), zstd.WithDecoderMaxWindow(1024), zstd.WithDecoderMaxMemory(2<<20))
			if err == nil {
				_, err = io.ReadAll(reader)
				reader.Close()
			}
			if !errors.Is(err, zstd.ErrDecoderSizeExceeded) {
				t.Fatal("single-segment content size bypassed streaming window bound", err)
			}
		})
	}
}
