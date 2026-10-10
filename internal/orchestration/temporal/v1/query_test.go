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

package temporal

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

type queryNativeValue struct {
	payloads     *commonpb.Payloads
	reads        atomic.Int32
	payloadReads atomic.Int32
	get          func(any) error
}

func (value *queryNativeValue) HasValue() bool { return value.payloads != nil }
func (value *queryNativeValue) Get(output any) error {
	value.reads.Add(1)
	if value.get != nil {
		return value.get(output)
	}
	return converter.GetDefaultDataConverter().FromPayloads(value.payloads, output)
}
func (value *queryNativeValue) Payloads() *commonpb.Payloads {
	value.payloadReads.Add(1)
	return value.payloads
}

type queryOpaqueValue struct{ converter.EncodedValue }

func testQueryValue(t *testing.T, client *Executions, native converter.EncodedValue) *QueryValue {
	t.Helper()
	value, err := newQueryValue(context.Background(), client, native, nil)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func releaseQueryEvidence(t *testing.T, client *Executions) {
	t.Helper()
	record, err := client.inbox.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := record.Receipt().WaitReleased(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := record.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestQueryValueRawPayloadsPreserveOptionalSupportAndBounds(t *testing.T) {
	client, _, _ := bridgeFixture(t, 8)
	payloads, err := converter.GetDefaultDataConverter().ToPayloads("result")
	if err != nil {
		t.Fatal(err)
	}
	native := &queryNativeValue{payloads: payloads}
	value := testQueryValue(t, client, native)
	copy, supported, err := value.RawPayloads(context.Background(), fault.Correlation{Call: "payloads"})
	if err != nil || !supported || !proto.Equal(copy, payloads) || native.payloadReads.Load() != 1 {
		t.Fatal("raw native payload inspection lost", supported, err)
	}
	releaseQueryEvidence(t, client)
	copy.Payloads[0].Data = []byte("mutation")
	var decoded string
	if err := value.Get(context.Background(), fault.Correlation{Call: "decode"}, &decoded); err != nil || decoded != "result" {
		t.Fatal("raw copy mutated retained encoding", err)
	}
	releaseQueryEvidence(t, client)

	opaque := testQueryValue(t, client, queryOpaqueValue{native})
	if payloads, supported, err := opaque.RawPayloads(context.Background(), fault.Correlation{Call: "opaque"}); err != nil || supported || payloads != nil {
		t.Fatal("invented optional payload support", supported, err)
	}
	releaseQueryEvidence(t, client)
	if native.payloadReads.Load() != 1 {
		t.Fatal("unsupported accessor invoked hidden callback")
	}
	absent := testQueryValue(t, client, &queryNativeValue{})
	if payloads, supported, err := absent.RawPayloads(context.Background(), fault.Correlation{Call: "absent"}); err != nil || !supported || payloads != nil || absent.HasValue() {
		t.Fatal("nil supported payload conflated with absent interface", supported, err)
	}
	releaseQueryEvidence(t, client)
	large := testQueryValue(t, client, &queryNativeValue{payloads: &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte(strings.Repeat("x", 1025))}}}})
	if payloads, supported, err := large.RawPayloads(context.Background(), fault.Correlation{Call: "too-large"}); !errors.Is(err, ErrLimit) || !supported || payloads != nil {
		t.Fatal("oversized custom payload copied outside bound", supported, err)
	}
	releaseQueryEvidence(t, client)
}

func TestQueryValueAdmissionPrecedesCustomAccessorAndAbruptExitReleases(t *testing.T) {
	client, _, _ := bridgeFixture(t, 1)
	payloads, _ := converter.GetDefaultDataConverter().ToPayloads("result")
	native := &queryNativeValue{payloads: payloads}
	value := testQueryValue(t, client, native)
	var decoded string
	if err := value.Get(context.Background(), fault.Correlation{Call: "first"}, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, _, err := value.RawPayloads(context.Background(), fault.Correlation{Call: "full"}); err == nil || native.payloadReads.Load() != 0 {
		t.Fatal("full evidence entered custom accessor", err)
	}
	releaseQueryEvidence(t, client)
	for _, exit := range []string{"panic", "goexit"} {
		t.Run(exit, func(t *testing.T) {
			native.get = func(any) error {
				if exit == "panic" {
					panic("query decoder panic")
				}
				runtime.Goexit()
				return nil
			}
			joined := make(chan struct{})
			go func() {
				defer close(joined)
				defer func() { _ = recover() }()
				_ = value.Get(context.Background(), fault.Correlation{Call: exit}, &decoded)
			}()
			<-joined
			releaseQueryEvidence(t, client)
			native.get = nil
			if err := value.Get(context.Background(), fault.Correlation{Call: exit + "-after"}, &decoded); err != nil || decoded != "result" {
				t.Fatal("abrupt decoder retained serialization/admission slot", err)
			}
			releaseQueryEvidence(t, client)
		})
	}
}
