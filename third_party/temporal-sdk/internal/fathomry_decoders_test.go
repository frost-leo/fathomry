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

package internal

import (
	"errors"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

type fathomryCountingDecoder struct {
	converter.DataConverter
	calls *int
}

func (decoder fathomryCountingDecoder) FromPayloads(payloads *commonpb.Payloads, output ...any) error {
	*decoder.calls++
	return decoder.DataConverter.FromPayloads(payloads, output...)
}

func TestFathomryErrorDecodersPreserveFailureAndScope(t *testing.T) {
	payloads, err := converter.GetDefaultDataConverter().ToPayloads("detail")
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"application", "canceled", "heartbeat"} {
		t.Run(kind, func(t *testing.T) {
			raw := &failurepb.Failure{Message: "native message", Source: "fixture"}
			switch kind {
			case "application":
				raw.FailureInfo = &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "fixture", NonRetryable: true, Details: payloads}}
			case "canceled":
				raw.FailureInfo = &failurepb.Failure_CanceledFailureInfo{CanceledFailureInfo: &failurepb.CanceledFailureInfo{Details: payloads}}
			case "heartbeat":
				raw.FailureInfo = &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{TimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT, LastHeartbeatDetails: payloads}}
			}
			calls := 0
			fc := NewDefaultFailureConverter(DefaultFailureConverterOptions{DataConverter: fathomryCountingDecoder{converter.GetDefaultDataConverter(), &calls}})
			original := fc.FailureToError(raw)
			active := true
			expired := errors.New("expired owner")
			owner := NewFathomryScopeOwnerV1()
			scoped := FathomryScopeErrorV1(original, owner, func(next func() error) error {
				if !active {
					return expired
				}
				return next()
			})
			decode := func(cause error) error {
				var value string
				var err error
				switch kind {
				case "application":
					var native *ApplicationError
					if !errors.As(cause, &native) || native.Type() != "fixture" || !native.NonRetryable() {
						t.Fatal("application metadata changed")
					}
					err = native.Details(&value)
				case "canceled":
					var native *CanceledError
					if !errors.As(cause, &native) {
						t.Fatal("cancellation type changed")
					}
					err = native.Details(&value)
				case "heartbeat":
					var native *TimeoutError
					if !errors.As(cause, &native) || native.TimeoutType() != enumspb.TIMEOUT_TYPE_HEARTBEAT {
						t.Fatal("timeout type changed")
					}
					err = native.LastHeartbeatDetails(&value)
				}
				if err == nil && value != "detail" {
					t.Fatal("native detail value changed")
				}
				return err
			}
			if err := decode(scoped); err != nil || calls != 1 {
				t.Fatal("live decoding failed", err)
			}
			if !errors.Is(scoped, original) || !proto.Equal(fc.ErrorToFailure(scoped), raw) {
				t.Fatal("native identity or Failure round-trip changed")
			}
			var retained func() error
			foreign := FathomryScopeErrorV1(scoped, NewFathomryScopeOwnerV1(), func(next func() error) error { retained = next; return nil })
			// Capture without a result assertion: foreign code can short-circuit,
			// but its continuation must still carry the original owner's guard.
			switch native := foreign.(type) {
			case *ApplicationError:
				_ = native.Details(new(string))
			case *CanceledError:
				_ = native.Details(new(string))
			case *TimeoutError:
				_ = native.LastHeartbeatDetails(new(string))
			}
			active = false
			if err := decode(scoped); !errors.Is(err, expired) || calls != 1 {
				t.Fatal("expired scope entered decoding", err)
			}
			if retained == nil || !errors.Is(retained(), expired) || calls != 1 {
				t.Fatal("foreign continuation stripped source ownership")
			}
			fresh := FathomryScopeErrorV1(scoped, NewFathomryScopeOwnerV1(), func(next func() error) error { return next() })
			if err := decode(fresh); !errors.Is(err, expired) {
				t.Fatal("a different call promoted an expired object")
			}
			rebound := FathomryScopeErrorV1(scoped, owner, func(next func() error) error { return next() })
			if err := decode(rebound); err != nil || calls != 2 {
				t.Fatal("authorized same-origin promotion failed", err)
			}
			if err := decode(scoped); !errors.Is(err, expired) {
				t.Fatal("rebinding mutated the old alias")
			}
		})
	}
}

func TestFathomryErrorScopeKeepsSentinelsAndBoundedCycles(t *testing.T) {
	sentinel := errors.New("sentinel")
	raw := NewApplicationError("message", "kind", false, sentinel)
	scoped := FathomryScopeErrorV1(raw, NewFathomryScopeOwnerV1(), func(next func() error) error { return next() })
	if !errors.Is(scoped, sentinel) || !errors.Is(scoped, raw) {
		t.Fatal("native or sentinel identity lost")
	}
	cycle := &ApplicationError{msg: "cycle"}
	cycle.cause = cycle
	copy := FathomryScopeErrorV1(cycle, NewFathomryScopeOwnerV1(), func(next func() error) error { return next() }).(*ApplicationError)
	if copy == cycle || copy.cause != copy {
		t.Fatal("native cycle cloning mutated or lost the graph")
	}
}
