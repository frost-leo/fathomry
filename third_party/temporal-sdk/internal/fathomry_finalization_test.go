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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
)

func finalizationNativeFixture(t testing.TB, kind string) (error, *failurepb.Failure, func(error) error) {
	t.Helper()
	payloads, err := converter.GetDefaultDataConverter().ToPayloads("detail")
	if err != nil {
		t.Fatal(err)
	}
	wire := &failurepb.Failure{Message: "native failure", Source: "fixture"}
	switch kind {
	case "application":
		wire.FailureInfo = &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "fixture", NonRetryable: true, Details: payloads}}
	case "canceled":
		wire.FailureInfo = &failurepb.Failure_CanceledFailureInfo{CanceledFailureInfo: &failurepb.CanceledFailureInfo{Details: payloads}}
	case "heartbeat":
		wire.FailureInfo = &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{TimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT, LastHeartbeatDetails: payloads}}
	default:
		t.Fatal("unsupported fixture kind", kind)
	}
	decode := func(value error) error {
		var detail string
		var err error
		switch kind {
		case "application":
			var application *ApplicationError
			if !errors.As(value, &application) {
				return errors.New("native ApplicationError shape lost")
			}
			err = application.Details(&detail)
		case "canceled":
			var canceled *CanceledError
			if !errors.As(value, &canceled) {
				return errors.New("native CanceledError shape lost")
			}
			err = canceled.Details(&detail)
		case "heartbeat":
			var timeout *TimeoutError
			if !errors.As(value, &timeout) {
				return errors.New("native TimeoutError shape lost")
			}
			err = timeout.LastHeartbeatDetails(&detail)
		}
		if err == nil && detail != "detail" {
			return errors.New("native detail value changed")
		}
		return err
	}
	return GetDefaultFailureConverter().FailureToError(wire), wire, decode
}

func TestFathomryFinalizationCopiesOnlyItsCallbackDecoderScopes(t *testing.T) {
	for _, kind := range []string{"application", "canceled", "heartbeat"} {
		t.Run(kind, func(t *testing.T) {
			original, wire, decode := finalizationNativeFixture(t, kind)
			group := NewFathomryScopeOwnerV1()
			expired := errors.New("callback expired")
			var active atomic.Bool
			active.Store(true)
			guard := func(next func() error) error {
				if !active.Load() {
					return expired
				}
				return next()
			}
			borrowed := FathomryScopeErrorV1(original, NewFathomryChildScopeOwnerV1(group), guard)
			borrowed = FathomryScopeErrorV1(borrowed, NewFathomryChildScopeOwnerV1(group), guard)
			if err := decode(borrowed); err != nil {
				t.Fatal("live callback positive control", err)
			}
			active.Store(false)
			prepared := FathomryPrepareErrorFinalizationV1(borrowed, group)
			if prepared == borrowed || reflect.TypeOf(prepared) != reflect.TypeOf(borrowed) {
				t.Fatal("finalization did not preserve a distinct native-shaped copy")
			}
			if err := decode(prepared); err == nil {
				t.Fatal("prepared error decoded outside native conversion")
			}
			var retained error
			converted := FathomryConvertErrorV1(prepared, func(value error) *failurepb.Failure {
				retained = value
				if err := decode(value); err != nil {
					t.Error("native conversion lost callback-owned details", err)
				}
				return GetDefaultFailureConverter().ErrorToFailure(value)
			})
			if !proto.Equal(converted, wire) {
				t.Fatal("native Failure round-trip changed")
			}
			if err := decode(borrowed); !errors.Is(err, expired) {
				t.Fatal("finalization revived the original callback error", err)
			}
			if err := decode(retained); err == nil {
				t.Fatal("retained converter argument outlived native conversion")
			}
		})
	}
}

func TestFathomryFinalizationPreservesForeignDecoderRestrictions(t *testing.T) {
	original, _, decode := finalizationNativeFixture(t, "application")
	group := NewFathomryScopeOwnerV1()
	callbackExpired, foreignExpired := errors.New("callback expired"), errors.New("foreign expired")
	var foreignActive atomic.Bool
	foreignActive.Store(true)
	borrowed := FathomryScopeErrorV1(original, NewFathomryChildScopeOwnerV1(group), func(func() error) error { return callbackExpired })
	borrowed = FathomryScopeErrorV1(borrowed, NewFathomryScopeOwnerV1(), func(next func() error) error {
		if !foreignActive.Load() {
			return foreignExpired
		}
		return next()
	})
	if FathomryPrepareErrorFinalizationV1(borrowed, NewFathomryScopeOwnerV1()) != borrowed {
		t.Fatal("foreign task group rewrote an error it does not own")
	}
	prepared := FathomryPrepareErrorFinalizationV1(borrowed, group)
	for _, permitted := range []bool{true, false} {
		foreignActive.Store(permitted)
		FathomryConvertErrorV1(prepared, func(value error) *failurepb.Failure {
			err := decode(value)
			if permitted && err != nil || !permitted && !errors.Is(err, foreignExpired) {
				t.Error("foreign decoder ownership changed", permitted, err)
			}
			return nil
		})
	}
}

func TestFathomryFinalizationPreservesKnownNativeWrappers(t *testing.T) {
	for _, kind := range []string{"workflow", "activity", "child", "server", "nexus-operation", "nexus-handler", "nexus-result"} {
		t.Run(kind, func(t *testing.T) {
			original, _, decode := finalizationNativeFixture(t, "application")
			group := NewFathomryScopeOwnerV1()
			borrowed := FathomryScopeErrorV1(original, NewFathomryChildScopeOwnerV1(group), func(func() error) error { return errors.New("expired callback") })
			var wrapped error
			switch kind {
			case "workflow":
				wrapped = &WorkflowExecutionError{workflowID: "workflow", runID: "run", workflowType: "fixture", cause: borrowed}
			case "activity":
				wrapped = &ActivityError{activityID: "activity", cause: borrowed}
			case "child":
				wrapped = &ChildWorkflowExecutionError{workflowID: "child", cause: borrowed}
			case "server":
				wrapped = &ServerError{msg: "server", nonRetryable: true, cause: borrowed}
			case "nexus-operation":
				wrapped = &NexusOperationError{Operation: "operation", Cause: borrowed}
			case "nexus-handler":
				wrapped = &nexus.HandlerError{Type: nexus.HandlerErrorTypeInternal, Cause: borrowed}
			case "nexus-result":
				wrapped = &nexus.OperationError{State: nexus.OperationStateFailed, Message: "operation failed", Cause: borrowed}
			}
			wire := GetDefaultFailureConverter().ErrorToFailure(wrapped)
			FathomryConvertErrorV1(FathomryPrepareErrorFinalizationV1(wrapped, group), func(value error) *failurepb.Failure {
				if reflect.TypeOf(value) != reflect.TypeOf(wrapped) || value.Error() != wrapped.Error() {
					t.Error("native wrapper type or metadata changed")
				}
				if err := decode(value); err != nil {
					t.Error("known native cause did not receive finalization scope", err)
				}
				if !proto.Equal(GetDefaultFailureConverter().ErrorToFailure(value), wire) {
					t.Error("native wrapper Failure round-trip changed")
				}
				return wire
			})
			if err := decode(wrapped); err == nil {
				t.Fatal("copy revived original native cause")
			}
		})
	}
}

func TestFathomryFinalizationLeavesUnownedAndOpaqueErrorsUntouched(t *testing.T) {
	original, _, _ := finalizationNativeFixture(t, "application")
	group := NewFathomryScopeOwnerV1()
	borrowed := FathomryScopeErrorV1(original, NewFathomryChildScopeOwnerV1(group), func(func() error) error { return errors.New("expired callback") })
	var typedNil *ApplicationError
	cycle := &ApplicationError{msg: "unscoped cycle"}
	cycle.cause = cycle
	long := error(original)
	for index := 0; index < 80; index++ {
		long = &ApplicationError{msg: "unscoped chain", cause: long}
	}
	values := []error{nil, typedNil, original, errors.New("plain"), ErrActivityResultPending, cycle, long,
		fmt.Errorf("opaque wrapper: %w", borrowed), errors.Join(borrowed, errors.New("other"))}
	for index, original := range values {
		prepared := FathomryPrepareErrorFinalizationV1(original, group)
		if prepared != original {
			t.Errorf("unowned or opaque error %d was rewritten", index)
		}
		FathomryConvertErrorV1(prepared, func(value error) *failurepb.Failure {
			if value != original {
				t.Errorf("unmarked converter argument %d changed identity", index)
			}
			return nil
		})
	}
}

type finalizationCustomError struct {
	scope  *FathomryDecoderScopeV1
	decode func() error
}

func (*finalizationCustomError) Error() string { return "cooperative custom failure" }

func (value *finalizationCustomError) Details() error {
	if value.scope == nil {
		return value.decode()
	}
	return value.scope.Decode(value.decode)
}

func (value *finalizationCustomError) FathomryMapDecoderScopeV1(change func(*FathomryDecoderScopeV1) *FathomryDecoderScopeV1) error {
	mapped := change(value.scope)
	if mapped == value.scope {
		return value
	}
	copy := *value
	copy.scope = mapped
	return &copy
}

func finalizationCustomFixture(decode func() error) (*finalizationCustomError, error) {
	group := NewFathomryScopeOwnerV1()
	original := FathomryScopeErrorV1(&finalizationCustomError{decode: decode}, NewFathomryChildScopeOwnerV1(group), func(func() error) error {
		return errors.New("callback expired")
	}).(*finalizationCustomError)
	return original, FathomryPrepareErrorFinalizationV1(original, group)
}

func TestFathomryFinalizationCooperatingCustomShapeAndScope(t *testing.T) {
	var decodes atomic.Int32
	original, prepared := finalizationCustomFixture(func() error { decodes.Add(1); return nil })
	var retained *finalizationCustomError
	FathomryConvertErrorV1(prepared, func(value error) *failurepb.Failure {
		var ok bool
		retained, ok = value.(*finalizationCustomError)
		if !ok || retained.Details() != nil {
			t.Fatal("custom native shape or decoding was lost")
		}
		return &failurepb.Failure{Message: value.Error(), EncodedAttributes: &commonpb.Payload{Data: []byte("unchanged")}}
	})
	if decodes.Load() != 1 || original.Details() == nil || retained.Details() == nil || decodes.Load() != 1 {
		t.Fatal("custom decoder escaped its conversion window")
	}
	unscoped := &finalizationCustomError{decode: func() error { return nil }}
	if FathomryPrepareErrorFinalizationV1(unscoped, NewFathomryScopeOwnerV1()) != unscoped {
		t.Fatal("unscoped custom error was rewritten")
	}
	if _, err := json.Marshal(retained.scope); err == nil {
		t.Fatal("custom decoder scope serialized")
	}
	var reconstructed FathomryDecoderScopeV1
	if err := json.Unmarshal([]byte(`{}`), &reconstructed); err == nil {
		t.Fatal("custom decoder scope reconstructed from JSON")
	}
}

func TestFathomryFinalizationCooperatingCustomKeepsForeignScope(t *testing.T) {
	group := NewFathomryScopeOwnerV1()
	foreign := errors.New("foreign expired")
	var decoded bool
	original := &finalizationCustomError{decode: func() error { decoded = true; return nil }}
	borrowed := FathomryScopeErrorV1(original, NewFathomryChildScopeOwnerV1(group), func(func() error) error { return errors.New("callback expired") })
	borrowed = FathomryScopeErrorV1(borrowed, NewFathomryScopeOwnerV1(), func(func() error) error { return foreign })
	FathomryConvertErrorV1(FathomryPrepareErrorFinalizationV1(borrowed, group), func(value error) *failurepb.Failure {
		if err := value.(*finalizationCustomError).Details(); !errors.Is(err, foreign) || decoded {
			t.Error("custom scope mapping removed a foreign restriction", err)
		}
		return nil
	})
}

type finalizationLegacyError struct{ guard func(func() error) error }

func (*finalizationLegacyError) Error() string { return "legacy custom error" }
func (value *finalizationLegacyError) FathomryScopeDecodersV1(guard func(func() error) error) error {
	return &finalizationLegacyError{guard: guard}
}

func TestFathomryFinalizationDoesNotInventLegacyCustomCooperation(t *testing.T) {
	group := NewFathomryScopeOwnerV1()
	expired := errors.New("callback expired")
	borrowed := FathomryScopeErrorV1(&finalizationLegacyError{}, NewFathomryChildScopeOwnerV1(group), func(func() error) error { return expired })
	prepared := FathomryPrepareErrorFinalizationV1(borrowed, group)
	FathomryConvertErrorV1(prepared, func(value error) *failurepb.Failure {
		if prepared != borrowed || value != borrowed {
			t.Error("legacy custom shape was automatically rewritten")
		}
		if err := value.(*finalizationLegacyError).guard(func() error { t.Error("legacy callback was revived"); return nil }); !errors.Is(err, expired) {
			t.Error("legacy custom owner changed", err)
		}
		return nil
	})
}

func TestFathomryFinalizationWindowJoinsActiveDecodeAndSealsNewEntry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		var decodes atomic.Int32
		original, prepared := finalizationCustomFixture(func() error {
			if decodes.Add(1) == 1 {
				close(entered)
				<-release
			}
			return nil
		})
		var retained *finalizationCustomError
		converted, decoded := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(converted)
			FathomryConvertErrorV1(prepared, func(value error) *failurepb.Failure {
				retained = value.(*finalizationCustomError)
				go func() {
					defer close(decoded)
					if err := retained.Details(); err != nil {
						t.Error("admitted finalization decode failed", err)
					}
				}()
				<-entered
				return nil
			})
		}()
		<-entered
		synctest.Wait()
		select {
		case <-converted:
			t.Fatal("conversion returned before its active decoder joined")
		default:
		}
		if original.Details() == nil || retained.Details() == nil || decodes.Load() != 1 {
			t.Fatal("closing finalization window admitted a new decoder")
		}
		close(release)
		<-decoded
		<-converted
		if retained.Details() == nil || decodes.Load() != 1 {
			t.Fatal("finished conversion revived a retained decoder")
		}
	})
}

func TestFathomryFinalizationWindowClosesOnPanicAndGoexit(t *testing.T) {
	for _, mode := range []string{"panic", "goexit"} {
		t.Run(mode, func(t *testing.T) {
			_, prepared := finalizationCustomFixture(func() error { return nil })
			var retained *finalizationCustomError
			completed := make(chan struct{})
			go func() {
				defer close(completed)
				defer func() {
					cause := recover()
					if mode == "panic" && cause != "fixture panic" || mode == "goexit" && cause != nil {
						t.Error("native abnormal exit was changed", cause)
					}
				}()
				FathomryConvertErrorV1(prepared, func(value error) *failurepb.Failure {
					retained = value.(*finalizationCustomError)
					if err := retained.Details(); err != nil {
						t.Error("positive control failed", err)
					}
					if mode == "panic" {
						panic("fixture panic")
					}
					runtime.Goexit()
					return nil
				})
				t.Error("native abnormal exit returned normally")
			}()
			<-completed
			if retained == nil || retained.Details() == nil {
				t.Fatal("abnormal conversion exit retained decoder authority")
			}
		})
	}
}

type finalizationMappingExitState struct {
	mode     string
	armed    bool
	retained *FathomryDecoderScopeV1
}

type finalizationMappingExitError struct {
	scope *FathomryDecoderScopeV1
	state *finalizationMappingExitState
}

func (*finalizationMappingExitError) Error() string { return "custom mapping exit" }
func (value *finalizationMappingExitError) FathomryMapDecoderScopeV1(change func(*FathomryDecoderScopeV1) *FathomryDecoderScopeV1) error {
	copy := *value
	copy.scope = change(value.scope)
	if value.state.armed {
		value.state.retained = copy.scope
		if value.state.mode == "panic" {
			panic("mapping panic")
		}
		runtime.Goexit()
	}
	return &copy
}

func TestFathomryFinalizationWindowClosesWhenCustomMappingExits(t *testing.T) {
	for _, mode := range []string{"panic", "goexit"} {
		t.Run(mode, func(t *testing.T) {
			state := &finalizationMappingExitState{mode: mode}
			group := NewFathomryScopeOwnerV1()
			borrowed := FathomryScopeErrorV1(&finalizationMappingExitError{state: state}, NewFathomryChildScopeOwnerV1(group), func(func() error) error { return errors.New("callback expired") })
			prepared := FathomryPrepareErrorFinalizationV1(borrowed, group)
			state.armed = true
			completed := make(chan struct{})
			go func() {
				defer close(completed)
				defer func() {
					cause := recover()
					if mode == "panic" && cause != "mapping panic" || mode == "goexit" && cause != nil {
						t.Error("custom mapping exit changed", cause)
					}
				}()
				FathomryConvertErrorV1(prepared, func(error) *failurepb.Failure {
					t.Error("native converter ran after custom mapping exit")
					return nil
				})
				t.Error("custom mapping exit returned normally")
			}()
			<-completed
			if state.retained == nil {
				t.Fatal("custom mapping never received decoder scope")
			}
			if err := state.retained.Decode(func() error { t.Error("exited mapping retained native decoder authority"); return nil }); err == nil {
				t.Fatal("custom mapping abnormal exit did not close the conversion window")
			}
		})
	}
}

func TestFathomryFinalizationPreservesEveryKnownOriginIdentity(t *testing.T) {
	for _, kind := range []string{"application", "canceled", "heartbeat", "activity", "child", "server", "workflow"} {
		t.Run(kind, func(t *testing.T) {
			fixtureKind := kind
			if kind != "canceled" && kind != "heartbeat" {
				fixtureKind = "application"
			}
			original, _, decode := finalizationNativeFixture(t, fixtureKind)
			switch kind {
			case "activity":
				original = &ActivityError{cause: original}
			case "child":
				original = &ChildWorkflowExecutionError{cause: original}
			case "server":
				original = &ServerError{cause: original}
			case "workflow":
				original = &WorkflowExecutionError{cause: original}
			}
			group := NewFathomryScopeOwnerV1()
			firstOwner, secondOwner := NewFathomryChildScopeOwnerV1(group), NewFathomryChildScopeOwnerV1(group)
			guard := func(next func() error) error { return next() }
			first := FathomryScopeErrorV1(original, firstOwner, guard)
			second := FathomryScopeErrorV1(first, secondOwner, guard)
			rebound := FathomryScopeErrorV1(second, firstOwner, guard)
			prepared := FathomryPrepareErrorFinalizationV1(rebound, group)
			ancestors := []error{original, first, second, rebound, prepared}
			for index, value := range ancestors {
				for prior := 0; prior <= index; prior++ {
					if !errors.Is(value, ancestors[prior]) {
						t.Errorf("known native copy %d lost origin %d", index, prior)
					}
				}
			}
			FathomryConvertErrorV1(prepared, func(value error) *failurepb.Failure {
				for index, prior := range ancestors {
					if !errors.Is(value, prior) {
						t.Errorf("conversion copy lost known native origin %d", index)
					}
				}
				if err := decode(value); err != nil {
					t.Error("preserving identity changed native details", err)
				}
				return nil
			})
		})
	}
}

type finalizationOriginTrap struct{ calls *int }

func (*finalizationOriginTrap) Error() string { return "opaque user cause" }
func (trap *finalizationOriginTrap) Is(error) bool {
	*trap.calls++
	return true
}
func (trap *finalizationOriginTrap) Unwrap() error {
	*trap.calls++
	return nil
}

func TestFathomryOriginIdentityDoesNotExecuteUserCauseCallbacks(t *testing.T) {
	calls := 0
	root := &ApplicationError{cause: &finalizationOriginTrap{calls: &calls}}
	owner := NewFathomryScopeOwnerV1()
	guard := func(next func() error) error { return next() }
	first := FathomryScopeErrorV1(root, owner, guard)
	second := FathomryScopeErrorV1(first, owner, guard).(*ApplicationError)
	if !second.fathomryErrorOrigin.Is(root) {
		t.Error("private origin traversal lost its root")
	}
	if second.fathomryErrorOrigin.Is(errors.New("unrelated")) || calls != 0 {
		t.Fatal("private origin traversal executed or selected a user cause")
	}
}
