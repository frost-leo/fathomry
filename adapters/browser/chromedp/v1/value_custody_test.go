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

package chromedp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestLiveOrdinaryEvaluationReleasesExceptionReferences(t *testing.T) {
	owner, retained, dependencies := objectFixture(t)
	_, receipt, err := RunAction(bounded(t), retained.Page(), Evaluate[int]("throw new Error('private-value-exception')"))
	var cause *sdk.ExceptionError
	if !errors.As(err, &cause) || cause.ExceptionDetails == nil || cause.ExceptionDetails.Exception == nil {
		t.Fatal("native exception inspection was lost", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || !errors.As(snapshot.Primary(), &cause) || snapshot.Cleanup() != nil {
		t.Fatal("terminal grouped exception did not release custody independently", err)
	}
	value, ok := snapshot.ValueCopy()
	if !ok || !value.Object().GroupReleased {
		t.Fatal("exception group release was not observed")
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int]("42")); err != nil {
		t.Fatal("released exception exhausted subsequent work", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("value exception prevented shutdown")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveCanceledOrdinaryValueRetainsIndependentCustody(t *testing.T) {
	for _, poll := range []bool{false, true} {
		t.Run(map[bool]string{false: "evaluate", true: "poll"}[poll], func(t *testing.T) {
			owner, retained, dependencies := objectFixture(t)
			ctx, cancel := context.WithTimeout(retained.Context(), 30*time.Millisecond)
			action := Evaluate[int]("new Promise(resolve => { globalThis.finishValue = () => resolve(42) })", EvalAwaitPromise())
			if poll {
				action = PollFunction[int]("() => new Promise(resolve => {globalThis.finishValue = () => resolve(42)})")
			}
			_, receipt, err := RunAction(ctx, retained.Page(), action)
			cancel()
			if err == nil || receipt == nil {
				t.Fatal("cancellation counterexample did not run")
			}
			if snapshot, _ := receipt.Snapshot(); snapshot.Info().Released {
				t.Fatal("canceled value producer refunded its independent evidence")
			}
			if _, _, err := RunAction(bounded(t), retained.Page(), Evaluate[bool]("finishValue(); true")); err != nil {
				t.Fatal("cannot resolve the original native producer", err)
			}
			drain(t, dependencies, 1)
			if snapshot, _ := receipt.Snapshot(); snapshot.Info().Released {
				t.Fatal("an unrelated operation proved an unobserved terminal result")
			}
			_ = owner.Close(bounded(t))
			if !owner.ShutdownComplete() {
				t.Fatal("positive teardown did not settle late value custody")
			}
			if _, err := receipt.WaitReleased(bounded(t)); err != nil {
				t.Fatal(err)
			}
			status, _ := dependencies.Evidence.Inspect()
			drain(t, dependencies, status.Outstanding)
		})
	}
}

type valueDecoderProbe struct{}

var valueDecodes atomic.Int64

func (*valueDecoderProbe) UnmarshalJSON([]byte) error { valueDecodes.Add(1); return nil }

func TestLiveOrdinaryValueCapacityRefusalPrecedesNativeAndDecoder(t *testing.T) {
	owner, retained, dependencies := objectFixture(t)
	first, _, err := retained.Page().NewObjectGroup(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := retained.Page().NewObjectGroup(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	before := valueDecodes.Load()
	if _, receipt, err := RunAction(bounded(t), retained.Page(), Evaluate[valueDecoderProbe]("globalThis.refusedValue = true; 1")); !errors.Is(err, ErrLimit) || receipt == nil || valueDecodes.Load() != before {
		t.Fatal("ordinary value ran before custody admission", err)
	}
	drain(t, dependencies, 1)
	if err := second.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if absent, _, err := RunAction(bounded(t), retained.Page(), Evaluate[bool]("typeof refusedValue === 'undefined'")); err != nil || !absent {
		t.Fatal("capacity refusal dispatched the value operation", err)
	}
	drain(t, dependencies, 1)
	if err := first.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("full group capacity prevented cleanup")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveOrdinaryValueDecoderRemainsJoinedAfterCancellation(t *testing.T) {
	owner, retained, dependencies := objectFixture(t)
	valueDecodeEntered = make(chan struct{})
	valueDecodeExit = make(chan struct{})
	done := make(chan struct{})
	var receipt *adapters.Receipt[Result]
	go func() {
		defer close(done)
		_, receipt, _ = RunAction(bounded(t), retained.Page(), Evaluate[blockedValueDecoder]("1"))
	}()
	<-valueDecodeEntered
	short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	_ = owner.Close(short)
	cancel()
	if owner.ShutdownComplete() {
		t.Fatal("active native decoder lost custody")
	}
	close(valueDecodeExit)
	<-done
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("decoder exit did not complete shutdown")
	}
	if _, err := receipt.WaitReleased(bounded(t)); err != nil {
		t.Fatal(err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

var valueDecodeEntered, valueDecodeExit chan struct{}

type blockedValueDecoder struct{}

func (*blockedValueDecoder) UnmarshalJSON([]byte) error {
	close(valueDecodeEntered)
	<-valueDecodeExit
	return nil
}
