/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package chromedp

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto/debugger"
	cdpio "github.com/chromedp/cdproto/io"
	"github.com/chromedp/cdproto/page"
	cdpruntime "github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
)

func TestProtocolLostRecordingStopMustNotStarveDebuggerDisable(t *testing.T) {
	fixture := &recordingWireFixture{faults: recordingFaults{dropStop: true}, requests: make(map[recordingWireKey]string), streams: make(map[string]cdpio.StreamHandle), startReply: make(chan struct{}), stopReply: make(chan struct{}), readReply: make(chan struct{}), stopRelease: make(chan struct{}), readRelease: make(chan struct{})}
	var resumes, disables, releases atomic.Int64
	request := func(data []byte) []byte {
		var message recordingWireMessage
		if json.Unmarshal(data, &message) == nil {
			switch message.Method {
			case debugger.Resume.Method:
				resumes.Add(1)
			case debugger.Disable.Method:
				disables.Add(1)
			case cdpruntime.ReleaseObjectGroup.Method:
				var params cdpruntime.ReleaseObjectGroupParams
				if json.Unmarshal(message.Params, &params) == nil && params.ObjectGroup == "backtrace" {
					releases.Add(1)
				}
			}
		}
		return fixture.request(data)
	}
	endpoint, peer := cleanupFaultPeerTransform(t, nil, nil, fixture.reply, request)
	prepared, err := Prepare(Settings{Name: "debugger-recording", RemoteURL: endpoint, CleanupTimeout: new(100 * time.Millisecond)}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		fixture.release()
		_ = owner.Close(bounded(t))
		if !owner.ShutdownComplete() {
			owner.state.stopNative(context.Canceled)
			t.Error("fixture cleanup incomplete")
		}
	})
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	control, _, err := retained.Page().Control(retained.Context(), ControlOptions{Debugger: &debugger.EnableParams{}, SharedIsolateEffects: true, Recording: &page.StartScreenRecordingParams{}})
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		_ = control.Close(bounded(t))
	}
	t.Logf("lost recording stop: resume=%d disable=%d releaseBacktrace=%d controlClosed=%v", resumes.Load(), disables.Load(), releases.Load(), control.Closed())
	if resumes.Load() != 1 || disables.Load() != 1 || releases.Load() != 1 {
		t.Error("independent debugger cleanup was starved by recording completion wait")
	}
	if control.Closed() {
		t.Error("lost recording completion falsely settled")
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Error("owned page destruction did not settle source")
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int]("31")); err != nil || value != 31 {
		t.Error("peer lost", err)
	}
}

func TestProtocolDroppedDebuggerResumeReplyStillDisablesAndJoins(t *testing.T) {
	var mu sync.Mutex
	methods := make(map[recordingWireKey]string)
	var resumes, disables, releases atomic.Int64
	request := func(data []byte) []byte {
		var message recordingWireMessage
		if json.Unmarshal(data, &message) == nil {
			mu.Lock()
			methods[recordingWireKey{message.ID, message.SessionID}] = message.Method
			mu.Unlock()
			switch message.Method {
			case debugger.Resume.Method:
				resumes.Add(1)
			case debugger.Disable.Method:
				disables.Add(1)
			case cdpruntime.ReleaseObjectGroup.Method:
				var params cdpruntime.ReleaseObjectGroupParams
				if json.Unmarshal(message.Params, &params) == nil && params.ObjectGroup == "backtrace" {
					releases.Add(1)
				}
			}
		}
		return data
	}
	reply := func(data []byte) []byte {
		var message recordingWireMessage
		if json.Unmarshal(data, &message) != nil || message.ID == 0 {
			return data
		}
		key := recordingWireKey{message.ID, message.SessionID}
		mu.Lock()
		method := methods[key]
		delete(methods, key)
		mu.Unlock()
		if method == debugger.Resume.Method {
			return nil
		}
		return data
	}
	endpoint, peer := cleanupFaultPeerTransform(t, nil, nil, reply, request)
	prepared, err := Prepare(Settings{Name: "dropped-resume", RemoteURL: endpoint, CleanupTimeout: new(100 * time.Millisecond)}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = owner.Close(bounded(t))
		if !owner.ShutdownComplete() {
			owner.state.stopNative(context.Canceled)
			t.Error("fixture cleanup incomplete")
		}
	})
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{Debugger: &debugger.EnableParams{}, SharedIsolateEffects: true})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := RunAction(bounded(t), retained.Page(), Evaluate[int](`debugger; 37`))
		done <- err
	}()
	_ = debuggerPause(t, control)
	_ = control.Close(bounded(t))
	if control.Closed() {
		t.Fatal("lost resume reply falsely completed")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-bounded(t).Done():
		t.Fatal("actual resume did not release paused evaluation")
	}
	if resumes.Load() != 1 || disables.Load() != 1 || releases.Load() != 1 {
		t.Fatal("lost resume reply blocked independent domain cleanup", resumes.Load(), disables.Load(), releases.Load())
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() || !control.Closed() {
		t.Fatal("owned destruction did not join cleanup workers")
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	value, _ := snapshot.ValueCopy()
	var records []struct {
		Debugger *struct{ ResumeCompleted, Disabled, BacktraceFreed, ResourcesGone bool } `json:"debugger"`
	}
	if err := json.Unmarshal(value.PayloadCopy(), &records); err != nil || len(records) == 0 || records[0].Debugger == nil {
		t.Fatal("metadata", err)
	}
	facts := records[0].Debugger
	if facts.ResumeCompleted || !facts.Disabled || !facts.BacktraceFreed || !facts.ResourcesGone {
		t.Fatal("resume acknowledgement and destruction were conflated")
	}
	if answer, err := sdk.Run(peer, sdk.Evaluate[int]("41")); err != nil || answer != 41 {
		t.Fatal("peer lost", err)
	}
}

func TestProtocolLateDebuggerDisableReplyRetainsBorrowedCustody(t *testing.T) {
	var mu sync.Mutex
	methods := make(map[recordingWireKey]string)
	held, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	finish := func() { releaseOnce.Do(func() { close(release) }) }
	var resumes, disables atomic.Int64
	request := func(data []byte) []byte {
		var message recordingWireMessage
		if json.Unmarshal(data, &message) == nil {
			mu.Lock()
			methods[recordingWireKey{message.ID, message.SessionID}] = message.Method
			mu.Unlock()
			if message.Method == debugger.Resume.Method {
				resumes.Add(1)
			}
			if message.Method == debugger.Disable.Method {
				disables.Add(1)
			}
		}
		return data
	}
	reply := func(data []byte) []byte {
		var message recordingWireMessage
		if json.Unmarshal(data, &message) != nil || message.ID == 0 {
			return data
		}
		key := recordingWireKey{message.ID, message.SessionID}
		mu.Lock()
		method := methods[key]
		delete(methods, key)
		mu.Unlock()
		if method == debugger.Disable.Method {
			once.Do(func() { close(held) })
			<-release
		}
		return data
	}
	endpoint, peer := cleanupFaultPeerTransform(t, nil, nil, reply, request)
	prepared, err := Prepare(Settings{Name: "late-disable", RemoteURL: endpoint, CleanupTimeout: new(100 * time.Millisecond)}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		finish()
		_ = owner.Close(bounded(t))
		if !owner.ShutdownComplete() {
			owner.state.stopNative(context.Canceled)
			t.Error("fixture cleanup incomplete")
		}
	})
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{Borrow: true, DefaultContext: true, TargetID: string(sdk.FromContext(peer).Target.TargetID)})
	if err != nil {
		t.Fatal(err)
	}
	control, _, err := retained.Page().Control(retained.Context(), ControlOptions{Debugger: &debugger.EnableParams{}, SharedIsolateEffects: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := control.Close(ctx); !errors.Is(err, context.DeadlineExceeded) || control.Closed() {
		t.Fatal("late disable reply was not retained", err)
	}
	select {
	case <-held:
	case <-bounded(t).Done():
		t.Fatal("disable was not issued")
	}
	if answer, err := sdk.Run(peer, sdk.Evaluate[int]("43")); err != nil || answer != 43 {
		t.Fatal("peer lost", err)
	}
	finish()
	_ = control.Close(bounded(t))
	if !control.Closed() || resumes.Load() != 1 || disables.Load() != 1 {
		t.Fatal("late acknowledged cleanup was replayed", resumes.Load(), disables.Load())
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("late cleanup retained source")
	}
}
