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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/fetch"
	cdpio "github.com/chromedp/cdproto/io"
	"github.com/chromedp/cdproto/page"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/adapters/v1"
)

type recordingFaults struct {
	failBodyClose                                      bool
	dropStart, holdStop, dropStop, wrongStop, holdRead bool
	failClose                                          int64
}

type recordingWireMessage struct {
	ID        int64           `json:"id"`
	SessionID string          `json:"sessionId,omitempty"`
	Method    string          `json:"method,omitempty"`
	Params    json.RawMessage `json:"params,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     *cdproto.Error  `json:"error,omitempty"`
}

type recordingWireKey struct {
	id      int64
	session string
}

// This local protocol surrogate uses a real native PDF IO file, not a video
// encoder. It qualifies custody/order/fault handling only. The separate native
// availability test qualifies or explicitly reports the actual recording peer.
type recordingWireFixture struct {
	bodyCloseFault                   atomic.Bool
	mu                               sync.Mutex
	faults                           recordingFaults
	requests                         map[recordingWireKey]string
	streams                          map[string]cdpio.StreamHandle
	starts, stops, reads             int
	closedHandles                    []cdpio.StreamHandle
	startParams                      page.StartScreenRecordingParams
	startReply, stopReply, readReply chan struct{}
	startOnce, stopOnce, readOnce    sync.Once
	stopRelease, readRelease         chan struct{}
	stopReleaseOnce, readReleaseOnce sync.Once
}

func (fixture *recordingWireFixture) release() {
	fixture.bodyCloseFault.Store(false)
	fixture.stopReleaseOnce.Do(func() { close(fixture.stopRelease) })
	fixture.readReleaseOnce.Do(func() { close(fixture.readRelease) })
}

func (fixture *recordingWireFixture) request(data []byte) []byte {
	var message recordingWireMessage
	if json.Unmarshal(data, &message) != nil {
		return data
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	key := recordingWireKey{message.ID, message.SessionID}
	switch message.Method {
	case page.StartScreenRecording.Method:
		fixture.starts++
		_ = json.Unmarshal(message.Params, &fixture.startParams)
		fixture.requests[key] = message.Method
		message.Method = page.PrintToPDF.Method
		message.Params = json.RawMessage(`{"transferMode":"ReturnAsStream"}`)
	case page.StopScreenRecording.Method:
		fixture.stops++
		fixture.requests[key] = message.Method
		message.Method, message.Params = page.GetLayoutMetrics.Method, json.RawMessage(`{}`)
	case cdpio.Read.Method:
		fixture.reads++
		fixture.requests[key] = message.Method
		return data
	case cdpio.Close.Method:
		var params cdpio.CloseParams
		_ = json.Unmarshal(message.Params, &params)
		fixture.closedHandles = append(fixture.closedHandles, params.Handle)
		return data
	default:
		return data
	}
	changed, err := json.Marshal(message)
	if err != nil {
		panic(err)
	}
	return changed
}

func (fixture *recordingWireFixture) reply(data []byte) []byte {
	var message recordingWireMessage
	if json.Unmarshal(data, &message) != nil || message.ID == 0 {
		return data
	}
	key := recordingWireKey{message.ID, message.SessionID}
	fixture.mu.Lock()
	method := fixture.requests[key]
	delete(fixture.requests, key)
	if method == page.StartScreenRecording.Method {
		var result page.PrintToPDFResult
		if json.Unmarshal(message.Result, &result) == nil && result.Stream != "" {
			fixture.streams[message.SessionID] = result.Stream
		}
	}
	stream := fixture.streams[message.SessionID]
	fixture.mu.Unlock()
	switch method {
	case page.StartScreenRecording.Method:
		fixture.startOnce.Do(func() { close(fixture.startReply) })
		if fixture.faults.dropStart {
			return nil
		}
		message.Result, _ = json.Marshal(page.StartScreenRecordingResult{Stream: stream})
	case page.StopScreenRecording.Method:
		fixture.stopOnce.Do(func() { close(fixture.stopReply) })
		if fixture.faults.holdStop {
			<-fixture.stopRelease
		}
		if fixture.faults.dropStop {
			return nil
		}
		if fixture.faults.wrongStop {
			stream = "foreign-recording-fixture"
		}
		message.Result, _ = json.Marshal(page.StopScreenRecordingResult{Stream: stream})
	case cdpio.Read.Method:
		fixture.readOnce.Do(func() { close(fixture.readReply) })
		if fixture.faults.holdRead {
			<-fixture.readRelease
		}
		return data
	default:
		return data
	}
	changed, err := json.Marshal(message)
	if err != nil {
		panic(err)
	}
	return changed
}

func recordingProtocolFixture(t *testing.T, faults recordingFaults, borrowed bool) (*Owner, *Context, context.Context, Dependencies, *recordingWireFixture) {
	t.Helper()
	fixture := &recordingWireFixture{faults: faults, requests: make(map[recordingWireKey]string), streams: make(map[string]cdpio.StreamHandle), startReply: make(chan struct{}), stopReply: make(chan struct{}), readReply: make(chan struct{}), stopRelease: make(chan struct{}), readRelease: make(chan struct{})}
	streams, closeFailures := 1, faults.failClose
	var match func(string, json.RawMessage) bool
	if faults.failBodyClose {
		streams, closeFailures = 2, -1
		fixture.bodyCloseFault.Store(true)
		match = func(_ string, data json.RawMessage) bool {
			if !fixture.bodyCloseFault.Load() {
				return false
			}
			var params cdpio.CloseParams
			if json.Unmarshal(data, &params) != nil {
				return false
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			for _, stream := range fixture.streams {
				if params.Handle == stream {
					return false
				}
			}
			return true
		}
	}
	endpoint, peer := cleanupFaultPeerTransform(t, map[string]int64{cdpio.Close.Method: closeFailures}, match, fixture.reply, fixture.request)
	prepared, err := Prepare(Settings{Name: "recording-protocol", RemoteURL: endpoint, MaxCommands: new(1), MaxStreams: new(streams), CleanupTimeout: new(300 * time.Millisecond)}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fixture.release(); _ = owner.Close(bounded(t)) })
	options := ContextOptions{}
	if borrowed {
		options = ContextOptions{Borrow: true, DefaultContext: true, TargetID: string(sdk.FromContext(peer).Target.TargetID)}
	}
	retained, _, err := owner.Client().Retain(bounded(t), options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retained.Page().Navigate(bounded(t), "data:text/html,<h1>recording protocol fixture</h1>"); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	return owner, retained, peer, dependencies, fixture
}

type recordingTestEvidence struct {
	Start         page.StartScreenRecordingResult `json:"start"`
	Stop          page.StopScreenRecordingResult  `json:"stop"`
	StartQueued   bool                            `json:"startQueued"`
	StartReplied  bool                            `json:"startReplied"`
	StartRejected bool                            `json:"startRejected"`
	Started       bool                            `json:"started"`
	Stopped       bool                            `json:"stopped"`
	ResourcesGone bool                            `json:"resourcesGone"`
	StreamClosed  bool                            `json:"streamClosed"`
	EOF           bool                            `json:"eof"`
	Bytes         int64                           `json:"bytes"`
}

func recordingEvidenceFrom(t *testing.T, receipt *adapters.Receipt[Result]) recordingTestEvidence {
	t.Helper()
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	value, _ := snapshot.ValueCopy()
	var records []struct {
		Recording *recordingTestEvidence `json:"recording"`
	}
	if err := json.Unmarshal(value.PayloadCopy(), &records); err != nil || len(records) != 1 || records[0].Recording == nil {
		t.Fatal("recording evidence missing or recording-only Control consumed unrelated events", err)
	}
	return *records[0].Recording
}

func TestLiveRecordingNativeAvailabilityAndRejectedCleanup(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<h1>native recorder</h1>", 8)
	control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{Recording: &page.StartScreenRecordingParams{Audio: new(false), MaxWidth: 128, MaxHeight: 96, FrameRate: 10}})
	if control == nil {
		t.Fatal("native recorder rejection lost its admitted cleanup/evidence", err)
	}
	var native *cdproto.Error
	unsupported := errors.As(err, &native) && native.Code == -32601
	if err != nil && !unsupported {
		t.Fatal("recording failed for a reason other than the qualified native absence", err)
	}
	if unsupported {
		t.Log("Native recording is unavailable on this browser (-32601); this checks managed refusal and cleanup, not encoded video.")
	} else if result, err := control.StopRecording(bounded(t)); err != nil || result.Stream == "" {
		t.Fatal("native recording did not return its completed stream", err)
	}
	_ = control.Close(bounded(t))
	if !control.Closed() {
		t.Fatal("native recorder left unresolved custody")
	}
	evidence := recordingEvidenceFrom(t, receipt)
	if unsupported && (!evidence.StartRejected || evidence.Started || evidence.Stopped) || !evidence.StreamClosed {
		t.Fatal("recording rejection and successful producer completion were conflated")
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("native recorder prevented owned shutdown")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestProtocolRecordingStopBeforeReadAndReservedCapacity(t *testing.T) {
	owner, retained, _, dependencies, fixture := recordingProtocolFixture(t, recordingFaults{}, false)
	options := page.StartScreenRecordingParams{Audio: new(false), MaxWidth: 100, MaxHeight: 80, FrameRate: 12}
	control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{Recording: &options})
	if err != nil {
		t.Fatal(err)
	}
	*options.Audio, options.MaxWidth = true, 1
	if _, _, err := control.ReadRecording(bounded(t), 1024); !errors.Is(err, ErrState) {
		t.Fatal("unfinished recording exposed native file EOF", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), retained.Page(), Title()); !errors.Is(err, ErrLimit) {
		t.Fatal("ordinary command allowance was not exhausted", err)
	}
	drain(t, dependencies, 1)
	result, err := control.StopRecording(bounded(t))
	if err != nil || result.Stream == "" {
		t.Fatal("recording stop required ordinary command admission", err)
	}
	if again, err := control.StopRecording(bounded(t)); err != nil || again != result {
		t.Fatal("repeated stop changed completion", err)
	}
	var body []byte
	ended := false
	for range 100 {
		chunk, chunkReceipt, err := control.ReadRecording(bounded(t), 1024)
		if err != nil {
			t.Fatal(err)
		}
		body = append(body, chunk.DataCopy()...)
		snapshot, err := chunkReceipt.WaitReleased(bounded(t))
		if err != nil {
			t.Fatal(err)
		}
		value, _ := snapshot.ValueCopy()
		if value.Observation().EOF != chunk.EOF() {
			t.Fatal("recording chunk EOF evidence differs")
		}
		drain(t, dependencies, 1)
		if chunk.EOF() {
			ended = true
			break
		}
	}
	if !ended || !bytes.HasPrefix(body, []byte("%PDF-")) {
		t.Fatal("protocol surrogate did not return its native IO bytes")
	}
	chunk, _, err := control.ReadRecordingAt(bounded(t), 4, 0)
	if err != nil || string(chunk.DataCopy()) != "%PDF" || chunk.EOF() {
		t.Fatal("recording seek after EOF changed native file semantics", err)
	}
	drain(t, dependencies, 1)
	chunk, _, err = control.ReadRecordingAt(bounded(t), 1, int64(len(body)))
	if err != nil || !chunk.EOF() || len(chunk.DataCopy()) != 0 {
		t.Fatal("recording seek did not preserve positive EOF", err)
	}
	drain(t, dependencies, 1)
	fillControlInbox(t, retained, dependencies)
	if again, err := control.StopRecording(bounded(t)); err != nil || again != result {
		t.Fatal("recording stop required a new evidence reservation", err)
	}
	_ = control.Close(bounded(t))
	if !control.Closed() {
		t.Fatal("recording cleanup needed new admission")
	}
	evidence := recordingEvidenceFrom(t, receipt)
	if !evidence.Started || !evidence.Stopped || !evidence.StreamClosed || !evidence.EOF || evidence.Bytes != int64(len(body))+4 || evidence.Start.Stream != result.Stream || evidence.Stop != result {
		t.Fatal("recording phases and byte/EOF facts differ")
	}
	fixture.mu.Lock()
	starts, stops, params := fixture.starts, fixture.stops, fixture.startParams
	fixture.mu.Unlock()
	if starts != 1 || stops != 1 || params.Audio == nil || *params.Audio || params.MaxWidth != 100 || params.FrameRate != 12 {
		t.Fatal("recording was repeated or native option presence changed")
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("recording survived owner shutdown")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestProtocolRecordingLateStopReplyKeepsBorrowedPeer(t *testing.T) {
	owner, retained, peer, dependencies, fixture := recordingProtocolFixture(t, recordingFaults{holdStop: true}, true)
	control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{Recording: &page.StartScreenRecordingParams{}})
	if err != nil {
		t.Fatal(err)
	}
	work, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := control.StopRecording(work); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("delayed native stop did not time out its caller wait", err)
	}
	select {
	case <-fixture.stopReply:
	default:
		t.Fatal("stop was not actually pending")
	}
	closing, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stop()
	_ = owner.Close(closing)
	if owner.ShutdownComplete() || control.Closed() {
		t.Fatal("unfinished native stop falsely settled borrowed resources")
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int]("37")); err != nil || value != 37 {
		t.Fatal("unfinished recording altered the external peer", err)
	}
	fixture.release()
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() || !control.Closed() {
		t.Fatal("late original stop reply was discarded")
	}
	evidence := recordingEvidenceFrom(t, receipt)
	if !evidence.Stopped || !evidence.StreamClosed || evidence.ResourcesGone {
		t.Fatal("borrowed IO cleanup was replaced by inferred target destruction")
	}
	fixture.mu.Lock()
	stops := fixture.stops
	fixture.mu.Unlock()
	if stops != 1 {
		t.Fatal("timed-out stop was resubmitted", stops)
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int]("41")); err != nil || value != 41 {
		t.Fatal("recording cleanup killed the peer", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestProtocolRecordingLostStartRecoversOnlyFromSameStop(t *testing.T) {
	owner, retained, _, dependencies, fixture := recordingProtocolFixture(t, recordingFaults{dropStart: true}, false)
	ctx, cancel := context.WithCancel(retained.Context())
	t.Cleanup(cancel)
	type opened struct {
		control *Control
		receipt *adapters.Receipt[Result]
		err     error
	}
	done := make(chan opened, 1)
	go func() {
		control, receipt, err := retained.Page().Control(ctx, ControlOptions{Recording: &page.StartScreenRecordingParams{}})
		done <- opened{control, receipt, err}
	}()
	select {
	case <-fixture.startReply:
	case <-bounded(t).Done():
		t.Fatal("start did not create a real native IO handle")
	}
	cancel()
	value := <-done
	if value.control == nil || !errors.Is(value.err, context.Canceled) {
		t.Fatal("lost start discarded producer custody", value.err)
	}
	_ = value.control.Close(bounded(t))
	if !value.control.Closed() {
		t.Fatal("same-session stop did not recover unknown start identity")
	}
	evidence := recordingEvidenceFrom(t, value.receipt)
	if evidence.StartReplied || evidence.StartRejected || evidence.Started || !evidence.Stopped || evidence.Stop.Stream == "" || !evidence.StreamClosed {
		t.Fatal("lost start was reported as known success or rejection")
	}
	_ = owner.Close(bounded(t))
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestProtocolRecordingUnknownStopNeedsOwnedDestruction(t *testing.T) {
	for _, wrong := range []bool{false, true} {
		t.Run(map[bool]string{false: "lost", true: "foreign"}[wrong], func(t *testing.T) {
			owner, retained, peer, dependencies, fixture := recordingProtocolFixture(t, recordingFaults{dropStop: !wrong, wrongStop: wrong}, false)
			control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{Recording: &page.StartScreenRecordingParams{}})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { _, err := control.StopRecording(context.Background()); done <- err }()
			select {
			case <-fixture.stopReply:
			case <-bounded(t).Done():
				t.Fatal("stop was not dispatched")
			}
			_ = owner.Close(bounded(t))
			if !owner.ShutdownComplete() || !control.Closed() {
				t.Fatal("unknown recording blocked destruction of its owned page")
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("unknown stop reported success")
				}
			case <-bounded(t).Done():
				t.Fatal("stop caller or native worker did not join")
			}
			evidence := recordingEvidenceFrom(t, receipt)
			if evidence.Stopped || !evidence.ResourcesGone || !evidence.StreamClosed {
				t.Fatal("resource destruction fabricated stop acknowledgement")
			}
			fixture.mu.Lock()
			stops := fixture.stops
			for _, id := range fixture.closedHandles {
				if id == "foreign-recording-fixture" {
					t.Error("foreign stop handle acquired IO cleanup authority")
				}
			}
			fixture.mu.Unlock()
			if stops != 1 {
				t.Fatal("unknown stop was replayed", stops)
			}
			if value, err := sdk.Run(peer, sdk.Evaluate[int]("43")); err != nil || value != 43 {
				t.Fatal("owned-page recording cleanup closed the browser peer", err)
			}
			status, _ := dependencies.Evidence.Inspect()
			drain(t, dependencies, status.Outstanding)
		})
	}
}

func TestProtocolRecordingCloseRetryKeepsIndependentHistory(t *testing.T) {
	owner, retained, _, dependencies, fixture := recordingProtocolFixture(t, recordingFaults{failClose: 1}, false)
	control, receipt, err := retained.Page().Control(retained.Context(), ControlOptions{Recording: &page.StartScreenRecordingParams{}})
	if err != nil {
		t.Fatal(err)
	}
	first := control.Close(bounded(t))
	var cause *cdproto.Error
	if !errors.As(first, &cause) || control.Closed() {
		t.Fatal("first native IO cleanup failure was not retained", first)
	}
	if err := control.Close(bounded(t)); !errors.Is(err, cause) || !control.Closed() {
		t.Fatal("successful retry lost history or remained incomplete", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || !errors.Is(snapshot.Cleanup(), cause) {
		t.Fatal("independent recording cleanup history missing", err)
	}
	fixture.mu.Lock()
	stops := fixture.stops
	fixture.mu.Unlock()
	if stops != 1 {
		t.Fatal("IO cleanup retry restarted stop", stops)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("historical cleanup failure prevented confirmed parent completion")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestProtocolRecordingCloseCancelsAndJoinsActiveRead(t *testing.T) {
	owner, retained, _, dependencies, fixture := recordingProtocolFixture(t, recordingFaults{holdRead: true}, true)
	control, _, err := retained.Page().Control(retained.Context(), ControlOptions{Recording: &page.StartScreenRecordingParams{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := control.StopRecording(bounded(t)); err != nil {
		t.Fatal(err)
	}
	read := make(chan error, 1)
	go func() { _, _, err := control.ReadRecording(context.Background(), 1024); read <- err }()
	select {
	case <-fixture.readReply:
	case <-bounded(t).Done():
		t.Fatal("IO read did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- control.Close(bounded(t)) }()
	select {
	case err := <-read:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("control cancellation did not end its active IO read", err)
		}
	case <-bounded(t).Done():
		t.Fatal("control close waited for a reader without canceling it")
	}
	fixture.release()
	if err := <-closed; err != nil || !control.Closed() {
		t.Fatal("recording reader did not join cleanup", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("recording reader retained source custody")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestProtocolRecordingStopsDespiteIndependentBodyCleanupFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		_, _ = w.Write([]byte("recording body fixture"))
	}))
	defer server.Close()
	owner, retained, peer, dependencies, fixture := recordingProtocolFixture(t, recordingFaults{failBodyClose: true}, true)
	if err := sdk.Do(peer, sdk.Navigate(server.URL)); err != nil {
		t.Fatal(err)
	}
	control, _, err := retained.Page().Control(retained.Context(), ControlOptions{
		Recording: &page.StartScreenRecordingParams{},
		Fetch:     &fetch.EnableParams{Patterns: []*fetch.RequestPattern{{URLPattern: server.URL + "/*", RequestStage: fetch.RequestStageResponse}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	address, _ := json.Marshal(server.URL + "/body")
	if _, err := sdk.Run(peer, sdk.Evaluate[bool]("fetch("+string(address)+").catch(()=>null);true")); err != nil {
		t.Fatal(err)
	}
	pending, err := control.Next(bounded(t))
	if err != nil || pending.Method() != fetch.RequestPaused.Method {
		t.Fatal("response fixture did not pause", err)
	}
	if _, _, err := pending.ResponseBodyStream(bounded(t)); err != nil {
		var cause *cdproto.Error
		if errors.As(err, &cause) {
			t.Logf("synthetic response stream refused: code=%d message=%s", cause.Code, cause.Message)
		}
		t.Fatal(err)
	}
	if err := control.Close(bounded(t)); err == nil || control.Closed() {
		t.Fatal("fixture body cleanup failure did not retain custody", err)
	}
	fixture.mu.Lock()
	stops, closed := fixture.stops, len(fixture.closedHandles)
	fixture.mu.Unlock()
	if stops != 1 || closed != 1 {
		t.Fatal("independent body cleanup failure prevented recording stop/IO cleanup", stops, closed)
	}
	fixture.bodyCloseFault.Store(false)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() || !control.Closed() {
		t.Fatal("settled combined control remained incomplete")
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int]("47")); err != nil || value != 47 {
		t.Fatal("mixed-control cleanup altered the borrowed peer", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
