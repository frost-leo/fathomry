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

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/page"
	sdk "github.com/chromedp/chromedp"
)

type recordingState struct {
	stream     *Stream
	evidence   recordingEvidence
	stopDone   chan struct{}
	stopCancel context.CancelFunc
	stopResult page.StopScreenRecordingResult
	stopErr    error
}

type recordingEvidence struct {
	Start         page.StartScreenRecordingResult `json:"start"`
	Stop          page.StopScreenRecordingResult  `json:"stop"`
	StartQueued   bool                            `json:"startQueued"`
	StartReplied  bool                            `json:"startReplied"`
	StartRejected bool                            `json:"startRejected"`
	Started       bool                            `json:"started"`
	StopQueued    bool                            `json:"stopQueued"`
	StopReplied   bool                            `json:"stopReplied"`
	Stopped       bool                            `json:"stopped"`
	ResourcesGone bool                            `json:"resourcesGone"`
	StreamClosed  bool                            `json:"streamClosed"`
	EOF           bool                            `json:"eof"`
	Bytes         int64                           `json:"bytes"`
}

func (state *controlState) prepareRecording() error {
	const ledgerBytes = 8192
	if len(state.records) >= state.session.owner.settings.MaxEvents || state.session.owner.settings.MaxResultBytes-state.bytes < ledgerBytes {
		return failure(ErrLimit, "recording-evidence")
	}
	stream, err := state.session.reserveStream()
	if err != nil {
		return err
	}
	state.recording = &recordingState{stream: stream}
	state.records = append(state.records, &pendingState{control: state, method: page.StartScreenRecording.Method, recording: state.recording})
	state.bytes += ledgerBytes
	return nil
}

func (state *controlState) recordingWork(ctx context.Context, start bool) context.Context {
	return sdk.WithCallLimits(ctx, sdk.CallLimits{
		MaxInputBytes: state.session.owner.settings.MaxCommandBytes, MaxOutputBytes: 1024,
		Queued: func(string) {
			state.mu.Lock()
			defer state.mu.Unlock()
			if start {
				state.recording.evidence.StartQueued = true
			} else {
				state.recording.evidence.StopQueued = true
			}
		},
		Result: func(string, int64) error {
			state.mu.Lock()
			defer state.mu.Unlock()
			if start {
				state.recording.evidence.StartReplied = true
			} else {
				state.recording.evidence.StopReplied = true
			}
			return nil
		},
	})
}

func (state *controlState) startRecording(ctx context.Context, params page.StartScreenRecordingParams) error {
	recording := state.recording
	work, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(state.life, func() { cancel(context.Cause(state.life)) })
	defer stop()
	defer cancel(nil)
	if err := state.life.Err(); err != nil {
		return failure(ErrState, "recording-start", err, context.Cause(state.life))
	}
	buffer := boundedBuffer{limit: state.session.owner.settings.MaxCommandBytes}
	if err := marshalBounded(&buffer, params); err != nil {
		return err
	}
	var copied page.StartScreenRecordingParams
	if err := jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions); err != nil {
		return err
	}
	result, err := cdp.Call(state.recordingWork(work, true), sdk.FromContext(state.session.native).Target, page.StartScreenRecording, copied)
	state.mu.Lock()
	recording.evidence.Start = result
	var native *cdproto.Error
	recording.evidence.StartRejected = recording.evidence.StartReplied && errors.As(err, &native)
	if err == nil && result.Stream == "" {
		err = failure(ErrNative, "recording-stream-identity")
	}
	recording.evidence.Started = err == nil
	recording.stream.state.id = result.Stream
	state.mu.Unlock()
	return err
}

// StopRecording starts one native stop and waits for its flush reply. A caller
// timeout ends only that wait. Later calls join the same stop, never submit it
// again. No ordinary command or independent evidence reservation is needed.
func (control *Control) StopRecording(ctx context.Context) (page.StopScreenRecordingResult, error) {
	if control == nil || control.state == nil || ctx == nil {
		return page.StopScreenRecordingResult{}, failure(ErrInput, "recording-stop")
	}
	state := control.state
	work, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(state.life, func() { cancel(context.Cause(state.life)) })
	defer stop()
	defer cancel(nil)
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-work.Done():
		return page.StopScreenRecordingResult{}, failure(ErrState, "recording-stop-wait", work.Err(), context.Cause(work))
	}
	state.mu.Lock()
	invalid := state.closing || state.closed || state.recording == nil || !state.recording.evidence.StartQueued || state.recording.evidence.StartRejected
	state.mu.Unlock()
	if invalid {
		return page.StopScreenRecordingResult{}, failure(ErrState, "recording-stop")
	}
	result, err := state.stopRecording(work)
	if err != nil {
		state.fail(err)
	}
	return result, err
}

// The control gate serializes stop setup and consumption. The one native call
// outlives individual waits, but resource destruction cancels and joins it.
func (state *controlState) stopRecording(ctx context.Context) (page.StopScreenRecordingResult, error) {
	if err := ctx.Err(); err != nil {
		return page.StopScreenRecordingResult{}, failure(ErrState, "recording-stop", err, context.Cause(ctx))
	}
	recording := state.recording
	state.beginRecordingStop()
	select {
	case <-recording.stopDone:
	case <-ctx.Done():
		return page.StopScreenRecordingResult{}, failure(ErrState, "recording-stop-wait", ctx.Err(), context.Cause(ctx))
	}
	state.mu.Lock()
	result, err, original := recording.stopResult, recording.stopErr, recording.evidence.Start.Stream
	recording.evidence.Stop = result
	state.mu.Unlock()
	if err != nil {
		return result, failure(ErrNative, "recording-stop", err)
	}
	if result.Stream == "" || original != "" && result.Stream != original {
		return result, failure(ErrNative, "recording-stop-identity")
	}
	stream := recording.stream.state
	select {
	case stream.gate <- struct{}{}:
		stream.id = result.Stream
		<-stream.gate
	case <-ctx.Done():
		return result, failure(ErrState, "recording-stream-join", ctx.Err(), context.Cause(ctx))
	}
	state.mu.Lock()
	recording.evidence.Stopped = true
	state.mu.Unlock()
	return result, errors.Join(ctx.Err(), context.Cause(ctx))
}

func (state *controlState) beginRecordingStop() {
	recording := state.recording
	if recording == nil || recording.stopDone != nil {
		return
	}
	state.mu.Lock()
	unused := !recording.evidence.StartQueued || recording.evidence.StartRejected
	state.mu.Unlock()
	if unused {
		return
	}
	recording.stopDone = make(chan struct{})
	work, cancel := context.WithCancel(context.WithoutCancel(state.session.work))
	recording.stopCancel = cancel
	go func() {
		defer close(recording.stopDone)
		defer cancel()
		result, err := cdp.Call(state.recordingWork(work, false), sdk.FromContext(state.session.native).Target, page.StopScreenRecording, cdp.Empty{})
		state.mu.Lock()
		recording.stopResult, recording.stopErr = result, err
		state.mu.Unlock()
	}()
}

// ReadRecording uses the existing stream allowance, but only after confirmed
// producer completion. File-backed native IO can report EOF before a recorder
// finishes writing. Such an intermediate EOF is not completion evidence.
func (control *Control) ReadRecording(ctx context.Context, size int) (Chunk, error) {
	return control.readRecording(ctx, size, nil)
}

// ReadRecordingAt seeks within the completed native recording. Repeated bytes
// count against the same cumulative stream allowance.
func (control *Control) ReadRecordingAt(ctx context.Context, size int, offset int64) (Chunk, error) {
	return control.readRecording(ctx, size, &offset)
}

func (control *Control) readRecording(ctx context.Context, size int, offset *int64) (Chunk, error) {
	if control == nil || control.state == nil {
		return Chunk{}, failure(ErrInput, "recording-read")
	}
	state := control.state
	work, finish, err := state.enterCommand(ctx, func() error {
		if state.recording == nil || !state.recording.evidence.Stopped {
			return failure(ErrState, "recording-not-stopped")
		}
		return nil
	})
	if err != nil {
		return Chunk{}, err
	}
	defer finish()
	if offset != nil {
		return state.recording.stream.ReadAt(work, size, *offset)
	}
	return state.recording.stream.Read(work, size)
}

func (state *controlState) closeRecording(ctx context.Context) (bool, error) {
	recording := state.recording
	if recording == nil {
		return true, nil
	}
	if state.session.nativeResourcesGone() {
		if recording.stopCancel != nil {
			recording.stopCancel()
			select {
			case <-recording.stopDone:
			case <-ctx.Done():
				return false, failure(ErrCleanup, "recording-stop-join", ctx.Err(), context.Cause(ctx))
			}
		}
		state.mu.Lock()
		recording.evidence.ResourcesGone = true
		state.mu.Unlock()
		err := recording.stream.Close(ctx)
		return recording.stream.Closed(), err
	}
	state.mu.Lock()
	unused := !recording.evidence.StartQueued || recording.evidence.StartRejected
	state.mu.Unlock()
	if unused {
		recording.stream.state.cancel()
		recording.stream.state.finish()
		return true, nil
	}
	if _, err := state.stopRecording(ctx); err != nil {
		return false, failure(ErrCleanup, "recording-stop", err)
	}
	err := recording.stream.Close(ctx)
	return recording.stream.Closed(), err
}
