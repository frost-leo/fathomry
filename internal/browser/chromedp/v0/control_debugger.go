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
	"reflect"
	"sync"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/debugger"
	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
)

type debuggerState struct {
	evidence       debuggerEvidence
	pause          uint64
	resumeReady    chan struct{}
	resumeDone     chan struct{}
	resumeCancel   context.CancelFunc
	resumeErr      error
	cleanupDone    chan struct{}
	cleanupCancel  context.CancelFunc
	cleanupErr     error
	cleanupHistory error
	scripts        map[cdp.ScriptID]*debuggerScript
	scriptEvents   map[uint64]*debuggerScript
	scriptEpoch    uint64
	scriptCount    int
	scriptBytes    int64
}

type debuggerEvidence struct {
	Enable          debugger.EnableResult `json:"enable"`
	EnableQueued    bool                  `json:"enableQueued"`
	EnableRejected  bool                  `json:"enableRejected"`
	Enabled         bool                  `json:"enabled"`
	ResumeQueued    bool                  `json:"resumeQueued"`
	ResumeReplied   bool                  `json:"resumeReplied"`
	ResumeCompleted bool                  `json:"resumeCompleted"`
	Disabled        bool                  `json:"disabled"`
	BacktraceFreed  bool                  `json:"backtraceFreed"`
	ResourcesGone   bool                  `json:"resourcesGone"`
}

func (state *controlState) prepareDebugger() error {
	const ledgerBytes = 4096
	if len(state.records) >= state.session.owner.settings.MaxEvents || state.session.owner.settings.MaxResultBytes-state.bytes < ledgerBytes {
		return failure(ErrLimit, "debugger-evidence")
	}
	state.debugger = &debuggerState{scripts: make(map[cdp.ScriptID]*debuggerScript), scriptEvents: make(map[uint64]*debuggerScript)}
	state.records = append(state.records, &pendingState{control: state, method: debugger.Enable.Method, debugger: state.debugger})
	state.bytes += ledgerBytes
	return nil
}

func (state *controlState) enableDebugger(ctx context.Context, params debugger.EnableParams) error {
	work, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(state.life, func() { cancel(context.Cause(state.life)) })
	defer stop()
	defer cancel(nil)
	buffer := boundedBuffer{limit: state.session.owner.settings.MaxCommandBytes}
	if err := marshalBounded(&buffer, params); err != nil {
		return err
	}
	var copied debugger.EnableParams
	if err := jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions); err != nil {
		return err
	}
	limits := sdk.CallLimits{MaxInputBytes: state.session.owner.settings.MaxCommandBytes, MaxOutputBytes: 1024,
		Before: func(string) error { return state.life.Err() },
		Queued: func(string) {
			state.mu.Lock()
			state.debugger.evidence.EnableQueued = true
			state.mu.Unlock()
		},
	}
	result, err := cdp.Call(sdk.WithCallLimits(work, limits), sdk.FromContext(state.session.native).Target, debugger.Enable, copied)
	state.mu.Lock()
	defer state.mu.Unlock()
	state.debugger.evidence.Enable = result
	var native *cdproto.Error
	state.debugger.evidence.EnableRejected = errors.As(err, &native)
	state.debugger.evidence.Enabled = err == nil
	return err
}

// Called under the control lock before delivery. Call-frame IDs are not pause
// generations; a resumed event must retire authority even while the reader waits.
func (state *controlState) observeDebugger(method string, data jsonv2.Value, sequence uint64) error {
	if state.debugger == nil {
		return nil
	}
	switch method {
	case debugger.Paused.Method:
		var event debugger.EventPaused
		if err := jsonv2.Unmarshal(data, &event, sdk.DefaultUnmarshalOptions); err != nil {
			return err
		}
		if len(event.CallFrames) > state.session.owner.settings.MaxEvents {
			return failure(ErrLimit, "debugger-call-frames")
		}
		for _, frame := range event.CallFrames {
			if frame == nil || frame.CallFrameID == "" {
				return failure(ErrNative, "debugger-frame-identity")
			}
		}
		state.debugger.pause = sequence
	case debugger.Resumed.Method:
		var event debugger.EventResumed
		if err := jsonv2.Unmarshal(data, &event, sdk.DefaultUnmarshalOptions); err != nil {
			return err
		}
		state.debugger.pause = 0
	case debugger.ScriptParsed.Method, debugger.ScriptFailedToParse.Method:
		return state.observeScript(data, sequence)
	case runtime.ExecutionContextsCleared.Method:
		state.debugger.scriptEpoch++
		state.debugger.pause = 0
		clear(state.debugger.scripts)
	}
	return nil
}

// ResumeDebugger uses the observed pause's reserved single-use reply lane.
// Native resume affects the current pause, not an atomic expected-pause ID.
func ResumeDebugger(params debugger.ResumeParams) Reply {
	return Reply{method: debugger.Resume.Method, params: func(pending *pendingState) (any, error) {
		if pending.method != debugger.Paused.Method {
			return nil, failure(ErrInput, "debugger-pause-grant")
		}
		return params, nil
	}}
}

// StepOutDebugger resumes until the current function returns. Any later pause
// has a new grant, even when native call-frame IDs are reused.
func StepOutDebugger() Reply {
	return Reply{method: debugger.StepOut.Method, params: func(pending *pendingState) (any, error) {
		if pending.method != debugger.Paused.Method {
			return nil, failure(ErrInput, "debugger-pause-grant")
		}
		return cdp.Empty{}, nil
	}}
}

// The control gate owns this one-shot cleanup attempt. Its wait can time out,
// but neither a late response nor an unknown effect authorizes resubmission.
func (state *controlState) beginDebuggerResume() {
	domain := state.debugger
	if domain == nil || domain.resumeDone != nil {
		return
	}
	state.mu.Lock()
	unused := !domain.evidence.EnableQueued || domain.evidence.EnableRejected
	state.mu.Unlock()
	if unused {
		return
	}
	domain.resumeReady, domain.resumeDone = make(chan struct{}), make(chan struct{})
	work, cancel := context.WithCancel(context.WithoutCancel(state.session.work))
	domain.resumeCancel = cancel
	go func() {
		defer close(domain.resumeDone)
		defer cancel()
		var ready sync.Once
		defer ready.Do(func() { close(domain.resumeReady) })
		limits := sdk.CallLimits{MaxInputBytes: state.session.owner.settings.MaxCommandBytes, MaxOutputBytes: 1024,
			Queued: func(string) {
				state.mu.Lock()
				domain.evidence.ResumeQueued = true
				state.mu.Unlock()
				ready.Do(func() { close(domain.resumeReady) })
			},
			Result: func(string, int64) error {
				state.mu.Lock()
				domain.evidence.ResumeReplied = true
				state.mu.Unlock()
				return nil
			},
		}
		_, err := cdp.Call(sdk.WithCallLimits(work, limits), sdk.FromContext(state.session.native).Target, debugger.Resume, debugger.ResumeParams{})
		var native *cdproto.Error
		if errors.As(err, &native) && native.Code == -32000 && native.Message == "Can only perform operation while paused." {
			err = nil
		}
		state.mu.Lock()
		domain.resumeErr, domain.evidence.ResumeCompleted = err, err == nil
		state.mu.Unlock()
	}()
}

func (state *controlState) closeDebugger(ctx context.Context) (bool, error) {
	domain := state.debugger
	if domain == nil {
		return true, nil
	}
	if state.session.nativeResourcesGone() {
		state.mu.Lock()
		domain.evidence.ResourcesGone = true
		domain.pause = 0
		state.mu.Unlock()
		for _, worker := range []struct {
			cancel context.CancelFunc
			done   <-chan struct{}
		}{{domain.resumeCancel, domain.resumeDone}, {domain.cleanupCancel, domain.cleanupDone}} {
			if worker.cancel == nil {
				continue
			}
			worker.cancel()
			select {
			case <-worker.done:
			case <-ctx.Done():
				return false, failure(ErrCleanup, "debugger-cleanup-join", ctx.Err(), context.Cause(ctx))
			}
		}
		state.mu.Lock()
		history := domain.cleanupHistory
		state.mu.Unlock()
		return true, history
	}
	state.mu.Lock()
	unused := !domain.evidence.EnableQueued || domain.evidence.EnableRejected
	state.mu.Unlock()
	if unused {
		return true, nil
	}
	state.beginDebuggerCleanup()
	for _, done := range []<-chan struct{}{domain.cleanupDone, domain.resumeDone} {
		select {
		case <-done:
		case <-ctx.Done():
			return false, failure(ErrCleanup, "debugger-cleanup-join", ctx.Err(), context.Cause(ctx))
		}
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	complete := domain.evidence.Disabled && domain.evidence.BacktraceFreed && domain.evidence.ResumeCompleted
	return complete, errors.Join(domain.cleanupHistory, domain.cleanupErr, domain.resumeErr)
}

// Domain cleanup must not wait behind another producer's lost response. Its
// owned worker retains late acknowledgments across bounded Close waits.
func (state *controlState) beginDebuggerCleanup() {
	domain := state.debugger
	if domain == nil {
		return
	}
	state.mu.Lock()
	unused := !domain.evidence.EnableQueued || domain.evidence.EnableRejected
	complete := domain.evidence.Disabled && domain.evidence.BacktraceFreed
	state.mu.Unlock()
	if unused || complete {
		return
	}
	if domain.cleanupDone != nil {
		select {
		case <-domain.cleanupDone:
		default:
			return
		}
	}
	state.beginDebuggerResume()
	work, cancel := context.WithCancel(context.WithoutCancel(state.session.work))
	done := make(chan struct{})
	domain.cleanupDone, domain.cleanupCancel = done, cancel
	go func() {
		defer close(done)
		defer cancel()
		select {
		case <-domain.resumeReady:
		case <-work.Done():
			return
		}
		err := state.disableDebugger(work)
		state.mu.Lock()
		if !domain.evidence.ResourcesGone {
			domain.cleanupErr = err
			if err != nil && domain.cleanupHistory == nil {
				domain.cleanupHistory = err
			}
		}
		state.mu.Unlock()
	}()
}

func (state *controlState) disableDebugger(ctx context.Context) error {
	domain := state.debugger
	// Resume is queued first. Disable and group release must still progress when
	// its reply is lost; a disabled domain alone does not prove the page resumed.
	target := sdk.FromContext(state.session.native).Target
	state.mu.Lock()
	disabled, freed := domain.evidence.Disabled, domain.evidence.BacktraceFreed
	state.mu.Unlock()
	var cleanup error
	if !disabled {
		_, err := cdp.Call(state.work(ctx), target, debugger.Disable, cdp.Empty{})
		cleanup = errors.Join(cleanup, err)
		state.mu.Lock()
		disabled, domain.evidence.Disabled = err == nil, err == nil
		if disabled {
			domain.pause = 0
		}
		state.mu.Unlock()
	}
	if disabled && !freed {
		_, err := cdp.Call(state.work(ctx), target, runtime.ReleaseObjectGroup, runtime.ReleaseObjectGroupParams{ObjectGroup: "backtrace"})
		cleanup = errors.Join(cleanup, err)
		state.mu.Lock()
		freed, domain.evidence.BacktraceFreed = err == nil, err == nil
		state.mu.Unlock()
	}
	if cleanup != nil {
		cleanup = failure(ErrCleanup, "debugger-close", cleanup)
	}
	return cleanup
}

// Debugger operations must coexist with the ordinary action that caused a
// pause. They keep normal command/output admission and join Control cleanup.
func runDebuggerCommand[P, R any](ctx context.Context, state *controlState, command cdp.Command[P, R], params P, checkLocked func() error, fill func(*P) error) (result R, err error) {
	if state == nil {
		return result, failure(ErrInput, "debugger-control")
	}
	check := func() error {
		if state.debugger == nil || !state.debugger.evidence.Enabled || state.closing || state.closed {
			return failure(ErrState, "debugger-domain")
		}
		if checkLocked != nil {
			return checkLocked()
		}
		return nil
	}
	work, finish, err := state.enterCommand(ctx, check)
	if err != nil {
		return result, err
	}
	defer finish()
	returned := false
	defer func() {
		if !returned {
			state.session.remember(failure(ErrPanic, "debugger-did-not-return"))
		} else {
			state.session.remember(err)
		}
	}()
	err = invoke(func() error {
		schema := protocolCommands[command.Method]
		if !nativeCommandType(reflect.TypeFor[P](), schema.params) || !nativeCommandType(reflect.TypeFor[R](), schema.result) {
			return failure(ErrInput, "debugger-command-types")
		}
		if err := state.session.reserveCommand(); err != nil {
			return err
		}
		buffer := boundedBuffer{limit: state.session.owner.settings.MaxCommandBytes}
		if err := marshalBounded(&buffer, params); err != nil {
			return err
		}
		var copied P
		if err := jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions); err != nil {
			return err
		}
		if fill != nil {
			if err := fill(&copied); err != nil {
				return err
			}
		}
		verify := func() error {
			state.mu.Lock()
			defer state.mu.Unlock()
			return check()
		}
		var callErr error
		result, callErr = cdp.Call(state.session.commandWork(work, false, verify), sdk.FromContext(state.session.native).Target, command, copied)
		return callErr
	})
	err = errors.Join(err, ctx.Err(), context.Cause(ctx), work.Err(), context.Cause(work))
	if errors.Is(err, sdk.ErrCommandLimit) {
		err = failure(ErrLimit, "debugger-command-bytes", err)
	}
	if err != nil {
		err = failure(ErrNative, "debugger-command", err)
	}
	returned = true
	return result, err
}
