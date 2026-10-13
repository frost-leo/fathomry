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
	"sync"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/debugger"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/performance"
	"github.com/chromedp/cdproto/profiler"
	"github.com/chromedp/cdproto/runtime"
	"github.com/chromedp/cdproto/serviceworker"
	sdk "github.com/chromedp/chromedp"
)

// ControlOptions selects page-scoped producers. Native parameters are copied
// before enabling interception. One control lease can own each page at a time.
type ControlOptions struct {
	private
	Fetch                *fetch.EnableParams
	Dialogs              bool
	Screencast           *page.StartScreencastParams
	Recording            *page.StartScreenRecordingParams
	FileChooser          bool
	Downloads            *DownloadOptions
	Network              bool
	ServiceWorkers       bool
	Profiler             bool
	Performance          *performance.EnableParams
	Debugger             *debugger.EnableParams
	SharedIsolateEffects bool
}

// Control owns bounded paused requests, dialogs and screencast frame replies.
// Its cumulative event limit is MaxEvents. Its ledger fits MaxResultBytes.
type Control struct {
	private
	state *controlState
}

type controlState struct {
	session             *Session
	life                context.Context
	cancel              context.CancelCauseFunc
	stopCaller          func() bool
	unsubscribe         func()
	setupDone, pumpDone chan struct{}
	gate                chan struct{}
	pending             chan *pendingState
	mu                  sync.Mutex
	records             []*pendingState
	bytes               int64
	eventCount          int
	eventReservations   map[uint64]struct{}
	active              int
	idle                chan struct{}
	commands            int
	commandsIdle        chan struct{}
	reader              bool
	readerDone          chan struct{}
	dialogCurrent       uint64
	fetch, dialogs      bool
	screencast          bool
	fileChooser         bool
	chooserCurrent      uint64
	chooserFrame        cdp.FrameID
	closing, closed     bool
	primary, cleanup    error
	downloads           *downloadState
	network             *requestLedger
	workers             *workerLedger
	workersEnabled      bool
	profilerEnabled     bool
	performanceEnabled  bool
	recording           *recordingState
	debugger            *debuggerState
}

type pendingState struct {
	control      *controlState
	method       string
	data         []byte
	request      fetch.RequestID
	frame        int64
	chooserNode  cdp.BackendNodeID
	chooserMode  page.FileChooserOpenedMode
	response     bool
	sequence     uint64
	attempted    bool
	reply        string
	acknowledged bool
	refused      int
	download     *downloadRecord
	body         *responseBodyState
	network      *requestState
	workers      workerEvent
	recording    *recordingState
	debugger     *debuggerState
	script       *debuggerScript
	frames       []debuggerFrame
}

// Pending is an opaque, single-use reply capability. Native request IDs in its
// payload are observations, not authority to act on a different target.
type Pending struct {
	private
	state *pendingState
}

// Reply is a closed native reply operation. Parameters are borrowed until Reply
// returns, and the caller's structs are not modified when identity is filled in.
type Reply struct {
	private
	method string
	params func(*pendingState) (any, error)
	check  func(*pendingState) error
}

func ContinueRequest(params fetch.ContinueRequestParams) Reply {
	return Reply{method: fetch.ContinueRequest.Method, params: func(p *pendingState) (any, error) {
		if p.method != fetch.RequestPaused.Method || p.body != nil && p.body.streaming || params.RequestID != "" && params.RequestID != p.request {
			return nil, failure(ErrInput, "request-grant")
		}
		copied := params
		copied.RequestID = p.request
		return copied, nil
	}}
}
func ContinueResponse(params fetch.ContinueResponseParams) Reply {
	return Reply{method: fetch.ContinueResponse.Method, params: func(p *pendingState) (any, error) {
		if p.method != fetch.RequestPaused.Method || !p.response || p.body != nil && p.body.streaming || params.RequestID != "" && params.RequestID != p.request {
			return nil, failure(ErrInput, "response-grant")
		}
		copied := params
		copied.RequestID = p.request
		return copied, nil
	}}
}
func FailRequest(params fetch.FailRequestParams) Reply {
	return Reply{method: fetch.FailRequest.Method, params: func(p *pendingState) (any, error) {
		if p.method != fetch.RequestPaused.Method || params.RequestID != "" && params.RequestID != p.request {
			return nil, failure(ErrInput, "request-grant")
		}
		copied := params
		copied.RequestID = p.request
		return copied, nil
	}}
}
func FulfillRequest(params fetch.FulfillRequestParams) Reply {
	return Reply{method: fetch.FulfillRequest.Method, params: func(p *pendingState) (any, error) {
		if p.method != fetch.RequestPaused.Method || params.RequestID != "" && params.RequestID != p.request {
			return nil, failure(ErrInput, "request-grant")
		}
		copied := params
		copied.RequestID = p.request
		return copied, nil
	}}
}
func ContinueWithAuth(params fetch.ContinueWithAuthParams) Reply {
	return Reply{method: fetch.ContinueWithAuth.Method, params: func(p *pendingState) (any, error) {
		if p.method != fetch.AuthRequired.Method || params.AuthChallengeResponse == nil || params.RequestID != "" && params.RequestID != p.request {
			return nil, failure(ErrInput, "authentication-grant")
		}
		copied := params
		copied.RequestID = p.request
		return copied, nil
	}}
}
func HandleDialog(params page.HandleJavaScriptDialogParams) Reply {
	return Reply{method: page.HandleJavaScriptDialog.Method, params: func(p *pendingState) (any, error) {
		if p.method != page.JavascriptDialogOpening.Method {
			return nil, failure(ErrInput, "dialog-grant")
		}
		return params, nil
	}}
}

// ScreencastFrameAck acknowledges only the frame named by the pending token.
func ScreencastFrameAck() Reply {
	return Reply{method: page.ScreencastFrameAck.Method, params: func(p *pendingState) (any, error) {
		if p.method != page.ScreencastFrame.Method {
			return nil, failure(ErrInput, "screencast-grant")
		}
		return page.ScreencastFrameAckParams{SessionID: p.frame}, nil
	}}
}

func (session *Session) Control(ctx context.Context, options ControlOptions) (*Control, error) {
	work, leave, err := session.enter(ctx, false)
	if err != nil {
		return nil, err
	}
	defer leave()
	if options.Fetch == nil && !options.Dialogs && options.Screencast == nil && options.Recording == nil && !options.FileChooser && options.Downloads == nil && !options.Network && !options.ServiceWorkers && !options.Profiler && options.Performance == nil && options.Debugger == nil {
		return nil, failure(ErrInput, "control-options")
	}
	if options.Debugger != nil && !options.SharedIsolateEffects {
		return nil, failure(ErrInput, "debugger-isolate-consent")
	}
	if options.Recording != nil && options.Screencast != nil {
		return nil, failure(ErrInput, "recording-screencast-exclusive")
	}
	settings := session.owner.settings
	if settings.MaxResultBytes < 1024 {
		return nil, failure(ErrLimit, "control-evidence")
	}
	if options.Downloads != nil {
		if err := session.checkDownloads(*options.Downloads); err != nil {
			return nil, err
		}
	}
	if options.ServiceWorkers {
		if _, err := session.storageContext(); err != nil {
			return nil, err
		}
	}
	session.mu.Lock()
	if session.control != nil {
		session.mu.Unlock()
		return nil, failure(ErrState, "control-owner")
	}
	life, cancel := context.WithCancelCause(session.work)
	state := &controlState{session: session, life: life, cancel: cancel, setupDone: make(chan struct{}), pumpDone: make(chan struct{}), gate: make(chan struct{}, 1), pending: make(chan *pendingState, settings.MaxEvents), dialogs: options.Dialogs}
	if options.Network {
		state.network = &requestLedger{requests: make(map[network.RequestID]*requestState), events: make(map[uint64]*requestState)}
	}
	if options.ServiceWorkers {
		state.workers = newWorkerLedger()
	}
	session.control = state
	session.mu.Unlock()
	session.owner.mu.Lock()
	if session.owner.cleanupControls == nil {
		session.owner.cleanupControls = make(map[*controlState]struct{})
	}
	session.owner.cleanupControls[state] = struct{}{}
	session.owner.mu.Unlock()
	defer close(state.setupDone)
	value := &Control{state: state}
	state.stopCaller = context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	if options.Debugger != nil {
		if err := state.prepareDebugger(); err != nil {
			state.fail(err)
			cancel(err)
			state.unsubscribe = func() {}
			close(state.pumpDone)
			return value, err
		}
	}
	if options.Recording != nil {
		if err := state.prepareRecording(); err != nil {
			state.fail(err)
			cancel(err)
			state.unsubscribe = func() {}
			close(state.pumpDone)
			return value, err
		}
	}
	if options.Downloads != nil {
		if err := state.prepareDownloads(*options.Downloads); err != nil {
			state.fail(err)
			cancel(err)
			state.unsubscribe = func() {}
			close(state.pumpDone)
			return value, err
		}
	}
	methods := []string{}
	if options.Fetch != nil {
		methods = append(methods, fetch.RequestPaused.Method, fetch.AuthRequired.Method)
	}
	if options.Dialogs {
		methods = append(methods, page.JavascriptDialogOpening.Method, page.JavascriptDialogClosed.Method)
	}
	if options.Screencast != nil {
		methods = append(methods, page.ScreencastFrame.Method)
	}
	if options.FileChooser {
		methods = append(methods, page.FileChooserOpened.Method, page.FrameNavigated.Method, page.FrameDetached.Method)
	}
	if options.Downloads != nil {
		methods = append(methods, browser.DownloadWillBegin.Method, browser.DownloadProgress.Method)
	}
	if options.Network {
		methods = append(methods, network.RequestWillBeSent.Method, network.ResponseReceived.Method, network.LoadingFinished.Method, network.LoadingFailed.Method)
	}
	if options.ServiceWorkers {
		methods = append(methods, serviceworker.WorkerRegistrationUpdated.Method, serviceworker.WorkerVersionUpdated.Method, serviceworker.WorkerErrorReported.Method)
	}
	if options.Profiler {
		methods = append(methods, profiler.ConsoleProfileStarted.Method, profiler.ConsoleProfileFinished.Method, profiler.PreciseCoverageDeltaUpdate.Method)
	}
	if options.Performance != nil {
		methods = append(methods, performance.Metrics.Method)
	}
	if options.Debugger != nil {
		methods = append(methods, debugger.Paused.Method, debugger.Resumed.Method, debugger.ScriptParsed.Method, debugger.ScriptFailedToParse.Method, runtime.ExecutionContextsCleared.Method)
	}
	if len(methods) == 0 {
		state.unsubscribe = func() {}
		close(state.pumpDone)
	} else {
		state.eventCount = len(state.records)
		state.eventReservations = make(map[uint64]struct{})
		raw, unsubscribe := sdk.FromContext(session.native).Target.SubscribeBounded(sdk.EventBuffer{
			MaxEvents: settings.MaxEvents, MaxBytes: settings.MaxEventBytes,
			OnOverflow: func(err error) {
				state.downloadLoss()
				state.fail(failure(ErrLimit, "control-overflow", err))
				cancel(err)
			},
			OnError: func(err error) {
				state.downloadLoss()
				state.fail(failure(ErrNative, "control-origin", err))
				cancel(err)
			},
			Observe: func(method string, data jsonv2.Value, sequence uint64) error {
				state.mu.Lock()
				defer state.mu.Unlock()
				if err := state.reserveControlEvent(method, data, sequence); err != nil {
					return err
				}
				switch method {
				case page.JavascriptDialogOpening.Method:
					state.dialogCurrent = sequence
				case page.JavascriptDialogClosed.Method:
					state.dialogCurrent = 0
				}
				if err := state.observeChooser(method, data, sequence); err != nil {
					return err
				}
				if err := state.observeRequest(method, data, sequence); err != nil {
					return err
				}
				if err := state.observeWorkers(method, data, sequence); err != nil {
					return err
				}
				if err := state.observeDebugger(method, data, sequence); err != nil {
					return err
				}
				return state.observeDownload(method, data)
			},
		}, methods...)
		state.unsubscribe = unsubscribe
		go state.pump(raw)
	}
	if options.Fetch != nil {
		buffer := boundedBuffer{limit: settings.MaxCommandBytes}
		err = marshalBounded(&buffer, options.Fetch)
		var copied fetch.EnableParams
		if err == nil {
			err = jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions)
		}
		if err == nil {
			state.fetch = true
			_, err = cdp.Call(state.work(work), sdk.FromContext(session.native).Target, fetch.Enable, copied)
		}
	}
	if err == nil && options.Screencast != nil {
		buffer := boundedBuffer{limit: settings.MaxCommandBytes}
		err = marshalBounded(&buffer, options.Screencast)
		var copied page.StartScreencastParams
		if err == nil {
			err = jsonv2.Unmarshal(buffer.data, &copied, sdk.DefaultUnmarshalOptions)
		}
		if err == nil {
			state.screencast = true
			_, err = cdp.Call(state.work(work), sdk.FromContext(session.native).Target, page.StartScreencast, copied)
		}
	}
	if err == nil && options.FileChooser {
		state.fileChooser = true
		_, err = cdp.Call(state.work(work), sdk.FromContext(session.native).Target, page.SetInterceptFileChooserDialog, page.SetInterceptFileChooserDialogParams{Enabled: true})
	}
	if err == nil && options.Downloads != nil {
		err = state.enableDownloads(work)
	}
	if err == nil && options.ServiceWorkers {
		state.workersEnabled = true
		_, err = cdp.Call(state.work(work), sdk.FromContext(session.native).Target, serviceworker.Enable, cdp.Empty{})
	}
	if err == nil && options.Profiler {
		state.profilerEnabled = true
		_, err = cdp.Call(state.work(work), sdk.FromContext(session.native).Target, profiler.Enable, cdp.Empty{})
	}
	if err == nil && options.Performance != nil {
		copied := *options.Performance
		state.performanceEnabled = true
		_, err = cdp.Call(state.work(work), sdk.FromContext(session.native).Target, performance.Enable, copied)
	}
	if err == nil && options.Recording != nil {
		err = state.startRecording(work, *options.Recording)
	}
	if err == nil && options.Debugger != nil {
		err = state.enableDebugger(work, *options.Debugger)
	}
	if err != nil {
		state.fail(err)
		cancel(err)
		err = failure(ErrNative, "control-setup", err)
	}
	return value, err
}

func (state *controlState) work(ctx context.Context) context.Context {
	return sdk.WithCallLimits(ctx, sdk.CallLimits{MaxInputBytes: state.session.owner.settings.MaxCommandBytes, MaxOutputBytes: 65536})
}
func (state *controlState) fail(err error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.primary == nil {
		state.primary = failure(ErrNative, "control", err)
	}
}
func (state *controlState) pump(raw <-chan jsonv2.Value) {
	defer close(state.pumpDone)
	defer func() {
		if state.downloads == nil {
			state.unsubscribe()
		}
	}()
	for {
		select {
		case <-state.life.Done():
			return
		case data, ok := <-raw:
			if !ok {
				if state.life.Err() == nil {
					state.fail(failure(ErrDisconnected, "control-events"))
					state.cancel(ErrDisconnected)
				}
				return
			}
			if state.eventReservations == nil {
				state.mu.Lock()
				if len(state.records) >= state.session.owner.settings.MaxEvents || int64(len(data))+512 > state.session.owner.settings.MaxResultBytes-state.bytes {
					state.mu.Unlock()
					err := failure(ErrLimit, "control-ledger")
					state.fail(err)
					state.cancel(err)
					return
				}
				state.bytes += int64(len(data)) + 512
				state.mu.Unlock()
			}
			var message struct {
				cdproto.Message
				Sequence uint64 `json:"sequence"`
			}
			err := jsonv2.Unmarshal(data, &message, sdk.DefaultUnmarshalOptions)
			if err == nil && state.eventReservations != nil {
				state.mu.Lock()
				_, reserved := state.eventReservations[message.Sequence]
				delete(state.eventReservations, message.Sequence)
				state.mu.Unlock()
				if !reserved {
					err = failure(ErrNative, "control-event-reservation")
				}
			}
			pending := &pendingState{control: state, method: string(message.Method), sequence: message.Sequence}
			if err == nil {
				switch pending.method {
				case fetch.RequestPaused.Method:
					var event fetch.EventRequestPaused
					err = jsonv2.Unmarshal(message.Params, &event, sdk.DefaultUnmarshalOptions)
					pending.request = event.RequestID
					pending.response = event.ResponseStatusCode != 0 || event.ResponseErrorReason != ""
					if err == nil && pending.request == "" {
						err = failure(ErrNative, "missing-fetch-identity")
					}
				case fetch.AuthRequired.Method:
					var event fetch.EventAuthRequired
					err = jsonv2.Unmarshal(message.Params, &event, sdk.DefaultUnmarshalOptions)
					pending.request = event.RequestID
					if err == nil && pending.request == "" {
						err = failure(ErrNative, "missing-auth-identity")
					}
				case page.JavascriptDialogOpening.Method:
					var event page.EventJavascriptDialogOpening
					err = jsonv2.Unmarshal(message.Params, &event, sdk.DefaultUnmarshalOptions)
				case page.JavascriptDialogClosed.Method:
					continue
				case page.FrameNavigated.Method, page.FrameDetached.Method:
					continue
				case page.FileChooserOpened.Method:
					var event page.EventFileChooserOpened
					err = jsonv2.Unmarshal(message.Params, &event, sdk.DefaultUnmarshalOptions)
					pending.chooserNode, pending.chooserMode = event.BackendNodeID, event.Mode
				case page.ScreencastFrame.Method:
					var event struct {
						page.EventScreencastFrame
						Frame *int64 `json:"sessionId"`
					}
					err = jsonv2.Unmarshal(message.Params, &event, sdk.DefaultUnmarshalOptions)
					if err == nil && (event.Frame == nil || *event.Frame < 0 || event.Metadata == nil) {
						err = failure(ErrNative, "screencast-frame")
					}
					if err == nil {
						pending.frame = *event.Frame
					}
				case browser.DownloadWillBegin.Method, browser.DownloadProgress.Method:
					var event struct {
						GUID string `json:"guid"`
					}
					err = jsonv2.Unmarshal(message.Params, &event, sdk.DefaultUnmarshalOptions)
					state.mu.Lock()
					pending.download = state.downloads.records[event.GUID]
					state.mu.Unlock()
					if err == nil && pending.download == nil {
						err = failure(ErrNative, "download-origin")
					}
				case network.RequestWillBeSent.Method, network.ResponseReceived.Method, network.LoadingFinished.Method, network.LoadingFailed.Method:
					state.mu.Lock()
					var observed bool
					if state.network != nil {
						pending.network, observed = state.network.events[message.Sequence]
						delete(state.network.events, message.Sequence)
					}
					state.mu.Unlock()
					if !observed {
						err = failure(ErrNative, "request-origin")
					}
				case serviceworker.WorkerRegistrationUpdated.Method, serviceworker.WorkerVersionUpdated.Method, serviceworker.WorkerErrorReported.Method:
					state.mu.Lock()
					var observed bool
					if state.workers != nil {
						pending.workers, observed = state.workers.events[message.Sequence]
						delete(state.workers.events, message.Sequence)
					}
					state.mu.Unlock()
					if !observed {
						err = failure(ErrNative, "service-worker-origin")
					}
				case profiler.ConsoleProfileStarted.Method, profiler.ConsoleProfileFinished.Method, profiler.PreciseCoverageDeltaUpdate.Method, performance.Metrics.Method:
					_, err = decodeEvent(data)
				case debugger.Paused.Method:
					err = pending.decodeFrames(message.Params)
				case debugger.Resumed.Method:
					// Wire-order observation already retired the previous pause.
				case runtime.ExecutionContextsCleared.Method:
					continue
				case debugger.ScriptParsed.Method, debugger.ScriptFailedToParse.Method:
					state.mu.Lock()
					pending.script = state.debugger.scriptEvents[message.Sequence]
					delete(state.debugger.scriptEvents, message.Sequence)
					state.mu.Unlock()
					if pending.script == nil {
						err = failure(ErrNative, "debugger-script-origin")
					}
				default:
					err = failure(ErrNative, "control-event")
				}
			}
			if err != nil {
				state.fail(err)
				state.cancel(err)
				return
			}
			pending.data = append([]byte(nil), message.Params...)
			state.mu.Lock()
			state.records = append(state.records, pending)
			state.mu.Unlock()
			select {
			case state.pending <- pending:
			case <-state.life.Done():
				return
			}
		}
	}
}

// The native observer and pump share one cumulative ledger. Queued events must
// reserve before identity decoding, not later when the consumer catches up.
// The caller holds state.mu; event method names are this fixed subscription set.
func (state *controlState) reserveControlEvent(method string, data jsonv2.Value, sequence uint64) error {
	charge := int64(len(method)) + int64(len(data)) + 640
	settings := state.session.owner.settings
	if sequence == 0 || state.eventCount >= settings.MaxEvents || charge > settings.MaxResultBytes-state.bytes {
		return failure(ErrLimit, "control-ledger")
	}
	if _, exists := state.eventReservations[sequence]; exists {
		return failure(ErrNative, "control-event-sequence")
	}
	state.eventCount++
	state.bytes += charge
	state.eventReservations[sequence] = struct{}{}
	return nil
}

func (control *Control) Next(ctx context.Context) (Pending, error) {
	if control == nil || control.state == nil || ctx == nil {
		return Pending{}, failure(ErrInput, "control-next")
	}
	state := control.state
	state.mu.Lock()
	if state.reader || state.closing || state.closed {
		state.mu.Unlock()
		return Pending{}, failure(ErrState, "control-reader")
	}
	state.reader = true
	state.readerDone = make(chan struct{})
	state.mu.Unlock()
	defer func() { state.mu.Lock(); state.reader = false; close(state.readerDone); state.mu.Unlock() }()
	if err := ctx.Err(); err != nil {
		return Pending{}, failure(ErrState, "control-next", err, context.Cause(ctx))
	}
	select {
	case <-ctx.Done():
		return Pending{}, failure(ErrState, "control-next", ctx.Err(), context.Cause(ctx))
	case <-state.life.Done():
		return Pending{}, failure(ErrState, "control-next", context.Cause(state.life))
	case pending := <-state.pending:
		if state.life.Err() != nil {
			return Pending{}, failure(ErrState, "control-next", context.Cause(state.life))
		}
		return Pending{state: pending}, nil
	}
}
func (pending Pending) Method() string {
	if pending.state == nil {
		return ""
	}
	return pending.state.method
}
func (pending Pending) DataCopy() []byte {
	if pending.state == nil {
		return nil
	}
	return append([]byte(nil), pending.state.data...)
}

// Reply uses the existing control reservation, not ordinary action admission.
// A submitted reply is single-use even when cancellation makes its effect unknown.
func (pending Pending) Reply(ctx context.Context, reply Reply) error {
	if pending.state == nil || reply.params == nil || ctx == nil {
		return failure(ErrInput, "control-reply")
	}
	p, state := pending.state, pending.state.control
	state.mu.Lock()
	if state.closing || state.closed || state.active != 0 || p.attempted || state.life.Err() != nil || ctx.Err() != nil {
		state.mu.Unlock()
		return failure(ErrState, "control-reply", ctx.Err(), context.Cause(ctx))
	}
	if p.stale() {
		p.refused++
		err := failure(ErrState, "stale-control")
		if state.primary == nil {
			state.primary = err
		}
		state.mu.Unlock()
		return err
	}
	if err := p.responseReplyReady(); err != nil {
		p.refused++
		if state.primary == nil {
			state.primary = err
		}
		state.mu.Unlock()
		return err
	}
	params, err := reply.params(p)
	if err != nil {
		p.refused++
		if state.primary == nil {
			state.primary = err
		}
		state.mu.Unlock()
		return err
	}
	if state.active == 0 {
		state.idle = make(chan struct{})
	}
	state.active++
	state.mu.Unlock()
	defer func() {
		state.mu.Lock()
		state.active--
		if state.active == 0 {
			close(state.idle)
		}
		state.mu.Unlock()
	}()
	work, cancel := context.WithCancelCause(state.life)
	stop := context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	defer stop()
	defer cancel(nil)
	buffer := boundedBuffer{limit: state.session.owner.settings.MaxCommandBytes}
	err = marshalBounded(&buffer, params)
	state.mu.Lock()
	if err == nil && (state.closing || state.life.Err() != nil || p.stale()) {
		err = failure(ErrState, "stale-control")
	}
	if err == nil && reply.check != nil {
		err = reply.check(p)
	}
	if err != nil {
		p.refused++
		if state.primary == nil {
			state.primary = failure(ErrNative, "control-preflight", err)
		}
		state.mu.Unlock()
		return failure(ErrNative, "control-preflight", err)
	}
	p.attempted = true
	p.reply = reply.method
	state.mu.Unlock()
	err = sdk.FromContext(state.session.native).Target.Call(state.work(work), reply.method, jsonv2.Value(buffer.data), nil)
	err = errors.Join(err, ctx.Err(), context.Cause(ctx), state.life.Err(), context.Cause(state.life))
	if err != nil {
		err = failure(ErrNative, "control-reply", err)
	}
	state.mu.Lock()
	p.acknowledged = err == nil
	if p.download != nil && reply.method == browser.CancelDownload.Method {
		p.download.evidence.CancelAttempted = true
		p.download.evidence.CancelAcknowledged = err == nil
	}
	if err != nil && state.primary == nil {
		state.primary = err
	}
	state.mu.Unlock()
	return err
}

func (control *Control) Stopping() <-chan struct{} {
	if control == nil || control.state == nil {
		return nil
	}
	return control.state.life.Done()
}
func (control *Control) Closed() bool {
	if control == nil || control.state == nil {
		return false
	}
	control.state.mu.Lock()
	defer control.state.mu.Unlock()
	return control.state.closed
}
func (control *Control) Err() error {
	if control == nil || control.state == nil {
		return failure(ErrInput, "control")
	}
	control.state.mu.Lock()
	defer control.state.mu.Unlock()
	return control.state.primary
}

// Close disables interception, stops screencast and dismisses outstanding dialogs
// before dropping the connection. Failed cleanup keeps the same owner for retry.
func (control *Control) Close(ctx context.Context) error {
	if control == nil || control.state == nil || ctx == nil {
		return failure(ErrInput, "control-close")
	}
	state := control.state
	defer func() { state.session.remember(control.Err()) }()
	state.mu.Lock()
	state.closing = true
	state.mu.Unlock()
	state.cancel(context.Canceled)
	work, cancel := context.WithTimeout(ctx, state.session.owner.settings.CleanupTimeout)
	defer cancel()
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-work.Done():
		return failure(ErrCleanup, "control-join", work.Err(), context.Cause(work))
	}
	select {
	case <-state.setupDone:
	case <-work.Done():
		return failure(ErrCleanup, "control-setup", work.Err(), context.Cause(work))
	}
	if !state.session.nativeResourcesGone() {
		state.beginRecordingStop()
		state.beginDebuggerCleanup()
	}
	select {
	case <-state.pumpDone:
	case <-work.Done():
		return failure(ErrCleanup, "control-producer", work.Err(), context.Cause(work))
	}
	state.mu.Lock()
	closed, active, idle, history := state.closed, state.active, state.idle, state.cleanup
	reading, readerDone := state.reader, state.readerDone
	state.mu.Unlock()
	if closed {
		return history
	}
	if active != 0 {
		select {
		case <-idle:
		case <-work.Done():
			return failure(ErrCleanup, "control-replies", work.Err(), context.Cause(work))
		}
	}
	if reading {
		select {
		case <-readerDone:
		case <-work.Done():
			return failure(ErrCleanup, "control-reader", work.Err(), context.Cause(work))
		}
	}
	if err := state.joinCommands(work); err != nil {
		return err
	}
	if state.session.nativeResourcesGone() && (state.downloads == nil || state.downloadContextGone()) {
		return errors.Join(history, state.complete(work))
	}
	var cleanup error
	target := sdk.FromContext(state.session.native).Target
	cleanup = state.closeDownloads(work)
	bodyComplete, bodyCleanup := state.closeResponseBodies(work)
	if bodyCleanup != nil {
		err := bodyCleanup
		state.session.rememberCleanup(err)
		state.mu.Lock()
		if state.cleanup == nil {
			state.cleanup = err
		}
		history = state.cleanup
		state.mu.Unlock()
	}
	if state.fetch && bodyComplete {
		_, err := cdp.Call(state.work(work), target, fetch.Disable, cdp.Empty{})
		cleanup = errors.Join(cleanup, err)
		if err == nil {
			state.fetch = false
		}
	}
	if state.workersEnabled {
		_, err := cdp.Call(state.work(work), target, serviceworker.Disable, cdp.Empty{})
		cleanup = errors.Join(cleanup, err)
		if err == nil {
			state.workersEnabled = false
		}
	}
	if state.profilerEnabled {
		_, err := cdp.Call(state.work(work), target, profiler.Disable, cdp.Empty{})
		cleanup = errors.Join(cleanup, err)
		if err == nil {
			state.profilerEnabled = false
		}
	}
	if state.performanceEnabled {
		_, err := cdp.Call(state.work(work), target, performance.Disable, cdp.Empty{})
		cleanup = errors.Join(cleanup, err)
		if err == nil {
			state.performanceEnabled = false
		}
	}
	if state.screencast {
		_, err := cdp.Call(state.work(work), target, page.StopScreencast, cdp.Empty{})
		cleanup = errors.Join(cleanup, err)
		if err == nil {
			state.screencast = false
		}
	}
	if state.fileChooser {
		_, err := cdp.Call(state.work(work), target, page.SetInterceptFileChooserDialog, page.SetInterceptFileChooserDialogParams{Enabled: false})
		cleanup = errors.Join(cleanup, err)
		if err == nil {
			state.fileChooser = false
		}
	}
	if state.dialogs {
		_, err := cdp.Call(state.work(work), target, page.HandleJavaScriptDialog, page.HandleJavaScriptDialogParams{Accept: false})
		var native *cdproto.Error
		if errors.As(err, &native) && native.Code == -32602 && native.Message == "No dialog is showing" {
			err = nil
		}
		cleanup = errors.Join(cleanup, err)
		if err == nil {
			state.dialogs = false
		}
	}
	recordingComplete, recordingCleanup := state.closeRecording(work)
	if recordingCleanup != nil {
		state.session.rememberCleanup(recordingCleanup)
		state.mu.Lock()
		if state.cleanup == nil {
			state.cleanup = recordingCleanup
		}
		history = state.cleanup
		state.mu.Unlock()
	}
	debuggerComplete, debuggerCleanup := state.closeDebugger(work)
	if debuggerCleanup != nil {
		state.session.rememberCleanup(debuggerCleanup)
		state.mu.Lock()
		if state.cleanup == nil {
			state.cleanup = debuggerCleanup
		}
		history = state.cleanup
		state.mu.Unlock()
	}
	if !bodyComplete || !recordingComplete || !debuggerComplete {
		return errors.Join(history, cleanup, bodyCleanup, recordingCleanup, debuggerCleanup)
	}
	if cleanup != nil {
		cleanup = failure(ErrCleanup, "control-close", cleanup)
		state.mu.Lock()
		if state.cleanup == nil {
			state.cleanup = cleanup
		}
		history = state.cleanup
		state.mu.Unlock()
		state.session.rememberCleanup(cleanup)
		return errors.Join(history, cleanup)
	}
	return errors.Join(history, state.complete(work))
}
func (state *controlState) disposed(ctx context.Context) error {
	state.cancel(context.Canceled)
	select {
	case state.gate <- struct{}{}:
		defer func() { <-state.gate }()
	case <-ctx.Done():
		return failure(ErrCleanup, "control-disposal-join", ctx.Err(), context.Cause(ctx))
	}
	return state.complete(ctx)
}
func (state *controlState) complete(ctx context.Context) error {
	state.mu.Lock()
	state.closing = true
	active, idle, reading, readerDone := state.active, state.idle, state.reader, state.readerDone
	state.mu.Unlock()
	for _, wait := range []struct {
		needed bool
		done   <-chan struct{}
	}{
		{true, state.setupDone}, {true, state.pumpDone}, {active != 0, idle}, {reading, readerDone},
	} {
		if wait.needed {
			select {
			case <-wait.done:
			case <-ctx.Done():
				return failure(ErrCleanup, "control-work-join", ctx.Err(), context.Cause(ctx))
			}
		}
	}
	if err := state.joinCommands(ctx); err != nil {
		return err
	}
	debuggerComplete, debuggerCleanup := state.closeDebugger(ctx)
	if !debuggerComplete {
		return debuggerCleanup
	}
	recordingComplete, recordingCleanup := state.closeRecording(ctx)
	if !recordingComplete {
		return recordingCleanup
	}
	if err := state.closeDownloads(ctx); err != nil {
		return failure(ErrCleanup, "download-close", err)
	}
	bodyComplete, bodyCleanup := state.closeResponseBodies(ctx)
	if !bodyComplete {
		return bodyCleanup
	}
	state.stopCaller()
	state.mu.Lock()
	state.closed = true
	state.mu.Unlock()
	state.session.mu.Lock()
	if state.session.control == state {
		state.session.control = nil
	}
	state.session.mu.Unlock()
	state.session.owner.mu.Lock()
	delete(state.session.owner.cleanupControls, state)
	state.session.owner.mu.Unlock()
	return errors.Join(bodyCleanup, recordingCleanup, debuggerCleanup)
}

// EvidenceCopy contains the bounded control ledger, not raw errors or reply
// credentials. It does not run a reply again and is available after Close.
func (control *Control) EvidenceCopy() ([]byte, error) {
	if control == nil || control.state == nil {
		return nil, failure(ErrInput, "control-evidence")
	}
	state := control.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.closed {
		return nil, failure(ErrState, "control-evidence")
	}
	type record struct {
		Method       string                `json:"method"`
		Event        jsonv2.Value          `json:"event"`
		Reply        string                `json:"reply"`
		Attempted    bool                  `json:"attempted"`
		Acknowledged bool                  `json:"acknowledged"`
		Refused      int                   `json:"refused"`
		Download     *downloadEvidence     `json:"download,omitempty"`
		Body         *responseBodyEvidence `json:"responseBody,omitempty"`
		Recording    *recordingEvidence    `json:"recording,omitempty"`
		Debugger     *debuggerEvidence     `json:"debugger,omitempty"`
	}
	records := make([]record, len(state.records))
	downloadSeen := make(map[*downloadRecord]bool)
	for i, pending := range state.records {
		records[i] = record{Method: pending.method, Event: pending.data, Reply: pending.reply, Attempted: pending.attempted, Acknowledged: pending.acknowledged, Refused: pending.refused}
		if pending.body != nil {
			records[i].Body = &pending.body.evidence
		}
		if pending.debugger != nil {
			records[i].Debugger = &pending.debugger.evidence
		}
		if pending.recording != nil {
			value := pending.recording.evidence
			stream := pending.recording.stream.state
			stream.mu.Lock()
			value.StreamClosed, value.EOF, value.Bytes = stream.closed, stream.eof, stream.bytes
			stream.mu.Unlock()
			records[i].Recording = &value
		}
		if pending.method == browser.DownloadWillBegin.Method && pending.download != nil {
			records[i].Download = &pending.download.evidence
			downloadSeen[pending.download] = true
		}
	}
	if state.downloads != nil {
		for _, value := range state.downloads.records {
			if !downloadSeen[value] {
				records = append(records, record{Method: browser.DownloadWillBegin.Method, Download: &value.evidence})
			}
		}
	}
	buffer := boundedBuffer{limit: state.session.owner.settings.MaxResultBytes}
	err := marshalBounded(&buffer, records)
	return buffer.data, err
}
