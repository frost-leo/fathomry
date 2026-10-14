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
	_ "embed"
	"errors"
	goruntime "runtime"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp"
	jsonv2 "github.com/chromedp/cdproto/cdp/jsonv2"
	"github.com/chromedp/cdproto/dom"
	"github.com/chromedp/cdproto/runtime"
)

func managedReplyTarget(t *testing.T, reply func(*cdproto.Message) *cdproto.Message) *Target {
	t.Helper()
	browser := &Browser{cmdQueue: make(chan *cdproto.Message), LostConnection: make(chan struct{})}
	target := &Target{browser: browser, stopped: make(chan struct{})}
	stop, done := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(stop); <-done })
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case message := <-browser.cmdQueue:
				result := reply(message)
				if result == nil {
					continue
				}
				result.ID = message.ID
				target.listenersMu.Lock()
				target.listeners = runListeners(target.listeners, result)
				target.listenersMu.Unlock()
			}
		}
	}()
	return target
}

func TestManagedNodeHelperPreservesPrimaryAndCleanupErrors(t *testing.T) {
	primary := &cdproto.Error{Code: -32001, Message: "primary fixture error"}
	cleanup := &cdproto.Error{Code: -32002, Message: "cleanup fixture error"}
	target := managedReplyTarget(t, func(message *cdproto.Message) *cdproto.Message {
		switch string(message.Method) {
		case dom.ResolveNode.Method:
			return &cdproto.Message{Result: jsonv2.Value(`{"object":{"type":"object","objectId":"original"}}`)}
		case runtime.CallFunctionOn.Method:
			return &cdproto.Message{Error: primary}
		case runtime.ReleaseObject.Method:
			var params runtime.ReleaseObjectParams
			if jsonv2.Unmarshal(message.Params, &params) != nil || params.ObjectID != "original" {
				t.Error("helper cleanup changed native identity")
			}
			return &cdproto.Message{Error: cleanup}
		}
		t.Error("unexpected helper command", message.Method)
		return &cdproto.Message{Result: jsonv2.Value(`{}`)}
	})
	_, err := callFunctionOnNode[string](context.Background(), target, &Node{Node: &cdp.Node{NodeID: 1}}, "function(){return this.textContent}")
	var observed *HelperCleanupError
	if !errors.Is(err, primary) || !errors.Is(err, cleanup) || !errors.As(err, &observed) || observed.Operation != runtime.ReleaseObject.Method {
		t.Fatal("helper discarded an independent failure", err)
	}
}

func TestManagedSearchHelperPreservesCleanupError(t *testing.T) {
	cleanup := &cdproto.Error{Code: -32002, Message: "search cleanup fixture"}
	target := managedReplyTarget(t, func(message *cdproto.Message) *cdproto.Message {
		switch string(message.Method) {
		case dom.PerformSearch.Method:
			return &cdproto.Message{Result: jsonv2.Value(`{"searchId":"original","resultCount":1}`)}
		case dom.GetSearchResults.Method:
			return &cdproto.Message{Result: jsonv2.Value(`{"nodeIds":[1]}`)}
		case dom.DiscardSearchResults.Method:
			return &cdproto.Message{Error: cleanup}
		}
		t.Error("unexpected search command", message.Method)
		return &cdproto.Message{Result: jsonv2.Value(`{}`)}
	})
	nodes, err := search("fixture")(context.Background(), target, &Node{})
	if len(nodes) != 1 || !errors.Is(err, cleanup) || !isHelperCleanup(err) {
		t.Fatal("search result or cleanup failure disappeared", err)
	}
}

func TestManagedHelperCleanupUsesIndependentBudget(t *testing.T) {
	var admission, calls atomic.Int64
	target := managedReplyTarget(t, func(message *cdproto.Message) *cdproto.Message {
		calls.Add(1)
		return &cdproto.Message{Result: jsonv2.Value(`{}`)}
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ctx = WithCallLimits(ctx, CallLimits{Before: func(string) error {
		admission.Add(1)
		return ErrCommandLimit
	}})
	if err := helperCleanup(ctx, target, runtime.ReleaseObject, runtime.ReleaseObjectParams{ObjectID: "original"}); err != nil || admission.Load() != 0 || calls.Load() != 1 {
		t.Fatal("cleanup needed ordinary admission or the canceled action context", err)
	}
	blocked := managedReplyTarget(t, func(*cdproto.Message) *cdproto.Message { return nil })
	ctx = context.WithValue(context.Background(), contextKey{}, &Context{cleanupTimeout: time.Millisecond})
	if err := helperCleanup(ctx, blocked, runtime.ReleaseObject, runtime.ReleaseObjectParams{ObjectID: "original"}); !errors.Is(err, context.DeadlineExceeded) || !isHelperCleanup(err) {
		t.Fatal("helper ignored its configured cleanup deadline", err)
	}
}

func TestManagedReadinessKeepsConcurrentCleanupFailure(t *testing.T) {
	cleanup := &HelperCleanupError{Operation: runtime.ReleaseObject.Method, Err: errors.New("cleanup fixture")}
	frame := &Frame{Nodes: map[cdp.NodeID]*Node{1: {Node: &cdp.Node{NodeID: 1}}, 2: {Node: &cdp.Node{NodeID: 2}}}}
	selector := &Selector{}
	check := selector.waitReady(func(_ context.Context, _ *Target, _ runtime.ExecutionContextID, node *Node) error {
		node.RLock()
		id := node.NodeID
		node.RUnlock()
		if id == 1 {
			return ErrNotVisible
		}
		return cleanup
	})
	for index := 0; index < 100; index++ {
		_, err := check(context.Background(), nil, frame, 0, 1, 2)
		if !errors.Is(err, cleanup) {
			t.Fatal("readiness retry hid another node's cleanup failure", err)
		}
	}
}

func TestManagedReadinessCancellationKeepsActualCleanupResult(t *testing.T) {
	cleanup := &HelperCleanupError{Operation: runtime.ReleaseObject.Method, Err: errors.New("canceled cleanup fixture")}
	frame := &Frame{Nodes: map[cdp.NodeID]*Node{1: {Node: &cdp.Node{NodeID: 1}}}}
	for index := 0; index < 100; index++ {
		ctx, cancel := context.WithCancel(context.Background())
		selector := &Selector{}
		check := selector.waitReady(func(context.Context, *Target, runtime.ExecutionContextID, *Node) error {
			cancel()
			return cleanup
		})
		_, err := check(ctx, nil, frame, 0, 1)
		cancel()
		if !errors.Is(err, cleanup) {
			t.Fatal("cancellation discarded the completed native cleanup error", err)
		}
	}
}

func TestManagedNodeCleanupNeedsObservedContextDestruction(t *testing.T) {
	for _, test := range []struct {
		name        string
		clear       bool
		code        int64
		message     string
		wantFailure bool
	}{
		{"observed-clear", true, -32000, "Cannot find context with specified id", false},
		{"missing-observation", false, -32000, "Cannot find context with specified id", true},
		{"unrelated-code", true, -32001, "Cannot find context with specified id", true},
		{"unrelated-native-error", true, -32000, "different cleanup failure", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var target *Target
			target = managedReplyTarget(t, func(message *cdproto.Message) *cdproto.Message {
				switch string(message.Method) {
				case dom.ResolveNode.Method:
					return &cdproto.Message{Result: jsonv2.Value(`{"object":{"type":"object","objectId":"original"}}`)}
				case runtime.CallFunctionOn.Method:
					if test.clear {
						if err := target.trackExecutionOrigin(&cdproto.Message{Method: cdproto.MethodType(runtime.ExecutionContextsCleared.Method), Params: jsonv2.Value(`{}`)}); err != nil {
							t.Error(err)
						}
					}
					return &cdproto.Message{Result: jsonv2.Value(`{"result":{"type":"string","value":"submitted"}}`)}
				case runtime.ReleaseObject.Method:
					return &cdproto.Message{Error: &cdproto.Error{Code: test.code, Message: test.message}}
				}
				return &cdproto.Message{Result: jsonv2.Value(`{}`)}
			})
			target.origins.contexts = map[runtime.ExecutionContextID]string{1: "original-context"}
			var observed *HelperCleanupError
			ctx := WithCallLimits(context.Background(), CallLimits{Cleanup: func(err *HelperCleanupError) { observed = err }})
			value, err := callFunctionOnNode[string](ctx, target, &Node{Node: &cdp.Node{NodeID: 1}}, "function(){return 'submitted'}")
			if value != "submitted" || (err != nil) != test.wantFailure || (observed != nil) != test.wantFailure {
				t.Fatal("context absence and generic cleanup errors were conflated", err)
			}
		})
	}
}

type managedCleanupExitValue struct{}

//go:embed js/managedNodeProperties.js
var managedNodePropertiesJS string

func TestManagedNodeGroupOwnsResultsAndExceptions(t *testing.T) {
	ctx, cancel := testAllocate(t, "")
	defer cancel()
	if err := Do(ctx, Navigate("data:text/html,<h1>node group</h1>")); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(ctx, Evaluate[bool](managedNodePropertiesJS)); err != nil {
		t.Fatal(err)
	}
	for _, property := range []string{"fixture", "throwing"} {
		name := "managed-node-" + property
		var queued, replies atomic.Int64
		work := WithNodeObjectGroup(WithCallLimits(ctx, CallLimits{
			Queued: func(string) { queued.Add(1) },
			Result: func(string, int64) error { replies.Add(1); return nil },
		}), name)
		value, err := Run(work, JavascriptAttribute[*runtime.RemoteObject](CSS("h1"), property))
		if property == "throwing" {
			var exception *ExceptionError
			if !errors.As(err, &exception) || exception.ExceptionDetails == nil {
				t.Fatal("native property exception missing", err)
			}
			value = exception.ExceptionDetails.Exception
		} else if err != nil {
			t.Fatal(err)
		}
		if value == nil || value.ObjectID == "" || queued.Load() < 2 || queued.Load() != replies.Load() {
			t.Fatal("native property result or queue accounting missing", queued.Load(), replies.Load())
		}
		if _, err := Call(ctx, runtime.GetProperties, runtime.GetPropertiesParams{ObjectID: value.ObjectID}); err != nil {
			t.Fatal("receiver cleanup also released its returned reference", err)
		}
		if _, err := Call(ctx, runtime.ReleaseObjectGroup, runtime.ReleaseObjectGroupParams{ObjectGroup: name}); err != nil {
			t.Fatal(err)
		}
		if _, err := Call(ctx, runtime.GetProperties, runtime.GetPropertiesParams{ObjectID: value.ObjectID}); err == nil {
			t.Fatal("node property reference escaped its native group")
		}
	}
}

func TestManagedPopulateWaitHonorsCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		selector := &Selector{}
		Populate(1, false, PopulateWait(time.Hour))(selector)
		start := time.Now()
		err := selector.after[0](ctx, nil, nil)
		if !errors.Is(err, context.Canceled) || time.Since(start) != 0 {
			t.Fatal("populate wait ignored cancellation", time.Since(start), err)
		}
	})
}

func (*managedCleanupExitValue) UnmarshalJSON(data []byte) error {
	if string(data) == `"goexit"` {
		goruntime.Goexit()
	}
	panic("decoder fixture")
}

func TestManagedNodeCleanupObservationSurvivesDecoderExit(t *testing.T) {
	for _, mode := range []string{"panic", "goexit"} {
		t.Run(mode, func(t *testing.T) {
			cause := &cdproto.Error{Code: -32002, Message: "cleanup fixture"}
			var calls atomic.Int64
			target := managedReplyTarget(t, func(message *cdproto.Message) *cdproto.Message {
				switch string(message.Method) {
				case dom.ResolveNode.Method:
					return &cdproto.Message{Result: jsonv2.Value(`{"object":{"type":"object","objectId":"original"}}`)}
				case runtime.CallFunctionOn.Method:
					return &cdproto.Message{Result: jsonv2.Value(`{"result":{"type":"string","value":"` + mode + `"}}`)}
				case runtime.ReleaseObject.Method:
					calls.Add(1)
					return &cdproto.Message{Error: cause}
				}
				t.Error("unexpected node helper command", message.Method)
				return &cdproto.Message{Result: jsonv2.Value(`{}`)}
			})
			var observed *HelperCleanupError
			ctx := WithCallLimits(context.Background(), CallLimits{Cleanup: func(err *HelperCleanupError) { observed = err }})
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { _ = recover() }()
				_, _ = callFunctionOnNode[managedCleanupExitValue](ctx, target, &Node{Node: &cdp.Node{NodeID: 1}}, attributeJS, "value")
				t.Error("decoder returned normally")
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("decoder cleanup did not finish")
			}
			if calls.Load() != 1 || observed == nil || !errors.Is(observed, cause) {
				t.Fatal("abnormal decoder exit discarded cleanup evidence", observed)
			}
		})
	}
}

func TestManagedReadinessBoundsConcurrentChecksBeforeCommandAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const count = 32768
		frame := &Frame{Nodes: make(map[cdp.NodeID]*Node, count)}
		ids := make([]cdp.NodeID, count)
		for index := range ids {
			id := cdp.NodeID(index + 1)
			ids[index] = id
			frame.Nodes[id] = &Node{Node: &cdp.Node{NodeID: id}}
		}
		release := make(chan struct{})
		var checks atomic.Int64
		ctx := WithCallLimits(context.Background(), CallLimits{Before: func(string) error {
			checks.Add(1)
			<-release
			return ErrCommandLimit
		}})
		selector := &Selector{}
		NodeVisible(selector)
		done := make(chan error, 1)
		go func() {
			_, err := selector.wait(ctx, &Target{stopped: make(chan struct{})}, frame, 0, ids...)
			done <- err
		}()
		synctest.Wait()
		if active := checks.Load(); active == 0 || active > 8 {
			t.Errorf("concurrent readiness checks = %d, want 1 through 8", active)
		}
		close(release)
		if err := <-done; !errors.Is(err, ErrCommandLimit) {
			t.Fatal("readiness lost its admission error", err)
		}
	})
}

func TestManagedJSPathCleanupTracksDispatchAndReplySeparately(t *testing.T) {
	for _, mode := range []string{"success", "refused", "lost-reply", "oversized-reply", "exception", "missing-result", "missing-property", "cleanup-failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var group string
			var evaluated, released atomic.Int64
			cleanupFailure := &cdproto.Error{Code: -32000, Message: "query cleanup fixture"}
			target := managedReplyTarget(t, func(message *cdproto.Message) *cdproto.Message {
				switch string(message.Method) {
				case runtime.Evaluate.Method:
					evaluated.Add(1)
					var params runtime.EvaluateParams
					if err := jsonv2.Unmarshal(message.Params, &params); err != nil {
						t.Error(err)
					}
					group = params.ObjectGroup
					if group == "" || group == "console" {
						t.Error("JSPath borrowed a shared object group")
					}
					switch mode {
					case "lost-reply":
						cancel()
						return nil
					case "exception":
						return &cdproto.Message{Result: jsonv2.Value(`{"result":{"type":"object","objectId":"exception"},"exceptionDetails":{"text":"fixture exception"}}`)}
					case "missing-result":
						return &cdproto.Message{Result: jsonv2.Value(`{}`)}
					case "missing-property":
						return &cdproto.Message{Result: jsonv2.Value(`{"result":{"type":"object","subtype":"array","objectId":"array"}}`)}
					}
					return &cdproto.Message{Result: jsonv2.Value(`{"result":{"type":"object","subtype":"node","objectId":"node"}}`)}
				case dom.RequestNode.Method:
					return &cdproto.Message{Result: jsonv2.Value(`{"nodeId":1}`)}
				case runtime.GetProperties.Method:
					return &cdproto.Message{Result: jsonv2.Value(`{"result":[null]}`)}
				case runtime.ReleaseObjectGroup.Method:
					released.Add(1)
					var params runtime.ReleaseObjectGroupParams
					if err := jsonv2.Unmarshal(message.Params, &params); err != nil || params.ObjectGroup != group {
						t.Error("query cleanup changed its group", err)
					}
					if mode == "cleanup-failure" {
						return &cdproto.Message{Error: cleanupFailure}
					}
					return &cdproto.Message{Result: jsonv2.Value(`{}`)}
				default:
					t.Error("unexpected query command", message.Method)
					return &cdproto.Message{Result: jsonv2.Value(`{}`)}
				}
			})
			limits := CallLimits{MaxInputBytes: 4096, MaxOutputBytes: 4096}
			if mode == "refused" {
				limits.Before = func(string) error { return ErrCommandLimit }
			}
			if mode == "oversized-reply" {
				limits.MaxOutputBytes = 1
			}
			ids, err := evaluatePath("fixture-expression")(WithCallLimits(ctx, limits), target, &Node{})
			if mode == "refused" {
				if !errors.Is(err, ErrCommandLimit) || evaluated.Load() != 0 || released.Load() != 0 {
					t.Fatal("refused query dispatched cleanup without an admitted producer", err)
				}
				return
			}
			if evaluated.Load() != 1 || released.Load() != 1 {
				t.Fatal("query did not attempt exact group cleanup", err)
			}
			wantCleanup := mode == "lost-reply" || mode == "cleanup-failure"
			if isHelperCleanup(err) != wantCleanup {
				t.Fatal("reply observation and cleanup settlement were conflated", err)
			}
			if mode == "success" {
				if err != nil || len(ids) != 1 || ids[0] != 1 {
					t.Fatal("query lost its node result", err)
				}
			} else if err == nil {
				t.Fatal("query failure disappeared")
			}
			if mode == "oversized-reply" && !errors.Is(err, ErrCommandLimit) || mode == "lost-reply" && !errors.Is(err, context.Canceled) || mode == "cleanup-failure" && !errors.Is(err, cleanupFailure) {
				t.Fatal("query lost its original cause", err)
			}
		})
	}
}

func TestManagedReadinessCancellationCannotCertifyUncheckedNodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		frame := &Frame{Nodes: make(map[cdp.NodeID]*Node)}
		ids := make([]cdp.NodeID, 9)
		for index := range ids {
			id := cdp.NodeID(index + 1)
			ids[index] = id
			frame.Nodes[id] = &Node{Node: &cdp.Node{NodeID: id}}
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		release := make(chan struct{})
		var checks atomic.Int64
		wait := (&Selector{}).waitReady(func(context.Context, *Target, runtime.ExecutionContextID, *Node) error {
			checks.Add(1)
			<-release
			return nil
		})
		done := make(chan error, 1)
		go func() { _, err := wait(ctx, nil, frame, 0, ids...); done <- err }()
		synctest.Wait()
		cancel()
		close(release)
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled readiness checked %d of 9 nodes but returned %v", checks.Load(), err)
		}
	})
}
