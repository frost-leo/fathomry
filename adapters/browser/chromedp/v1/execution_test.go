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
	"testing"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
)

func executionValue(t *testing.T, object *Object, expected string, err error) {
	t.Helper()
	if err != nil || object == nil || object.native.NativeValue() == nil || string(object.native.NativeValue().Value) != expected {
		t.Fatal("unexpected execution result", err)
	}
}

func TestLiveExecutionContextsIncludeInitialWorldAndPreserveIsolatedSemantics(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<p>execution world</p>", 128)
	target := retained.Page()
	origins, receipt, err := target.ExecutionContexts(bounded(t))
	if err != nil || len(origins) != 1 {
		t.Fatal("initial context was not available without an event replay", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	value, present := snapshot.ValueCopy()
	if !present || !bytes.Contains(value.PayloadCopy(), []byte("uniqueId")) {
		t.Fatal("identity evidence missing")
	}
	var original sdk.ExecutionOrigin
	if err := json.Unmarshal(origins[0].DataCopy(), &original); err != nil || original.ID == 0 || original.UniqueID == "" {
		t.Fatal("native identity missing", err)
	}
	drain(t, dependencies, 1)
	frames, _, err := target.Frames(bounded(t))
	if err != nil || len(frames) != 1 {
		t.Fatal("frame tree missing", err)
	}
	drain(t, dependencies, 1)
	params := page.CreateIsolatedWorldParams{WorldName: "managed-world", GrantUniveralAccess: new(false)}
	isolated, _, err := target.CreateWorld(bounded(t), frames[0], params)
	if err != nil || isolated == nil {
		t.Fatal("native isolated world was not created", err)
	}
	if params.FrameID != "" || *params.GrantUniveralAccess {
		t.Fatal("world parameters changed")
	}
	drain(t, dependencies, 1)
	repeated, _, err := target.CreateWorld(bounded(t), frames[0], params)
	if err != nil || !bytes.Equal(repeated.DataCopy(), isolated.DataCopy()) {
		t.Fatal("existing named world did not reuse its native identity", err)
	}
	drain(t, dependencies, 1)
	group, _, err := target.NewObjectGroup(retained.Context())
	if err != nil {
		t.Fatal(err)
	}
	input := runtime.EvaluateParams{Expression: "globalThis.marker = 41", ReturnByValue: new(true)}
	answer, _, err := group.EvaluateIn(bounded(t), isolated, input)
	executionValue(t, answer, "41", err)
	if input.ContextID != 0 || input.UniqueContextID != "" || input.ObjectGroup != "" {
		t.Fatal("evaluation mutated caller identity")
	}
	drain(t, dependencies, 1)
	answer, _, err = group.EvaluateIn(bounded(t), origins[0], runtime.EvaluateParams{Expression: "typeof globalThis.marker", ReturnByValue: new(true)})
	executionValue(t, answer, `"undefined"`, err)
	drain(t, dependencies, 1)
	answer, _, err = group.EvaluateIn(bounded(t), isolated, runtime.EvaluateParams{Expression: "Promise.resolve({value:42})", AwaitPromise: new(true)})
	if err != nil || answer == nil || answer.native.NativeValue().ObjectID == "" {
		t.Fatal("promise reference not retained", err)
	}
	drain(t, dependencies, 1)
	called, _, err := answer.Call(bounded(t), "function(){ return this.value }")
	executionValue(t, called, "42", err)
	drain(t, dependencies, 1)
	for _, expression := range []string{"null", "undefined"} {
		object, _, err := group.EvaluateIn(bounded(t), isolated, runtime.EvaluateParams{Expression: expression, ReturnByValue: new(true)})
		if err != nil || object == nil {
			t.Fatal(err)
		}
		var result runtime.RemoteObject
		if err := json.Unmarshal(object.DataCopy(), &result); err != nil {
			t.Fatal(err)
		}
		if expression == "null" && result.Subtype != runtime.RemoteObjectSubtypeNull || expression == "undefined" && result.Type != runtime.RemoteObjectTypeUndefined {
			t.Fatal("null and undefined conflated")
		}
		drain(t, dependencies, 1)
	}
	_, _, err = group.EvaluateIn(bounded(t), isolated, runtime.EvaluateParams{Expression: "throw new Error('synthetic world exception')"})
	var exception *sdk.ExceptionError
	if !errors.As(err, &exception) {
		t.Fatal("native exception cause lost", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() || !group.Closed() {
		t.Fatal("execution references retained native ownership")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveFrameExecutionRetiresChildReferencesWithoutRevokingRoot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		if r.URL.Path == "/" {
			_, _ = w.Write([]byte("<iframe src='/old'></iframe>"))
			return
		}
		_, _ = w.Write([]byte("<p>" + r.URL.Path + "</p>"))
	}))
	defer server.Close()
	owner, retained, dependencies := controlFixture(t, server.URL, 256)
	target := retained.Page()
	frames, _, err := target.Frames(bounded(t))
	if err != nil || len(frames) != 2 {
		t.Fatal("native child frame absent", err)
	}
	drain(t, dependencies, 1)
	world, _, err := target.CreateWorld(bounded(t), frames[1], page.CreateIsolatedWorldParams{WorldName: "child-world"})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	group, _, err := target.NewObjectGroup(retained.Context())
	if err != nil {
		t.Fatal(err)
	}
	child, _, err := group.EvaluateIn(bounded(t), world, runtime.EvaluateParams{Expression: "({nested:{value:73}})"})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	properties, _, err := child.Properties(bounded(t), runtime.GetPropertiesParams{OwnProperties: new(true)})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	var reference *runtime.RemoteObject
	for _, property := range properties.Result {
		if property.Name == "nested" {
			reference = property.Value
		}
	}
	nested, err := target.Object(reference)
	if err != nil {
		t.Fatal(err)
	}
	called, _, err := nested.Call(bounded(t), "function(){return this.value}")
	executionValue(t, called, "73", err)
	drain(t, dependencies, 1)
	root, _, err := group.Evaluate(bounded(t), "({root:17})")
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), target, Evaluate[bool]("document.querySelector('iframe').src='/new';true")); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), target, Poll[bool]("document.querySelector('iframe').contentDocument?.body?.textContent.includes('/new')")); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	for _, object := range []*Object{child, nested} {
		if _, _, err := object.Call(bounded(t), "function(){return 1}"); !errors.Is(err, ErrState) {
			t.Fatal("retired execution reference reached native dispatch", err)
		}
		drain(t, dependencies, 1)
	}
	if _, _, err := target.CreateWorld(bounded(t), frames[1], page.CreateIsolatedWorldParams{WorldName: "stale"}); !errors.Is(err, ErrState) {
		t.Fatal("departed frame document accepted world creation", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := group.EvaluateIn(bounded(t), world, runtime.EvaluateParams{Expression: "globalThis.stale=1"}); !errors.Is(err, ErrState) {
		t.Fatal("retired world fell back to default", err)
	}
	drain(t, dependencies, 1)
	rootValue, _, err := root.Call(bounded(t), "function(){return this.root}")
	executionValue(t, rootValue, "17", err)
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("frame world prevented native cleanup")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveExecutionRejectsForeignAndBareIdentityBeforeDispatch(t *testing.T) {
	owner, retained, dependencies := controlFixture(t, "data:text/html,<p>origin grants</p>", 128)
	target := retained.Page()
	origins, _, err := target.ExecutionContexts(bounded(t))
	if err != nil || len(origins) == 0 {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	frames, _, err := target.Frames(bounded(t))
	if err != nil || len(frames) == 0 {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	other, _, err := retained.NewPage(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	group, _, err := other.NewObjectGroup(retained.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, execution := range []*ExecutionContext{nil, {}, origins[0]} {
		if _, _, err := group.EvaluateIn(bounded(t), execution, runtime.EvaluateParams{Expression: "globalThis.foreign=1"}); !errors.Is(err, ErrInput) {
			t.Fatal("foreign or zero context became authority", err)
		}
		drain(t, dependencies, 1)
	}
	if _, _, err := other.CreateWorld(bounded(t), frames[0], page.CreateIsolatedWorldParams{}); !errors.Is(err, ErrInput) {
		t.Fatal("foreign frame became authority", err)
	}
	drain(t, dependencies, 1)
	own, _, err := target.NewObjectGroup(retained.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, params := range []runtime.EvaluateParams{
		{Expression: "1", ContextID: runtime.ExecutionContextID(-1)},
		{Expression: "1", UniqueContextID: "not-issued"},
		{Expression: "1", ObjectGroup: "caller-selected"},
	} {
		if _, _, err := own.EvaluateIn(bounded(t), origins[0], params); !errors.Is(err, ErrInput) {
			t.Fatal("raw context/group identifier changed scope", err)
		}
		drain(t, dependencies, 1)
	}
	if _, _, err := target.CreateWorld(bounded(t), frames[0], page.CreateIsolatedWorldParams{FrameID: cdp.FrameID("foreign")}); !errors.Is(err, ErrInput) {
		t.Fatal("raw frame ID changed scope", err)
	}
	drain(t, dependencies, 1)
	if _, err := target.Navigate(bounded(t), "data:text/html,<p>next document</p>"); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	_, _, err = own.EvaluateIn(bounded(t), origins[0], runtime.EvaluateParams{Expression: "1"})
	var nativeError *cdproto.Error
	if !errors.Is(err, ErrState) || errors.As(err, &nativeError) {
		t.Fatal("stale identity reached native evaluation", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("refused execution prevented cleanup")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestExecutionZeroFacades(t *testing.T) {
	if (&ExecutionContext{}).DataCopy() != nil || (&Frame{}).DataCopy() != nil {
		t.Fatal("zero grant contains native data")
	}
	var page *Page
	if _, _, err := page.Frames(context.Background()); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	if _, _, err := page.ExecutionContexts(context.Background()); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
	var group *ObjectGroup
	if _, _, err := group.EvaluateIn(context.Background(), nil, runtime.EvaluateParams{}); !errors.Is(err, ErrInput) {
		t.Fatal(err)
	}
}

func TestLiveFramePollingKeepsNativeWorldArgsAndRevokesRunningWork(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		if r.URL.Path == "/" {
			_, _ = w.Write([]byte("<script>globalThis.pollMarker=11</script><iframe src='/child'></iframe>"))
			return
		}
		_, _ = w.Write([]byte("<script>globalThis.pollMarker=23</script><p>child</p>"))
	}))
	defer server.Close()
	owner, retained, dependencies := controlFixture(t, server.URL, 256)
	target := retained.Page()
	nodes, _, err := QueryNodes(bounded(t), target, sdk.CSS("iframe"))
	if err != nil || len(nodes) != 1 {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	frameNode := nodes[0]
	value, _, err := RunAction(bounded(t), target, PollFunction[int]("(addition)=>globalThis.pollMarker+addition", WithPollingInFrame(frameNode), WithPollingArgs(2)))
	if err != nil || value != 25 {
		t.Fatal("native frame polling or arguments changed", err)
	}
	drain(t, dependencies, 1)
	value, _, err = RunAction(bounded(t), target, Poll[int]("globalThis.pollMarker", WithPollingInFrame(frameNode), WithPollingInFrame(nil)))
	if err != nil || value != 11 {
		t.Fatal("last nil frame option lost native default-world semantics", err)
	}
	drain(t, dependencies, 1)
	frames, _, err := target.Frames(bounded(t))
	if err != nil || len(frames) != 2 {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := target.CreateWorld(bounded(t), frames[1], page.CreateIsolatedWorldParams{WorldName: "must-not-replace-poll"}); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	value, _, err = RunAction(bounded(t), target, Poll[int]("globalThis.pollMarker", WithPollingInFrame(frameNode)))
	if err != nil || value != 23 {
		t.Fatal("isolated world changed the native default frame", err)
	}
	drain(t, dependencies, 1)
	other, _, err := retained.NewPage(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), other, Poll[bool]("true", WithPollingInFrame(frameNode))); !errors.Is(err, ErrInput) {
		t.Fatal("foreign node entered frame polling", err)
	}
	drain(t, dependencies, 1)
	if _, _, err := RunAction(bounded(t), target, Poll[bool]("true", WithPollingInFrame(&Node{}))); !errors.Is(err, ErrInput) {
		t.Fatal("zero node became default frame", err)
	}
	drain(t, dependencies, 1)
	entered := make(chan struct{})
	binding, _, err := BindFunc(retained.Context(), target, "pollStarted", func(context.Context, bool) (bool, error) { close(entered); return true, nil })
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := RunAction(bounded(t), target, Poll[bool]("(globalThis.reported || (globalThis.reported=true, window.pollStarted(true)), false)", WithPollingInFrame(frameNode), WithPollingTimeout(0)))
		done <- err
	}()
	select {
	case <-entered:
	case <-bounded(t).Done():
		t.Fatal("native frame predicate did not execute")
	}
	// Navigation has its own lane and must revoke an active poll's old document.
	if _, err := target.Navigate(bounded(t), "data:text/html,<p>replacement root</p>"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("retired frame poll completed successfully")
		}
	case <-bounded(t).Done():
		t.Fatal("retired frame poll continued indefinitely")
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() || !binding.Closed() {
		t.Fatal("frame poll or binding retained cleanup")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

type framePollingCodec struct {
	entered chan struct{}
	release chan struct{}
	once    *sync.Once
}

func (value framePollingCodec) MarshalJSON() ([]byte, error) {
	value.once.Do(func() { close(value.entered) })
	<-value.release
	return []byte("1"), nil
}

func TestLiveFramePollingRevocationPrecedesBlockedArgumentDispatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		if r.URL.Path == "/" {
			_, _ = w.Write([]byte("<iframe src='/old'></iframe>"))
			return
		}
		_, _ = w.Write([]byte("<p>" + r.URL.Path + "</p>"))
	}))
	defer server.Close()
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	if err := sdk.Do(peer, sdk.Navigate(server.URL)); err != nil {
		t.Fatal(err)
	}
	nodes, _, err := QueryNodes(bounded(t), retained.Page(), sdk.CSS("iframe"))
	if err != nil || len(nodes) != 1 {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	entered, release := make(chan struct{}), make(chan struct{})
	releaseCodec := sync.OnceFunc(func() { close(release) })
	defer releaseCodec()
	result := make(chan error, 1)
	go func() {
		_, _, err := RunAction(bounded(t), retained.Page(), PollFunction[bool]("()=>{globalThis.invalidPollEffect=1;return true}", WithPollingInFrame(nodes[0]), WithPollingArgs(framePollingCodec{entered: entered, release: release, once: &sync.Once{}})))
		result <- err
	}()
	select {
	case <-entered:
	case <-bounded(t).Done():
		t.Fatal("argument codec did not start")
	}
	if _, err := sdk.Run(peer, sdk.Evaluate[bool]("document.querySelector('iframe').src='/new';true")); err != nil {
		t.Fatal(err)
	}
	if _, err := sdk.Run(peer, sdk.Poll[bool]("document.querySelector('iframe').contentDocument?.body?.textContent.includes('/new')")); err != nil {
		t.Fatal(err)
	}
	if retained.Context().Err() != nil {
		t.Fatal("frame revocation canceled the whole retained context")
	}
	releaseCodec()
	select {
	case err := <-result:
		if !errors.Is(err, ErrState) {
			t.Fatal("known frame revocation did not reach the caller", err)
		}
	case <-bounded(t).Done():
		t.Fatal("native frame poll did not join the returned codec")
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[bool]("typeof document.querySelector('iframe').contentWindow.invalidPollEffect === 'undefined'")); err != nil || !value {
		t.Fatal("revoked predicate reached the new document", err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() {
		t.Fatal("borrowed frame polling cleanup did not detach")
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int]("23")); err != nil || value != 23 {
		t.Fatal("borrowed peer was terminated", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveBorrowedWorldLifetimeIsNotMisreportedAsDisposal(t *testing.T) {
	owner, retained, peer, dependencies := borrowedControlFixture(t)
	frames, _, err := retained.Page().Frames(bounded(t))
	if err != nil || len(frames) != 1 {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	world, _, err := retained.Page().CreateWorld(bounded(t), frames[0], page.CreateIsolatedWorldParams{WorldName: "borrowed-page-world"})
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	group, _, err := retained.Page().NewObjectGroup(retained.Context())
	if err != nil {
		t.Fatal(err)
	}
	value, _, err := group.EvaluateIn(bounded(t), world, runtime.EvaluateParams{Expression: "globalThis.worldValue=61", ReturnByValue: new(true)})
	executionValue(t, value, "61", err)
	drain(t, dependencies, 1)
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() || !group.Closed() {
		t.Fatal("borrowed execution cleanup", err)
	}
	snapshot, err := retained.call.Receipt().WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	evidence, _ := snapshot.ValueCopy()
	if !evidence.Observation().ContextDetached || evidence.Observation().ContextDisposed {
		t.Fatal("detachment claimed document/world destruction")
	}
	var origin sdk.ExecutionOrigin
	if err := json.Unmarshal(world.DataCopy(), &origin); err != nil {
		t.Fatal(err)
	}
	reply, err := cdp.Call(bounded(t), sdk.FromContext(peer).Target, runtime.Evaluate, runtime.EvaluateParams{UniqueContextID: origin.UniqueID, Expression: "globalThis.worldValue", ReturnByValue: new(true)})
	if err != nil || reply.Result == nil || string(reply.Result.Value) != "61" {
		t.Fatal("native borrowed world lifetime changed", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
