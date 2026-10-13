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
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/heapprofiler"
	"github.com/chromedp/cdproto/runtime"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/fault"
)

type objectArgument struct{ calls *atomic.Int64 }

func (value objectArgument) MarshalJSON() ([]byte, error) {
	value.calls.Add(1)
	return []byte("1"), nil
}

type retiringObjectArgument struct{ origin *objectOrigin }

func (value retiringObjectArgument) MarshalJSON() ([]byte, error) {
	value.origin.retirement.Add(1)
	return []byte("0"), nil
}

func TestBrowserFencedObjectCallRechecksAfterArgumentCodec(t *testing.T) {
	fixture := bindFixture(t, browserOptions(t), 1)
	id := fault.Correlation{Call: "object-codec-retirement"}
	retained, err := fixture.client.Retain(deadline(t), id)
	if err != nil {
		t.Fatal(err)
	}
	session := retained.Page()
	group, err := session.NewObjectGroup(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	object, err := RunObjectAction(deadline(t), session, group, Evaluate[*runtime.RemoteObject]("({answer:1})"))
	if err != nil {
		t.Fatal(err)
	}
	origin := session.objectOrigin
	object.state.fence = &executionFence{origin: origin, revision: origin.retirement.Load()}
	_, err = object.Call(deadline(t), "function(){globalThis.fenceMutation=true;return 1}", retiringObjectArgument{origin: origin})
	if !errors.Is(err, ErrState) {
		t.Fatal("codec retirement did not revoke the fenced receiver", err)
	}
	unchanged, err := sdk.Run(session.native, sdk.Evaluate[bool]("typeof fenceMutation==='undefined'"))
	if err != nil || !unchanged {
		t.Fatal("post-codec revocation was reported only after dispatching the mutation", err)
	}
	before := session.browserContext.objectCount
	if err := object.Close(deadline(t)); !errors.Is(err, ErrCleanup) || object.Closed() || session.browserContext.objectCount != before {
		t.Fatal("retirement released a potentially reused ID or refunded custody", err)
	}
	if err := group.Close(deadline(t)); err != nil || !object.Closed() {
		t.Fatal("private group cleanup could not settle revoked references", err)
	}
	_ = retained.Close(deadline(t))
	settle(t, fixture, id, retained.Receipt(), ErrState, nil, func(testing.TB, Result) {})
}

func TestBrowserObjectCommandAdmissionPrecedesArgumentAccess(t *testing.T) {
	options := browserOptions(t)
	options.MaxCommands = 1
	fixture := bindFixture(t, options, 1)
	id := fault.Correlation{Call: "object-argument-admission"}
	retained, err := fixture.client.Retain(deadline(t), id)
	if err != nil {
		t.Fatal(err)
	}
	group, err := retained.Page().NewObjectGroup(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := RunObjectAction(deadline(t), retained.Page(), group, Evaluate[*runtime.RemoteObject]("({})"))
	if err != nil {
		t.Fatal(err)
	}
	var access, codecs atomic.Int64
	_, err = CallObjectArguments(deadline(t), receiver, "function(value){return value}", 1, func(int) Argument {
		access.Add(1)
		return ValueArgument(objectArgument{calls: &codecs})
	})
	if !errors.Is(err, ErrLimit) || access.Load() != 0 || codecs.Load() != 0 || retained.Page().data.commands != 1 {
		t.Fatal("argument preparation bypassed command admission", err)
	}
	if err := group.Close(deadline(t)); err != nil || !group.Closed() {
		t.Fatal("argument refusal incorrectly retained unknown native work", err)
	}
	_ = retained.Close(deadline(t))
	settle(t, fixture, id, retained.Receipt(), ErrLimit, nil, func(testing.TB, Result) {})
}

func TestBrowserObjectIdentityAndSharedCapacity(t *testing.T) {
	options := browserOptions(t)
	options.MaxObjects = 2
	f := bindFixture(t, options, 1)
	retained, err := f.client.Retain(deadline(t), fault.Correlation{Call: "object-identity"})
	if err != nil {
		t.Fatal(err)
	}
	page := retained.Page()
	other, err := retained.NewPage(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	group, err := page.NewObjectGroup(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	object, err := RunObjectAction(deadline(t), page, group, Evaluate[*runtime.RemoteObject]("({answer:41})"))
	if err != nil || object == nil || object.NativeValue().ObjectID == "" {
		t.Fatal("remote object not retained", err)
	}
	raw := object.NativeValue()
	copy := *raw
	if _, err := page.Object(&copy); !errors.Is(err, ErrInput) {
		t.Fatal("copied data became authority", err)
	}
	if _, err := other.Object(raw); !errors.Is(err, ErrInput) {
		t.Fatal("foreign page accepted object", err)
	}
	id := raw.ObjectID
	raw.ObjectID = "substituted"
	if _, err := page.Object(raw); !errors.Is(err, ErrInput) {
		t.Fatal("mutable identity became authority", err)
	}
	raw.ObjectID = id
	alias, err := page.Object(raw)
	if err != nil {
		t.Fatal(err)
	}
	answer, err := alias.Call(deadline(t), "function(value){return this.answer+value}", 1)
	if err != nil || string(answer.NativeValue().Value) != "42" {
		t.Fatal("native receiver/value semantics", err)
	}
	var encodes atomic.Int64
	if _, err := alias.Call(deadline(t), "function(value){return value}", objectArgument{calls: &encodes}); !errors.Is(err, ErrLimit) || encodes.Load() != 0 {
		t.Fatal("object capacity checked after argument codec", err)
	}
	otherGroup, err := other.NewObjectGroup(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RunObjectAction(deadline(t), other, otherGroup, Evaluate[*runtime.RemoteObject]("globalThis.objectOverflow=1;({})")); !errors.Is(err, ErrLimit) {
		t.Fatal("object budget not shared", err)
	}
	if err := answer.Close(deadline(t)); err != nil {
		t.Fatal("completed result could not make room for the value probe", err)
	}
	if value, err := RunAction(deadline(t), other, Evaluate[bool]("typeof objectOverflow === 'undefined'")); err != nil || !value {
		t.Fatal("rejected producer executed", err)
	}
	if err := group.Close(deadline(t)); err != nil || !object.Closed() || !answer.Closed() {
		t.Fatal("group did not release aliases", err)
	}
	if err := otherGroup.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := alias.Call(deadline(t), "function(){return 1}"); !errors.Is(err, ErrState) {
		t.Fatal("closed object revived", err)
	}
	_ = retained.Close(deadline(t))
	settle(t, f, fault.Correlation{Call: "object-identity"}, retained.Receipt(), ErrLimit, nil, func(testing.TB, Result) {})
}

func TestBrowserObjectRechecksEpochAfterWaitingForGroup(t *testing.T) {
	f := bindFixture(t, browserOptions(t), 1)
	retained, err := f.client.Retain(deadline(t), fault.Correlation{Call: "object-epoch"})
	if err != nil {
		t.Fatal(err)
	}
	page := retained.Page()
	group, err := page.NewObjectGroup(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	object, err := RunObjectAction(deadline(t), page, group, Evaluate[*runtime.RemoteObject]("({value:1})"))
	if err != nil {
		t.Fatal(err)
	}
	group.state.gate <- struct{}{}
	result := make(chan error, 1)
	go func() { _, err := object.Call(deadline(t), "function(){return this.value}"); result <- err }()
	until := time.Now().Add(time.Second)
	for {
		page.mu.Lock()
		busy := page.busy
		page.mu.Unlock()
		if busy {
			break
		}
		if time.Now().After(until) {
			<-group.state.gate
			t.Fatal("object call did not enter")
		}
		time.Sleep(time.Millisecond)
	}
	if err := page.Navigate(deadline(t), "data:text/html,<h1>new object world</h1>"); err != nil {
		<-group.state.gate
		t.Fatal(err)
	}
	before := page.data.commands
	<-group.state.gate
	if err := <-result; !errors.Is(err, ErrState) {
		t.Fatal("stale receiver was dispatched", err)
	}
	if page.data.commands != before {
		t.Fatal("stale call consumed native command allowance")
	}
	_ = group.Close(deadline(t))
	_ = retained.Close(deadline(t))
	settle(t, f, fault.Correlation{Call: "object-epoch"}, retained.Receipt(), ErrState, nil, func(testing.TB, Result) {})
}

func TestBrowserObjectPollTimeoutHasTerminalProducer(t *testing.T) {
	f := bindFixture(t, browserOptions(t), 1)
	retained, err := f.client.Retain(deadline(t), fault.Correlation{Call: "object-poll-timeout"})
	if err != nil {
		t.Fatal(err)
	}
	group, err := retained.Page().NewObjectGroup(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RunObjectAction(deadline(t), retained.Page(), group, Poll[*runtime.RemoteObject]("false", WithPollingTimeout(10*time.Millisecond))); !errors.Is(err, sdk.ErrPollingTimeout) {
		t.Fatal("native polling timeout changed", err)
	}
	if err := group.Close(deadline(t)); err != nil || !group.Closed() {
		t.Fatal("observed undefined result retained unknown custody", err)
	}
	_ = retained.Close(deadline(t))
	settle(t, f, fault.Correlation{Call: "object-poll-timeout"}, retained.Receipt(), sdk.ErrPollingTimeout, nil, func(testing.TB, Result) {})
}

func TestBrowserObjectCleanupWorkRemainsJoinedAfterDetach(t *testing.T) {
	f := bindFixture(t, browserOptions(t), 1)
	retained, err := f.client.Retain(deadline(t), fault.Correlation{Call: "object-cleanup-join"})
	if err != nil {
		t.Fatal(err)
	}
	group, err := retained.Page().NewObjectGroup(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RunObjectAction(deadline(t), retained.Page(), group, Evaluate[*runtime.RemoteObject]("({value:1})")); err != nil {
		t.Fatal(err)
	}
	group.state.gate <- struct{}{}
	release := sync.OnceFunc(func() { <-group.state.gate })
	defer release()
	cleanup, cancelCleanup := context.WithCancel(context.Background())
	cancelCleanup()
	short, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := retained.close(short, cleanup); err == nil {
		t.Fatal("close unexpectedly joined held cleanup")
	}
	retained.Page().mu.Lock()
	detached := retained.Page().objectSessionDetached
	retained.Page().mu.Unlock()
	if !detached {
		t.Fatal("fixture did not reach confirmed native detach")
	}
	if snapshot, ok := retained.Receipt().Result(); ok && snapshot.Released {
		t.Fatal("context receipt released while object cleanup still held its gate")
	}
	release()
	if err := retained.Close(deadline(t)); err != nil || !group.Closed() {
		t.Fatal("cleanup retry failed after actual work joined", err)
	}
	settle(t, f, fault.Correlation{Call: "object-cleanup-join"}, retained.Receipt(), nil, nil, func(testing.TB, Result) {})
}

func TestBrowserBorrowedObjectUnknownProducerNeedsConfirmedDetach(t *testing.T) {
	host := bindFixture(t, browserOptions(t), 1)
	original, err := host.client.Retain(deadline(t), fault.Correlation{Call: "object-peer"})
	if err != nil {
		t.Fatal(err)
	}
	address, err := host.client.owner.debugURL(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	options := OptionsV1{Name: "object-borrower", RemoteURL: address, MaxObjectGroups: 1}
	f := bindFixture(t, options, 1)
	peer := original.Page()
	retained, err := f.client.Retain(deadline(t), fault.Correlation{Call: "object-borrow"}, SessionOptionsV1{Borrow: true, ContextID: peer.browserID, TargetID: sdk.FromContext(peer.native).Target.TargetID})
	if err != nil {
		t.Fatal(err)
	}
	group, err := retained.Page().NewObjectGroup(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	object, err := RunObjectAction(short, retained.Page(), group, Evaluate[*runtime.RemoteObject](`new Promise(resolve=>{globalThis.finishObject=()=>{const value={answer:19};globalThis.objectWeak=new WeakRef(value);resolve(value)}})`, EvalAwaitPromise()))
	cancel()
	if object == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("canceled native producer fixture", err)
	}
	if err := group.Close(deadline(t)); !errors.Is(err, ErrCleanup) || group.Closed() {
		t.Fatal("early release certified producer completion", err)
	}
	if _, err := retained.Page().NewObjectGroup(deadline(t)); !errors.Is(err, ErrLimit) {
		t.Fatal("unknown custody refunded its group slot", err)
	}
	if err := retained.Close(deadline(t)); err == nil || !group.Closed() {
		t.Fatal("confirmed detach did not settle owned references", err)
	}
	settle(t, f, fault.Correlation{Call: "object-borrow"}, retained.Receipt(), context.DeadlineExceeded, ErrCleanup, func(t testing.TB, result Result) {
		if !result.ContextDetached() || result.ContextReleased() {
			t.Fatal("borrowed target was reported destroyed")
		}
	})
	if _, err := RunAction(deadline(t), peer, Evaluate[sdk.Void](`finishObject();delete globalThis.finishObject;undefined`)); err != nil {
		t.Fatal("peer was not preserved", err)
	}
	for range 3 {
		if _, err := RunAction(deadline(t), peer, Evaluate[int]("0")); err != nil {
			t.Fatal(err)
		}
		if _, err := cdp.Call(deadline(t), sdk.FromContext(peer.native).Target, heapprofiler.CollectGarbage, cdp.Empty{}); err != nil {
			t.Fatal(err)
		}
	}
	if alive, err := RunAction(deadline(t), peer, Evaluate[bool](`objectWeak.deref() !== undefined`)); err != nil || alive {
		t.Fatal("detached session retained a late object", alive, err)
	}
	if err := original.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	settle(t, host, fault.Correlation{Call: "object-peer"}, original.Receipt(), nil, nil, func(testing.TB, Result) {})
}
