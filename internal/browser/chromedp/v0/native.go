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

package chromedp

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"

	"github.com/chromedp/cdproto/cdp"
	sdk "github.com/chromedp/chromedp"
	jsonv2 "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
)

// Actions runs executor-only chromedp Actions (Evaluate, input and typed CDP).
// Native Run/ListenTarget/selector/navigation helpers needing FromContext are
// unsupported; use Navigate, CDP DOM and NextEvent instead. No owning SDK context
// is passed. Outputs belong to caller pointers; Save separately copies evidence.
//
// Technical restrictions permit target-scoped Runtime, DOM, CSS, Input,
// Accessibility, Animation, Performance, Profiler, Debugger, DOMSnapshot,
// DOMStorage, Emulation, Page, Network and Fetch commands. Browser/Target/Storage,
// global mutation, downloads, IO streams and lifecycle escape commands are refused.
// This is capability restriction, not a business URL/profile allowlist.
//
// Actions/custom serializers must be synchronous, bounded, and return immutable
// errors without owning capabilities. This is not an untrusted-Go sandbox.
func (session *Session) Actions(ctx context.Context, actions ...sdk.Action) error {
	work, leave, err := session.enter(ctx, false)
	if err != nil {
		session.remember(err)
		return err
	}
	defer leave()
	if len(actions) == 0 || len(actions) > session.owner.settings.MaxCommands {
		err = failure(ErrInput, "actions")
		session.remember(err)
		return err
	}
	grant := &executorGrant{session: session, work: work}
	actionContext := cdp.WithExecutor(bareContext{work}, grant)
	err = invoke(func() error {
		for _, action := range actions {
			if action == nil || nilLike(action) {
				return failure(ErrInput, "nil-action")
			}
			if err := work.Err(); err != nil {
				return errors.Join(err, context.Cause(work))
			}
			if err := action.Do(actionContext); err != nil {
				return err
			}
		}
		return nil
	})
	grant.closeAndJoin()
	err = errors.Join(err, ctx.Err(), context.Cause(ctx), work.Err(), context.Cause(work))
	if err != nil {
		err = failure(ErrNative, "actions", err, work.Err(), context.Cause(work))
	}
	session.remember(err)
	return err
}
func nilLike(value any) bool {
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Pointer, reflect.Func, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan:
		return reflected.IsNil()
	}
	return false
}

type executorGrant struct {
	private
	session        *Session
	work           context.Context
	mu             sync.Mutex
	closed, active bool
	idle           chan struct{}
}

func (grant *executorGrant) closeAndJoin() {
	grant.mu.Lock()
	grant.closed = true
	active, idle := grant.active, grant.idle
	grant.mu.Unlock()
	if active {
		<-idle
	}
}
func (grant *executorGrant) Execute(ctx context.Context, method string, params, result any) (err error) {
	grant.mu.Lock()
	if grant.closed || grant.active || ctx == nil {
		grant.mu.Unlock()
		return failure(ErrState, "executor")
	}
	grant.active = true
	grant.idle = make(chan struct{})
	grant.mu.Unlock()
	defer func() {
		err = errors.Join(err, ctx.Err(), context.Cause(ctx), grant.work.Err(), context.Cause(grant.work))
		grant.session.remember(err)
		grant.mu.Lock()
		grant.active = false
		close(grant.idle)
		grant.mu.Unlock()
	}()
	if err := grant.work.Err(); err != nil {
		return failure(ErrState, "executor", err, context.Cause(grant.work))
	}
	if err := ctx.Err(); err != nil {
		return failure(ErrState, "executor", err, context.Cause(ctx))
	}
	if !permitted(method) {
		return failure(ErrUnsupported, "command")
	}
	value := grant.session.owner.settings
	encoded := boundedBuffer{limit: value.MaxCommandBytes}
	if params != nil {
		if err := jsonv2.MarshalWrite(&encoded, params, sdk.DefaultMarshalOptions); err != nil {
			return err
		}
	}
	if method == "Page.printToPDF" && len(encoded.data) != 0 {
		var options struct {
			TransferMode string `json:"transferMode"`
		}
		if err := jsonv2.Unmarshal(encoded.data, &options); err != nil {
			return failure(ErrInput, "pdf-options", err)
		}
		if options.TransferMode != "" && options.TransferMode != "ReturnAsBase64" {
			return failure(ErrUnsupported, "native-stream")
		}
	}
	session := grant.session
	session.mu.Lock()
	if session.closed || session.data.commands >= uint64(value.MaxCommands) {
		session.mu.Unlock()
		return failure(ErrLimit, "commands")
	}
	session.data.commands++
	session.mu.Unlock()
	work, cancel := context.WithCancelCause(grant.work)
	stop := context.AfterFunc(ctx, func() { cancel(context.Cause(ctx)) })
	defer stop()
	defer cancel(context.Canceled)
	var raw jsontext.Value
	var input any
	if params != nil {
		input = jsontext.Value(encoded.data)
	}
	// The SDK has already read the WebSocket message. This checks output before
	// caller decoding, not native wire-reader allocation or total process memory.
	err = sdk.FromContext(session.native).Target.Execute(work, method, input, &raw)
	if err != nil {
		return failure(ErrNative, "command", err, work.Err(), context.Cause(work))
	}
	session.mu.Lock()
	if int64(len(raw)) > value.MaxResultBytes-session.delivered {
		session.mu.Unlock()
		err = failure(ErrLimit, "command-output")
		session.cancel(err)
		return err
	}
	session.delivered += int64(len(raw))
	session.mu.Unlock()
	if result != nil {
		return jsonv2.Unmarshal(raw, result, sdk.DefaultUnmarshalOptions)
	}
	return nil
}

func permitted(method string) bool {
	domain, command, ok := strings.Cut(method, ".")
	if !ok || command == "" || len(method) > 128 {
		return false
	}
	switch method {
	case "Page.close", "Page.crash", "Page.setDownloadBehavior", "Network.clearBrowserCookies", "Network.clearBrowserCache", "Fetch.takeResponseBodyAsStream",
		"Network.takeResponseBodyForInterceptionAsStream", "Network.loadNetworkResource",
		"Emulation.getScreenInfos", "Emulation.addScreen", "Emulation.updateScreen", "Emulation.removeScreen", "Emulation.setPrimaryScreen":
		return false
	}
	switch domain {
	case "Runtime", "DOM", "CSS", "Input", "Accessibility", "Animation", "Performance", "Profiler", "Debugger", "DOMSnapshot", "DOMStorage", "Emulation", "Page", "Network", "Fetch":
		return true
	}
	return false
}
