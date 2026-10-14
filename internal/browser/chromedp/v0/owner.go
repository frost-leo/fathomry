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
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/browser"
	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/runtime"
	"github.com/chromedp/cdproto/systeminfo"
	"github.com/chromedp/cdproto/target"
	sdk "github.com/chromedp/chromedp"
	"github.com/chromedp/chromedp/remote"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

// bareContext deliberately removes native/executor and caller value capabilities.
type bareContext struct{ parent context.Context }

func (bareContext) Value(any) any                   { return nil }
func (ctx bareContext) Deadline() (time.Time, bool) { return ctx.parent.Deadline() }
func (ctx bareContext) Done() <-chan struct{}       { return ctx.parent.Done() }
func (ctx bareContext) Err() error                  { return ctx.parent.Err() }

type owner struct {
	native                            NativeOptionsV1
	abort                             context.CancelCauseFunc
	settings                          settings
	life                              context.Context
	cancel                            context.CancelFunc
	gate                              chan struct{}
	mu                                sync.Mutex
	sessions                          map[*browserContext]struct{}
	pendingObjects                    map[*objectGroupState]struct{}
	cleanupControls                   map[*controlState]struct{}
	trace                             *traceState
	transferred                       bool
	started, closed, connectionClosed bool
	startErr                          error
	root                              context.Context
	closeRoot, closeAllocator         context.CancelFunc
	allocator                         sdk.Allocator
	process                           *exec.Cmd
	processDone                       chan struct{}
	processErr                        error
	profileDir                        string
	profileLock                       *os.File
	launchArguments                   []string
	version                           browserVersion
	commands                          uint64
	resultBytes                       int64
}
type browserContext struct {
	browserID               cdp.BrowserContextID
	observedID              cdp.BrowserContextID
	defaultContext          bool
	defaultIdentityObserved bool
	creationAttempted       bool
	borrowed                bool
	sharedStorage           bool
	sharedPermissions       bool
	sharedDownloads         bool
	unknownTargets          int
	ownedTargets            map[target.ID]struct{}
	detachUncertain         bool
	streams                 map[*streamState]struct{}
	subscriptions           map[*subscriptionState]struct{}
	bindings                map[*managedBinding]struct{}
	bindingCalls            int
	objectGroups            map[*objectGroupState]struct{}
	objectRefs              map[*runtime.RemoteObject]*objectState
	objectCount             int
}

type browserVersion struct{ protocol, product, revision, js string }

func (own *owner) lock(ctx context.Context) error {
	select {
	case own.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return errors.Join(ctx.Err(), context.Cause(ctx))
	}
}
func (own *owner) unlock() { <-own.gate }

func (own *owner) start(ctx context.Context) error {
	if err := own.lock(ctx); err != nil {
		return err
	}
	defer own.unlock()
	if own.closed {
		return failure(ErrState, "closed")
	}
	if err := own.life.Err(); err != nil {
		return failure(ErrState, "lifetime", err, context.Cause(own.life))
	}
	if own.started {
		if own.startErr == nil && own.root != nil && own.root.Err() != nil {
			return failure(ErrDisconnected, "connection", own.root.Err(), context.Cause(own.root))
		}
		return own.startErr
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(err, context.Cause(ctx))
	}
	own.started = true
	work, cancel, err := (invocation.Budget{Limit: own.settings.StartupTimeout}).Context(ctx, invocation.Establish)
	if err != nil {
		own.startErr = err
		return err
	}
	defer cancel()
	err = own.initialize(work)
	if err == nil {
		err = work.Err()
	}
	if err != nil {
		own.startErr = failure(ErrNative, "startup", err, work.Err(), context.Cause(work))
	}
	return own.startErr
}

func (own *owner) initialize(ctx context.Context) error {
	address := own.settings.RemoteURL
	if address == "" {
		var err error
		if own.settings.ProfileDir != "" {
			own.profileDir = own.settings.ProfileDir
			own.profileLock, err = os.OpenFile(filepath.Join(own.profileDir, ".fathomry-browser.lock"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		} else {
			own.profileDir, err = os.MkdirTemp(own.settings.TempDir, "fathomry-cdp-")
		}
		if err != nil {
			return err
		}
		args := own.arguments()
		own.mu.Lock()
		own.launchArguments = append([]string(nil), args...)
		own.mu.Unlock()
		own.process = exec.Command(own.settings.ExecPath, args...)
		own.process.Env = append([]string{}, own.settings.Environment...)
		if own.settings.InheritEnvironment {
			own.process.Env = append(os.Environ(), own.process.Env...)
		}
		own.process.Stdout, own.process.Stderr = io.Discard, io.Discard
		if err := own.process.Start(); err != nil {
			return err
		}
		own.processDone = make(chan struct{})
		stopProcess := context.AfterFunc(own.life, func() {
			own.mu.Lock()
			defer own.mu.Unlock()
			if !own.transferred {
				_ = own.process.Process.Kill()
			}
		})
		go func() {
			own.processErr = own.process.Wait()
			stopProcess()
			own.cancel()
			close(own.processDone)
		}()
		address, err = own.debugURL(ctx)
		if err != nil {
			return err
		}
	}
	allocator, closeAllocator := remote.NewAllocator(own.life, address, remote.NoModifyURL, remote.WithDialTimeout(own.settings.StartupTimeout), remote.WithDialHTTPHeader(own.settings.Headers), remote.WithDialTLSConfig(own.native.TLS))
	own.closeAllocator = closeAllocator
	own.allocator = sdk.FromContext(allocator).Allocator
	own.root, own.closeRoot = sdk.NewContext(allocator, sdk.WithBrowserOption(sdk.WithBrowserLogf(func(string, ...any) {}), sdk.WithBrowserErrorf(func(string, ...any) {}), sdk.WithEventBuffer(sdk.EventBuffer{MaxEvents: own.settings.MaxEvents, MaxBytes: own.settings.MaxEventBytes, OnOverflow: func(err error) { own.abort(failure(ErrLimit, "native-events", err)) }})))
	// Cancel only this owned connection on failed startup, never the borrowed process.
	abort := context.AfterFunc(ctx, func() { closeAllocator() })
	defer abort()
	version, err := sdk.CallBrowser(own.root, browser.GetVersion, cdp.Empty{})
	if err := ctx.Err(); err != nil {
		return errors.Join(err, context.Cause(ctx))
	}
	if err != nil {
		return err
	}
	if own.process != nil {
		processes, err := sdk.CallBrowser(own.root, systeminfo.GetProcessInfo, cdp.Empty{})
		if err != nil {
			return err
		}
		matched := false
		for _, process := range processes.ProcessInfo {
			if process.Type == "browser" && process.ID == int64(own.process.Process.Pid) {
				matched = true
			}
		}
		if !matched {
			return failure(ErrState, "process-identity")
		}
	}
	own.mu.Lock()
	own.version = browserVersion{protocol: version.ProtocolVersion, product: version.Product, revision: version.Revision, js: version.JsVersion}
	own.mu.Unlock()
	return nil
}
func (own *owner) arguments() []string {
	names := make([]string, 0, len(own.settings.Flags))
	for name := range own.settings.Flags {
		names = append(names, name)
	}
	slices.Sort(names)
	args := make([]string, 0, len(names)+4)
	for _, name := range names {
		arg := "--" + name
		if value := own.settings.Flags[name]; value != "" {
			arg += "=" + value
		}
		args = append(args, arg)
	}
	return append(args, "--user-data-dir="+own.profileDir, "--remote-debugging-address=127.0.0.1", "--remote-debugging-port=0", "about:blank")
}
func (own *owner) debugURL(ctx context.Context) (string, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		file, err := os.Open(filepath.Join(own.profileDir, "DevToolsActivePort"))
		if err == nil {
			data, readErr := io.ReadAll(io.LimitReader(file, 8193))
			closeErr := file.Close()
			if readErr != nil || closeErr != nil {
				return "", errors.Join(readErr, closeErr)
			}
			lines := strings.Split(strings.TrimSpace(string(data)), "\n")
			if len(data) <= 8192 && len(lines) == 2 {
				port, err := strconv.Atoi(lines[0])
				if err == nil && port > 0 && port <= 65535 && strings.HasPrefix(lines[1], "/devtools/browser/") {
					return "ws://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(port)) + lines[1], nil
				}
			}
			if len(data) > 8192 {
				return "", failure(ErrLimit, "debug-address")
			}
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		select {
		case <-ctx.Done():
			return "", errors.Join(ctx.Err(), context.Cause(ctx))
		case <-own.processDone:
			return "", failure(ErrNative, "process-exit", own.processErr)
		case <-ticker.C:
		}
	}
}

func (own *owner) reserve(session *Session, maximum int) error {
	own.mu.Lock()
	defer own.mu.Unlock()
	if own.transferred {
		return failure(ErrState, "source-transferred")
	}
	pendingContexts := make(map[*browserContext]struct{})
	for control := range own.cleanupControls {
		if _, retained := own.sessions[control.session.browserContext]; !retained {
			pendingContexts[control.session.browserContext] = struct{}{}
		}
	}
	if len(own.sessions)+len(pendingContexts) >= min(own.settings.MaxSessions, maximum) {
		return failure(ErrLimit, "browser-contexts", resource.ErrCapacity)
	}
	own.sessions[session.browserContext] = struct{}{}
	return nil
}
func (own *owner) forget(session *browserContext) {
	own.mu.Lock()
	delete(own.sessions, session)
	own.mu.Unlock()
}

// dispose is also used by explicit assembly cleanup for retained native contexts.
// A canceled command is not evidence that the context was never created.
func (own *owner) dispose(ctx context.Context, session *browserContext) (bool, error) {
	return own.disposeWith(ctx, session, sdk.FromContext(own.root).Browser)
}

func (own *owner) disposeWith(ctx context.Context, session *browserContext, native *sdk.Browser) (bool, error) {
	if own.processDone != nil {
		select {
		case <-own.processDone:
			own.forget(session)
			return true, nil
		default:
		}
	}
	if session.borrowed {
		return own.disposeBorrowed(ctx, session, native)
	}
	if !session.creationAttempted {
		own.forget(session)
		return true, nil
	}
	if session.browserID == "" {
		return false, failure(ErrCleanup, "unknown-context")
	}
	contexts, err := cdp.Call(ctx, native, target.GetBrowserContexts, cdp.Empty{})
	if err != nil {
		return false, err
	}
	if !slices.Contains(contexts.BrowserContextIDs, session.browserID) {
		own.forget(session)
		return true, nil
	}
	if _, err := cdp.Call(ctx, native, target.DisposeBrowserContext, target.DisposeBrowserContextParams{BrowserContextID: session.browserID}); err != nil {
		return false, err
	}
	// The protocol response acknowledges disposal, not reversal of HTTP effects.
	own.forget(session)
	return true, nil
}

func (own *owner) release(ctx context.Context) resource.ReleaseResult {
	work, cancel, err := (invocation.Budget{Limit: own.settings.CleanupTimeout}).Context(ctx, invocation.Cleanup)
	if err != nil {
		return resource.ReleaseResult{Err: err, Continue: own.release}
	}
	defer cancel()
	ctx = work
	if err := own.lock(ctx); err != nil {
		return resource.ReleaseResult{Err: err, Continue: own.release}
	}
	defer own.unlock()
	own.closed = true
	if err := ctx.Err(); err != nil {
		return resource.ReleaseResult{Err: errors.Join(err, context.Cause(ctx)), Continue: own.release}
	}
	own.mu.Lock()
	pending := make([]*browserContext, 0, len(own.sessions))
	for session := range own.sessions {
		pending = append(pending, session)
	}
	own.mu.Unlock()
	var native *sdk.Browser
	if own.processDone != nil {
		select {
		case <-own.processDone:
			for _, record := range pending {
				own.forget(record)
			}
			pending = nil
		default:
		}
	}
	if own.root != nil {
		native = sdk.FromContext(own.root).Browser
	}
	for _, record := range pending {
		if record.detachUncertain && own.root != nil && !own.connectionClosed {
			own.closeRoot()
			own.closeAllocator()
			own.allocator.Wait()
			if err := native.WaitStopped(ctx); err != nil {
				return resource.ReleaseResult{Err: err, Continue: own.release}
			}
			own.connectionClosed = true
		}
	}
	if len(pending) != 0 && native != nil && own.settings.RemoteURL != "" {
		select {
		case <-native.LostConnection:
			var finish func()
			native, finish, err = own.reconcileConnection(ctx)
			if err != nil {
				return resource.ReleaseResult{Err: err, Continue: own.release}
			}
			defer finish()
		default:
		}
	}
	var failures []error
	for _, session := range pending {
		released, err := own.disposeWith(ctx, session, native)
		if err != nil {
			failures = append(failures, failure(ErrCleanup, "dispose", err))
		}
		if !released && own.settings.RemoteURL != "" {
			return resource.ReleaseResult{Err: errors.Join(failures...), Continue: own.release}
		}
	}
	// Process termination must not wait behind a blocked CDP socket writer.
	own.mu.Lock()
	trace := own.trace
	own.mu.Unlock()
	if trace != nil {
		if err := (&Trace{state: trace}).Close(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	if own.processDone != nil && !own.transferred {
		select {
		case <-own.processDone:
		default:
			if err := own.process.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
				failures = append(failures, err)
			}
			select {
			case <-own.processDone:
			case <-ctx.Done():
				if err := own.process.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
					failures = append(failures, err)
				}
				return resource.ReleaseResult{Err: errors.Join(append(failures, ctx.Err(), context.Cause(ctx))...), Continue: own.release}
			}
		}
	}
	if !own.connectionClosed {
		if own.closeRoot != nil {
			own.closeRoot()
			own.closeAllocator()
			own.allocator.Wait()
			if native := sdk.FromContext(own.root); native.Browser != nil {
				select {
				case <-native.Browser.LostConnection:
				case <-ctx.Done():
					return resource.ReleaseResult{Err: errors.Join(append(failures, ctx.Err(), context.Cause(ctx))...), Continue: own.release}
				}
				if err := native.Browser.WaitStopped(ctx); err != nil {
					return resource.ReleaseResult{Err: errors.Join(append(failures, err)...), Continue: own.release}
				}
			}
		}
		own.connectionClosed = true
	}
	own.cancel()
	own.mu.Lock()
	controls := make([]*controlState, 0, len(own.cleanupControls))
	for control := range own.cleanupControls {
		controls = append(controls, control)
	}
	own.mu.Unlock()
	for _, state := range controls {
		control := &Control{state: state}
		cleanup := control.Close(ctx)
		if !control.Closed() {
			return resource.ReleaseResult{Err: errors.Join(append(failures, cleanup)...), Continue: own.release}
		}
		failures = append(failures, cleanup)
	}
	if trace != nil {
		value := &Trace{state: trace}
		cleanup := value.Close(ctx)
		if !value.Closed() {
			return resource.ReleaseResult{Err: errors.Join(append(failures, cleanup)...), Continue: own.release}
		}
		failures = append(failures, cleanup)
	}
	own.mu.Lock()
	objects := make([]*objectGroupState, 0, len(own.pendingObjects))
	for group := range own.pendingObjects {
		objects = append(objects, group)
	}
	own.mu.Unlock()
	for _, state := range objects {
		group := &ObjectGroup{state: state}
		cleanup := group.Close(ctx)
		if !group.Closed() {
			return resource.ReleaseResult{Err: errors.Join(append(failures, cleanup)...), Continue: own.release}
		}
		failures = append(failures, cleanup)
		own.mu.Lock()
		delete(own.pendingObjects, state)
		own.mu.Unlock()
	}
	if !own.transferred {
		if err := own.cleanProfile(); err != nil {
			return resource.ReleaseResult{Quiescent: true, Err: errors.Join(append(failures, err)...), Continue: own.release}
		}
	}
	own.mu.Lock()
	clear(own.sessions)
	own.mu.Unlock()
	return resource.ReleaseResult{Quiescent: true, Released: true, Err: errors.Join(failures...)}
}

// Caller holds the source/process gate and has observed actual process exit.
func (own *owner) cleanProfile() error {
	if own.profileLock != nil {
		info, statErr := own.profileLock.Stat()
		path := filepath.Join(own.profileDir, ".fathomry-browser.lock")
		current, pathErr := os.Lstat(path)
		if statErr != nil || pathErr != nil || !os.SameFile(info, current) {
			return failure(ErrCleanup, "profile-lock", statErr, pathErr)
		}
		if err := os.Remove(path); err != nil {
			return err
		}
		if err := own.profileLock.Close(); err != nil {
			own.profileLock = nil
			return err
		}
		own.profileLock = nil
	}
	if own.profileDir != "" && own.settings.ProfileDir == "" {
		if err := os.RemoveAll(own.profileDir); err != nil {
			return err
		}
	}
	return nil
}

// A fresh cleanup-only connection can reconcile our known IDs after a borrowed
// connection is lost. It creates no targets/contexts and never closes the browser.
func (own *owner) reconcileConnection(ctx context.Context) (*sdk.Browser, func(), error) {
	allocatorContext, closeAllocator := remote.NewAllocator(bareContext{ctx}, own.settings.RemoteURL, remote.NoModifyURL, remote.WithDialTimeout(own.settings.CleanupTimeout), remote.WithDialHTTPHeader(own.settings.Headers), remote.WithDialTLSConfig(own.native.TLS))
	root, closeRoot := sdk.NewContext(allocatorContext, sdk.WithBrowserOption(sdk.WithBrowserLogf(func(string, ...any) {}), sdk.WithBrowserErrorf(func(string, ...any) {})))
	allocator := sdk.FromContext(allocatorContext).Allocator
	finish := func() {
		closeRoot()
		closeAllocator()
		allocator.Wait()
	}
	_, err := sdk.CallBrowser(root, browser.GetVersion, cdp.Empty{})
	if err != nil {
		finish()
		return nil, nil, err
	}
	native := sdk.FromContext(root).Browser
	return native, func() { finish(); <-native.LostConnection }, nil
}

func (own *owner) versionCopy() browserVersion {
	own.mu.Lock()
	defer own.mu.Unlock()
	return own.version
}
