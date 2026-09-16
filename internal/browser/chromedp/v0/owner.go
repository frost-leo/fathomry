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
	"github.com/chromedp/cdproto/target"
	sdk "github.com/chromedp/chromedp"
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
	settings                          settings
	life                              context.Context
	cancel                            context.CancelFunc
	gate                              chan struct{}
	mu                                sync.Mutex
	sessions                          map[*browserContext]struct{}
	started, closed, connectionClosed bool
	startErr                          error
	root                              context.Context
	closeRoot, closeAllocator         context.CancelFunc
	allocator                         sdk.Allocator
	process                           *exec.Cmd
	processDone                       chan struct{}
	processErr                        error
	profileDir                        string
	launchArguments                   []string
	version                           browserVersion
}
type browserContext struct {
	browserID         cdp.BrowserContextID
	creationAttempted bool
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
		own.profileDir, err = os.MkdirTemp(own.settings.TempDir, "fathomry-cdp-")
		if err != nil {
			return err
		}
		args := own.arguments()
		own.mu.Lock()
		own.launchArguments = append([]string(nil), args...)
		own.mu.Unlock()
		own.process = exec.CommandContext(own.life, own.settings.ExecPath, args...)
		own.process.Stdout, own.process.Stderr = io.Discard, io.Discard
		if err := own.process.Start(); err != nil {
			return err
		}
		own.processDone = make(chan struct{})
		go func() {
			own.processErr = own.process.Wait()
			own.cancel()
			close(own.processDone)
		}()
		address, err = own.debugURL(ctx)
		if err != nil {
			return err
		}
	}
	allocator, closeAllocator := sdk.NewRemoteAllocator(own.life, address, sdk.NoModifyURL)
	own.closeAllocator = closeAllocator
	own.allocator = sdk.FromContext(allocator).Allocator
	own.root, own.closeRoot = sdk.NewContext(allocator)
	// Cancel only this owned connection on failed startup, never the borrowed process.
	abort := context.AfterFunc(ctx, func() { closeAllocator() })
	defer abort()
	native, err := own.allocator.Allocate(own.root, sdk.WithBrowserLogf(func(string, ...any) {}), sdk.WithBrowserErrorf(func(string, ...any) {}), sdk.WithDialTimeout(own.settings.StartupTimeout))
	if err != nil {
		return err
	}
	sdk.FromContext(own.root).Browser = native
	if err := ctx.Err(); err != nil {
		return errors.Join(err, context.Cause(ctx))
	}
	executor := cdp.WithExecutor(ctx, sdk.FromContext(own.root).Browser)
	protocol, product, revision, _, js, err := browser.GetVersion().Do(executor)
	if err != nil {
		return err
	}
	own.mu.Lock()
	own.version = browserVersion{protocol: protocol, product: product, revision: revision, js: js}
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
	if len(own.sessions) >= min(own.settings.MaxSessions, maximum) {
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
	if !session.creationAttempted {
		own.forget(session)
		return true, nil
	}
	if session.browserID == "" {
		return false, failure(ErrCleanup, "unknown-context")
	}
	executor := cdp.WithExecutor(ctx, native)
	ids, _, err := target.GetBrowserContexts().Do(executor)
	if err != nil {
		return false, err
	}
	if !slices.Contains(ids, session.browserID) {
		own.forget(session)
		return true, nil
	}
	if err := target.DisposeBrowserContext(session.browserID).Do(executor); err != nil {
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
	if own.processDone != nil {
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
			}
		}
		own.connectionClosed = true
	}
	own.cancel()
	if own.profileDir != "" {
		if err := os.RemoveAll(own.profileDir); err != nil {
			return resource.ReleaseResult{Quiescent: true, Err: errors.Join(append(failures, err)...), Continue: own.release}
		}
	}
	own.mu.Lock()
	clear(own.sessions)
	own.mu.Unlock()
	return resource.ReleaseResult{Quiescent: true, Released: true, Err: errors.Join(failures...)}
}

// A fresh cleanup-only connection can reconcile our known IDs after a borrowed
// connection is lost. It creates no targets/contexts and never closes the browser.
func (own *owner) reconcileConnection(ctx context.Context) (*sdk.Browser, func(), error) {
	allocatorContext, closeAllocator := sdk.NewRemoteAllocator(bareContext{ctx}, own.settings.RemoteURL, sdk.NoModifyURL)
	root, closeRoot := sdk.NewContext(allocatorContext)
	allocator := sdk.FromContext(allocatorContext).Allocator
	finish := func() {
		closeRoot()
		closeAllocator()
		allocator.Wait()
	}
	native, err := allocator.Allocate(root, sdk.WithBrowserLogf(func(string, ...any) {}), sdk.WithBrowserErrorf(func(string, ...any) {}), sdk.WithDialTimeout(own.settings.CleanupTimeout))
	if err != nil {
		finish()
		return nil, nil, err
	}
	sdk.FromContext(root).Browser = native
	return native, func() { finish(); <-native.LostConnection }, nil
}

func (own *owner) versionCopy() browserVersion {
	own.mu.Lock()
	defer own.mu.Unlock()
	return own.version
}
