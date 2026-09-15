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

package httpcloak

import (
	"context"
	"errors"
	"sync"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	"github.com/sardanioss/httpcloak/fingerprint"
	"github.com/sardanioss/httpcloak/transport"
)

type Source struct {
	private
	owner *owner
}
type Client struct {
	private
	owner    *owner
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}
type owner struct {
	held           map[*binding]struct{}
	retiring       int
	settings       settings
	native         NativeOptionsV1
	mu             sync.Mutex
	bindings       []*binding
	sockets        map[*trackedConn]struct{}
	connections    int
	stopping       bool
	cleanup        error
	releaseMu      sync.Mutex
	releaseStarted bool
	done           chan struct{}
}
type binding struct {
	notifyMu                                     sync.Mutex
	notifyDone                                   chan struct{}
	notifyClosed                                 bool
	notifications                                []error
	callbackActive, callbackCount, callbackLimit int
	key                                          string
	native                                       *transport.FathomryTransport
	busy                                         bool
	udpRelease                                   func()
}

// Select freezes externally supplied configuration and native containers without
// performing network I/O or registering a global fingerprint.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	native, err := copyNative(options.Native)
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	version := options.Version
	if version == 0 {
		version = 1
	}
	var preset *fingerprint.Preset
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: func(value settings) error {
		if err := validate(value); err != nil {
			return err
		}
		var err error
		preset, err = choosePreset(value, native)
		return err
	}}, resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: version, Layers: layers})
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return resource.Select(prepared, func(ctx context.Context, value settings) (resource.Resource[Source], error) {
		if err := ctx.Err(); err != nil {
			return resource.Resource[Source]{}, failure(ErrState, "construct", err)
		}
		copied, err := copyNative(native)
		if err != nil {
			return resource.Resource[Source]{}, err
		}
		copied.Preset = fingerprint.Clone(preset)
		own := &owner{settings: value, native: copied, done: make(chan struct{}), sockets: make(map[*trackedConn]struct{})}
		return resource.Resource[Source]{Acquired: true, Capability: Source{owner: own}, Release: own.release}, nil
	}), nil
}

// LimitsV1 supplies process-local admission bounds for unoverridden options.
func LimitsV1(options OptionsV1) (resource.Limits, error) {
	if options.Version != 0 && options.Version != 1 {
		return resource.Limits{}, failure(ErrInput, "version")
	}
	value := defaults(options)
	if err := validate(value); err != nil {
		return resource.Limits{}, err
	}
	native, err := copyNative(options.Native)
	if err != nil {
		return resource.Limits{}, err
	}
	if _, err := choosePreset(value, native); err != nil {
		return resource.Limits{}, err
	}
	return value.limits(), nil
}

// Bind preserves the source's authoritative identity and limits across aliases.
func Bind(assembly *resource.Assembly, selected resource.Selection[Source], inbox *invocation.Inbox[Result], observer *invocation.Observer) (*Client, error) {
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		return nil, err
	}
	access, err := resource.AccessFor(assembly, selected)
	if err != nil {
		return nil, err
	}
	if source.owner == nil || inbox == nil {
		return nil, failure(ErrInput, "bind")
	}
	value, limits := source.owner.settings, access.Limits()
	if limits.Active > value.MaxActive || limits.Queued > value.QueuedCalls || limits.Bytes < value.reservation() || limits.Queued > 0 && limits.QueuedBytes < value.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Client{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}
func (client *Client) EvidenceBytes() int64 {
	if client == nil || client.owner == nil {
		return 0
	}
	return client.owner.settings.evidenceBytes()
}
func (own *owner) acquireConnection() (func(), error) {
	own.mu.Lock()
	defer own.mu.Unlock()
	if own.stopping || own.connections >= own.settings.MaxConnections {
		return nil, failure(ErrLimit, "connections", resource.ErrCapacity)
	}
	own.connections++
	var once sync.Once
	return func() { once.Do(func() { own.mu.Lock(); own.connections--; own.mu.Unlock() }) }, nil
}
func (own *owner) recordCleanup(err error) {
	if err == nil {
		return
	}
	own.mu.Lock()
	defer own.mu.Unlock()
	own.cleanup = errors.Join(own.cleanup, err)
	own.stopping = true
}
func (own *owner) release(ctx context.Context) resource.ReleaseResult {
	own.releaseMu.Lock()
	start := !own.releaseStarted
	if !start {
		select {
		case <-own.done:
			own.mu.Lock()
			start = own.connections != 0
			own.mu.Unlock()
		default:
		}
	}
	if start {
		own.releaseStarted = true
		own.done = make(chan struct{})
		own.mu.Lock()
		own.stopping = true
		bindings := append([]*binding(nil), own.bindings...)
		for current := range own.held {
			found := false
			for _, candidate := range bindings {
				if candidate == current {
					found = true
					break
				}
			}
			if !found {
				bindings = append(bindings, current)
			}
		}
		own.mu.Unlock()
		done := own.done
		go func() {
			for _, current := range bindings {
				own.retire(current)
			}
			own.mu.Lock()
			sockets := make([]*trackedConn, 0, len(own.sockets))
			for conn := range own.sockets {
				sockets = append(sockets, conn)
			}
			own.mu.Unlock()
			for _, conn := range sockets {
				_ = conn.Close()
			}
			close(done)
		}()
	}
	done := own.done
	own.releaseMu.Unlock()
	select {
	case <-done:
		own.mu.Lock()
		defer own.mu.Unlock()
		if own.connections != 0 || own.retiring != 0 {
			return resource.ReleaseResult{Err: own.cleanup, Continue: own.release}
		}
		return resource.ReleaseResult{Quiescent: true, Released: true, Err: own.cleanup}
	case <-ctx.Done():
		return resource.ReleaseResult{Err: errors.Join(ctx.Err(), context.Cause(ctx)), Continue: own.release}
	}
}
