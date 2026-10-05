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

package minio

import (
	"context"
	"errors"
	"iter"
	"net/url"
	"strings"
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/minio/minio-go/v7"
)

// Cursor retains contextual SDK continuation, including same-key version IDs.
// It is process-local, serial, and not a snapshot or a durable restart token.
type Cursor struct {
	private
	state *cursorState
}
type cursorState struct {
	mu       sync.Mutex
	client   *Client
	call     *invocation.Call[Result]
	lifetime context.Context
	cancel   context.CancelFunc
	wire     *exchange
	next     func() (native.ObjectInfo, bool)
	stop     func()
	request  ListRequest
	listing  *objectListing
	closed   bool
}

// Enumerate creates a retained iterator without fetching a page. Each Next has
// its own operation deadline. MaxRequests and MaxResponseBytes are cumulative
// cursor-wide ceilings; MaxEntries bounds each result. Exhaustion is not EOF.
func (client *Client) Enumerate(ctx, lifetime context.Context, id fault.Correlation, request ListRequest) (*Cursor, *invocation.Receipt[Result], error) {
	if err := client.valid(ctx); err != nil {
		return nil, nil, err
	}
	if err := client.prefix(request.Prefix); err != nil {
		return nil, nil, err
	}
	if request.StartAfter != "" {
		if err := client.address(Address{Key: request.StartAfter}, false); err != nil {
			return nil, nil, err
		}
	}
	if request.Versions && !client.owner.settings.Versions {
		return nil, nil, failure(ErrUnsupported, "versions")
	}
	call, live, cancel, err := client.beginOwned(ctx, lifetime, id, "enumerate")
	if err != nil {
		return nil, nil, err
	}
	state := &cursorState{client: client, call: call, lifetime: live, cancel: cancel, request: request,
		listing: &objectListing{bucket: client.owner.settings.Bucket, request: request, seen: make(map[string]bool)}}
	state.wire = newExchange(client.owner.settings, call, false)
	capture := &controlResponseCapture{check: state.listing.checkPage}
	owner := false
	state.next, state.stop = iter.Pull(client.owner.native.ListObjectsIter(context.WithValue(controlledContext(live, state.wire), controlResponseKey{}, capture), client.owner.settings.Bucket,
		native.ListObjectsOptions{Prefix: request.Prefix, StartAfter: request.StartAfter, Recursive: true, WithVersions: request.Versions, MaxKeys: client.owner.settings.MaxEntries, FetchOwner: &owner}))
	cursor := &Cursor{state: state}
	go func() {
		<-live.Done()
		state.mu.Lock()
		defer state.mu.Unlock()
		if !state.closed {
			state.end(errors.Join(live.Err(), context.Cause(live)), false)
		}
	}()
	return cursor, call.Receipt(), nil
}

func decodeKey(value, encoding string) (string, error) {
	if encoding == "" {
		return value, nil
	}
	if encoding != "url" {
		return "", failure(ErrProtocol, "list-encoding")
	}
	return url.QueryUnescape(value)
}

// Next returns at most MaxEntries copied observations. Canceling an admitted
// fetch terminates the cursor, since the SDK owns one retained contextual iterator.
func (cursor *Cursor) Next(ctx context.Context, id fault.Correlation) (*invocation.Receipt[Result], error) {
	if cursor == nil || cursor.state == nil || ctx == nil {
		return nil, failure(ErrInput, "cursor-next")
	}
	state := cursor.state
	if !state.mu.TryLock() {
		return nil, failure(ErrState, "cursor-busy")
	}
	if state.closed {
		state.mu.Unlock()
		return nil, failure(ErrState, "cursor-closed")
	}
	call, err := state.client.child(ctx, state.call, id, "enumerate-next", false)
	if err != nil {
		state.mu.Unlock()
		return nil, err
	}
	go func() {
		data := &resultData{}
		work, stop, primary := state.client.phase(ctx, state.lifetime)
		if primary == nil {
			stopped := make(chan struct{})
			cancelFetch := context.AfterFunc(work, func() { defer close(stopped); state.cancel() })
			for len(data.objects) < state.client.owner.settings.MaxEntries {
				info, ok := state.next()
				if !ok {
					data.complete = work.Err() == nil && state.lifetime.Err() == nil
					break
				}
				if info.Err != nil {
					primary = info.Err
					break
				}
				if !validPath(info.Key, false) || !strings.HasPrefix(info.Key, state.request.Prefix) || info.Key <= state.request.StartAfter || info.Size < 0 || !validText(info.VersionID, 1024, !state.request.Versions) {
					primary = failure(ErrProtocol, "cursor-object")
					break
				}
				data.objects = append(data.objects, objectInfo(info))
			}
			if !cancelFetch() {
				<-stopped
			}
			primary = errors.Join(primary, work.Err(), context.Cause(work), state.lifetime.Err(), context.Cause(state.lifetime))
			stop()
		}
		if primary != nil || data.complete {
			if primary != nil {
				data.complete = false
			}
			state.end(primary, data.complete)
		}
		state.mu.Unlock()
		call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: nativeFailure(ErrList, "enumerate-next", ctx, primary)})
	}()
	return call.Receipt(), nil
}
func (state *cursorState) end(primary error, complete bool) {
	if state.closed {
		return
	}
	state.closed = true
	state.stop()
	wireErr, cleanup := state.wire.finish()
	state.listing.seen = nil
	state.next, state.stop = nil, nil
	state.call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: &resultData{complete: complete && primary == nil && wireErr == nil}}, Primary: nativeFailure(ErrList, "enumerate", state.lifetime, errors.Join(primary, wireErr)), Cleanup: cleanup})
	state.cancel()
}

// Close requests stop, joins the actual iterator and HTTP bodies, and retains
// final evidence using the root slot. ctx controls only waiting.
func (cursor *Cursor) Close(ctx context.Context) error {
	if cursor == nil || cursor.state == nil || ctx == nil {
		return failure(ErrInput, "cursor-close")
	}
	state := cursor.state
	state.cancel()
	value, err := state.call.Receipt().WaitReleased(ctx)
	if err != nil {
		return err
	}
	return value.Err()
}
