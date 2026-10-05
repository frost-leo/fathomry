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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"maps"
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/minio/minio-go/v7"
)

// Multipart owns one process-local serial upload. Copies share authority.
// Parts must be consecutive from one; replacements and automatic retries reject.
type Multipart struct {
	private
	state *multipartState
}
type multipartState struct {
	mu       sync.Mutex
	client   *Client
	call     *invocation.Call[Result]
	lifetime context.Context
	cancel   context.CancelFunc
	cleanup  context.Context
	request  WriteRequest
	opts     native.PutObjectOptions
	data     resultData
	parts    []native.CompletePart
	hash     hash.Hash
	requests int
	short    bool
	closed   bool
}

// BeginMultipart separates setup, retained lifetime and authorized cleanup.
// Accepted setup failures return a completed receipt, not an effect-free refusal.
func (client *Client) BeginMultipart(ctx, lifetime, cleanup context.Context, id fault.Correlation, request WriteRequest) (*Multipart, *invocation.Receipt[Result], error) {
	if err := client.valid(ctx); err != nil {
		return nil, nil, err
	}
	if cleanup == nil {
		return nil, nil, failure(ErrInput, "multipart")
	}
	opts, err := client.writeOptions(request)
	if err != nil {
		return nil, nil, err
	}
	if request.Size == 0 {
		return nil, nil, failure(ErrInput, "multipart-empty")
	}
	call, live, cancel, err := client.beginOwned(ctx, lifetime, id, "multipart")
	if err != nil {
		return nil, nil, err
	}
	state := &multipartState{client: client, call: call, lifetime: live, cancel: cancel, cleanup: cleanup, request: request, opts: opts, hash: sha256.New()}
	state.request.Metadata = nil
	work, stop, err := client.phase(ctx, live)
	var closeErr error
	if err == nil {
		wire := newExchange(client.owner.settings, call, false)
		capture := &controlResponseCapture{}
		initial := native.PutObjectOptions{ContentType: opts.ContentType, UserMetadata: maps.Clone(opts.UserMetadata), ServerSideEncryption: opts.ServerSideEncryption}
		state.data.transfer.Effect = Unknown
		uploadID, nativeErr := client.owner.native.NewMultipartUpload(context.WithValue(controlledContext(work, wire), controlResponseKey{}, capture), client.owner.settings.Bucket, request.Key, initial)
		err = nativeErr
		if len(capture.pages) != 1 {
			err = errors.Join(err, failure(ErrProtocol, "upload-allocation"))
		} else {
			owned, identityErr := capture.pages[0].allocationIdentity(client.owner.settings.Bucket, request.Key, uploadID)
			if owned {
				state.data.transfer.UploadID = uploadID
			}
			err = errors.Join(err, identityErr)
		}
		wireErr, cleanupErr := wire.finish()
		closeErr = cleanupErr
		state.requests += wire.requests
		err = nativeFailure(ErrWrite, "multipart-begin", work, errors.Join(err, wireErr))
		stop()
	}
	if err != nil || closeErr != nil || live.Err() != nil {
		state.end(err, closeErr, true)
		return nil, call.Receipt(), nil
	}
	session := &Multipart{state: state}
	go func() {
		<-live.Done()
		state.mu.Lock()
		defer state.mu.Unlock()
		if !state.closed {
			state.end(errors.Join(live.Err(), context.Cause(live)), nil, true)
		}
	}()
	return session, call.Receipt(), nil
}

func (session *Multipart) lock() (*multipartState, error) {
	if session == nil || session.state == nil {
		return nil, failure(ErrState, "multipart")
	}
	state := session.state
	if !state.mu.TryLock() {
		return nil, failure(ErrState, "multipart-busy")
	}
	if state.closed {
		state.mu.Unlock()
		return nil, failure(ErrState, "multipart-closed")
	}
	return state, nil
}

// Part borrows input until the child receipt releases, never closing it. A failed
// accepted part poisons the session and triggers one authorized abort; no replay.
func (session *Multipart) Part(ctx context.Context, id fault.Correlation, number int, size int64, input io.Reader) (*invocation.Receipt[Result], error) {
	state, err := session.lock()
	if err != nil {
		return nil, err
	}
	value := state.client.owner.settings
	if ctx == nil || input == nil || number != len(state.parts)+1 || number > value.MaxParts || size < 1 || size > int64(value.PartBytes) ||
		state.short || size > value.MaxTransferBytes-state.data.transfer.Bytes || state.request.Size >= 0 && size > state.request.Size-state.data.transfer.Bytes {
		state.mu.Unlock()
		return nil, failure(ErrInput, "multipart-part")
	}
	call, err := state.client.child(ctx, state.call, id, "multipart-part", true)
	if err != nil {
		state.mu.Unlock()
		return nil, err
	}
	go func() {
		data := &resultData{}
		primary, cleanup := state.part(ctx, call, number, size, input, data)
		if primary != nil || cleanup != nil {
			state.end(primary, cleanup, true)
		}
		state.mu.Unlock()
		call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: primary, Cleanup: cleanup})
	}()
	return call.Receipt(), nil
}
func (state *multipartState) part(ctx context.Context, call *invocation.Call[Result], number int, size int64, input io.Reader, data *resultData) (error, error) {
	work, stop, err := state.client.phase(ctx, state.lifetime)
	if err != nil {
		return err, nil
	}
	defer stop()
	buffer := make([]byte, int(size)+1)
	count, readErr := readInput(work, input, buffer)
	data.transfer.Bytes = int64(count)
	_, _ = state.hash.Write(buffer[:count])
	state.data.transfer.Bytes += int64(count)
	state.data.transfer.SHA256 = hex.EncodeToString(state.hash.Sum(nil))
	_, data.transfer.SHA256 = hashes(buffer[:count])
	if readErr != nil && readErr != io.EOF {
		return nativeFailure(ErrWrite, "part-input", work, readErr), nil
	}
	if int64(count) != size {
		return failure(ErrInput, "part-size"), nil
	}
	if err := work.Err(); err != nil {
		return err, nil
	}
	value := state.client.owner.settings
	wire := newExchange(value, call, false)
	wire.maximum = value.MaxRequests - state.requests
	wire.parent = state.call
	md5sum, sha := hashes(buffer[:count])
	data.transfer.Effect = Unknown
	part, nativeErr := state.client.owner.native.PutObjectPart(controlledContext(work, wire), value.Bucket, state.request.Key, state.data.transfer.UploadID, number, bytes.NewReader(buffer[:count]), size,
		native.PutObjectPartOptions{Md5Base64: md5sum, Sha256Hex: sha, DisableContentSha256: true, SSE: state.opts.ServerSideEncryption})
	wireErr, cleanupErr := wire.finish()
	state.requests += wire.requests
	err = errors.Join(nativeErr, wireErr)
	if err == nil && (part.ETag == "" || !validETag(part.ETag)) {
		err = ErrProtocol
	}
	if err == nil {
		state.parts = append(state.parts, native.CompletePart{PartNumber: number, ETag: part.ETag})
		state.short = size < 5<<20
		state.data.transfer.PartsAcknowledged++
		data.transfer.Effect = Acknowledged
		data.transfer.Complete = true
		data.transfer.PartsAcknowledged = 1
		data.complete = true
	}
	return nativeFailure(ErrWrite, "multipart-part", work, err), nativeFailure(ErrCleanup, "multipart-part", work, cleanupErr)
}

// Complete consumes existing root responsibility, including at evidence saturation.
// Once attempted it is never replayed; failed completion may coexist with an object.
func (session *Multipart) Complete(ctx context.Context) (*invocation.Receipt[Result], error) {
	state, err := session.lock()
	if err != nil {
		return nil, err
	}
	if ctx == nil || len(state.parts) == 0 || state.request.Size >= 0 && state.request.Size != state.data.transfer.Bytes {
		state.mu.Unlock()
		return nil, failure(ErrInput, "multipart-complete")
	}
	work, stop, err := state.client.phase(ctx, state.lifetime)
	if err != nil {
		state.mu.Unlock()
		return nil, err
	}
	go func() {
		defer stop()
		value := state.client.owner.settings
		wire := newExchange(value, state.call, false)
		wire.maximum = value.MaxRequests - state.requests
		state.data.transfer.CompletionAttempted = true
		capture := &controlResponseCapture{}
		info, primary := state.client.owner.native.CompleteMultipartUpload(context.WithValue(controlledContext(work, wire), controlResponseKey{}, capture), value.Bucket, state.request.Key, state.data.transfer.UploadID, state.parts, state.opts)
		primary = errors.Join(primary, completionResponseFailure(capture))
		wireErr, cleanupErr := wire.finish()
		state.requests += wire.requests
		primary = errors.Join(primary, wireErr)
		if primary == nil && (info.Bucket != value.Bucket || info.Key != state.request.Key || info.ETag == "" || !validETag(info.ETag)) {
			primary = ErrProtocol
		}
		if primary == nil {
			info.Size = state.data.transfer.Bytes
			state.data.object, state.data.objectPresent = uploadInfo(info), true
			state.data.transfer.Effect, state.data.transfer.Complete, state.data.complete = Acknowledged, true, true
		}
		state.end(nativeFailure(ErrWrite, "multipart-complete", work, primary), cleanupErr, primary != nil)
		state.mu.Unlock()
	}()
	return state.call.Receipt(), nil
}

// Abort joins local release after one attempt under the separately supplied
// context. It does not assert absence of a final object or retry failed cleanup.
func (session *Multipart) Abort(ctx context.Context) (*invocation.Receipt[Result], error) {
	state, err := session.lock()
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		state.mu.Unlock()
		return nil, failure(ErrInput, "multipart-abort")
	}
	state.cleanup = ctx
	go func() { state.end(nil, nil, true); state.mu.Unlock() }()
	return state.call.Receipt(), nil
}

// Close cancels retained work and waits for local release. Waiting cancellation
// cannot release a blocked Reader. Automatic abort uses BeginMultipart's context.
func (session *Multipart) Close(ctx context.Context) error {
	if session == nil || session.state == nil || ctx == nil {
		return failure(ErrInput, "multipart-close")
	}
	state := session.state
	state.cancel()
	result, err := state.call.Receipt().WaitReleased(ctx)
	if err != nil {
		return err
	}
	return result.Err()
}

func (state *multipartState) end(primary, cleanup error, abort bool) {
	if state.closed {
		return
	}
	state.closed = true
	if abort && state.data.transfer.UploadID != "" {
		work, cancel, err := (invocation.Budget{Limit: state.client.owner.settings.CleanupTimeout}).Context(state.cleanup, invocation.Cleanup)
		if err == nil {
			wire := newExchange(state.client.owner.settings, state.call, false)
			wire.maximum = 1
			state.data.transfer.AbortAttempted = true
			err = state.client.owner.native.AbortMultipartUpload(context.WithValue(controlledContext(work, wire), cleanupKey{}, true), state.client.owner.settings.Bucket, state.request.Key, state.data.transfer.UploadID)
			state.data.transfer.AbortAcknowledged = err == nil
			wireErr, closeErr := wire.finish()
			err = errors.Join(err, wireErr, closeErr)
			cancel()
		}
		cleanup = errors.Join(cleanup, nativeFailure(ErrCleanup, "multipart-abort", state.cleanup, err))
	}
	data := state.data
	state.parts = nil
	state.hash = nil
	state.call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: &data}, Primary: primary, Cleanup: cleanup})
	state.cancel()
}
