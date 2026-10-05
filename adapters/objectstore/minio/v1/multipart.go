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
	"io"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/objectstore/minio/v7"
)

// UploadQuery retains exact opaque markers. Listing is not allocation ownership.
type UploadQuery struct {
	private
	Prefix, KeyMarker, UploadIDMarker string
}

// Upload is observed incomplete-upload identity, not proof of ownership.
type Upload struct {
	private
	Key, ID   string
	Initiated time.Time
}

// Part is copied inspection metadata; no SDK handle or completion authority escapes.
type Part struct {
	private
	PartNumber                                                                        int
	LastModified                                                                      time.Time
	ETag                                                                              string
	Size                                                                              int64
	ChecksumCRC32, ChecksumCRC32C, ChecksumSHA1, ChecksumSHA256, ChecksumCRC64NVME    string
	ChecksumMD5, ChecksumSHA512, ChecksumXXHash64, ChecksumXXHash3, ChecksumXXHash128 string
}

func upload(value Upload) native.Upload {
	return native.Upload{Key: value.Key, ID: value.ID, Initiated: value.Initiated}
}

func (client *Client) ListUploads(ctx context.Context, query UploadQuery) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "list-uploads", func(group *family) (*invocation.Receipt[native.Result], error) {
		return group.native.ListUploads(group.lifetime, group.rootCorrelation(), native.UploadQuery{Prefix: query.Prefix, KeyMarker: query.KeyMarker, UploadIDMarker: query.UploadIDMarker})
	})
}

func (client *Client) ListParts(ctx context.Context, target Upload, after int) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "list-parts", func(group *family) (*invocation.Receipt[native.Result], error) {
		return group.native.ListParts(group.lifetime, group.rootCorrelation(), upload(target), after)
	})
}

// Abort requires caller-established authority for this exact upload; enumeration
// alone does not authorize it. The receipt separates abort ACK from object absence.
func (client *Client) Abort(ctx context.Context, target Upload) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "abort", func(group *family) (*invocation.Receipt[native.Result], error) {
		return group.native.Abort(group.lifetime, group.rootCorrelation(), upload(target))
	})
}

// Multipart owns a serial native upload and its generation. It cannot close the
// source. Concurrent operations reject; parts are consecutive and never replayed.
type Multipart struct {
	private
	group  *family
	native *native.Multipart
}

// BeginMultipart separates setup, session lifetime, operation and cleanup contexts.
// Accepted setup failures return nil session and a retained completed receipt.
// Size must be -1 or positive. Empty objects use Put instead of an unusable session.
func (client *Client) BeginMultipart(ctx, lifetime, cleanup context.Context, request WriteRequest) (*Multipart, *adapters.Receipt[Result], error) {
	var session *Multipart
	receipt, err := client.dispatch(ctx, lifetime, "multipart", func(group *family) {
		setup, stop := joinContexts(ctx, group.lifetime)
		owned, nativeReceipt, err := group.native.BeginMultipart(setup, group.lifetime, cleanup, group.rootCorrelation(), writeRequest(request))
		stop()
		if owned != nil {
			session = &Multipart{group: group, native: owned}
		}
		group.attach(group.call, group.guard, nativeReceipt, err, true, nil)
	})
	return session, receipt, err
}

// Receipt observes finalization and local release without producer authority.
func (session *Multipart) Receipt() *adapters.Receipt[Result] {
	if session == nil || session.group == nil {
		return nil
	}
	return session.group.call.Receipt()
}

// Part borrows input through the child's public release, not just method return.
func (session *Multipart) Part(ctx context.Context, number int, size int64, input io.Reader) (*adapters.Receipt[Result], error) {
	if session == nil || session.native == nil {
		return nil, fail(ErrState, "part")
	}
	return session.group.child(ctx, "part", func(work context.Context, id fault.Correlation) (*invocation.Receipt[native.Result], error) {
		return session.native.Part(work, id, number, size, input)
	})
}

func (session *Multipart) finalize(ctx context.Context, complete bool) (*adapters.Receipt[Result], error) {
	if session == nil || session.native == nil || ctx == nil {
		return nil, fail(ErrState, "finalize")
	}
	group := session.group
	if !group.gate.TryLock() {
		return nil, fail(ErrState, "session-busy")
	}
	var err error
	if complete {
		_, err = session.native.Complete(ctx)
	} else {
		_, err = session.native.Abort(ctx)
	}
	group.gate.Unlock()
	return session.Receipt(), translate(err, "finalize")
}

// Complete consumes the root's already-reserved evidence and work authority.
func (session *Multipart) Complete(ctx context.Context) (*adapters.Receipt[Result], error) {
	return session.finalize(ctx, true)
}

// Abort attempts one exact owned allocation cleanup. Lost completion is not undone.
func (session *Multipart) Abort(ctx context.Context) (*adapters.Receipt[Result], error) {
	return session.finalize(ctx, false)
}

// Close cancels and joins actual work using the original cleanup authorization.
func (session *Multipart) Close(ctx context.Context) error {
	if session == nil || session.native == nil || ctx == nil {
		return fail(ErrInput, "close")
	}
	_ = session.native.Close(ctx)
	value, err := session.Receipt().WaitReleased(ctx)
	if err != nil {
		return err
	}
	return value.Err()
}
