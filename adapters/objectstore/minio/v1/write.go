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

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/objectstore/minio/v7"
)

// WriteRequest specifies Size=-1 for unknown input, zero for empty input.
// IfAbsent/MatchETag are mutually exclusive final-object conditions. Metadata
// keys omit x-amz-meta-. Encryption is empty or SSE-S3 over HTTPS.
type WriteRequest struct {
	private
	Key                    string
	Size                   int64
	IfAbsent               bool
	MatchETag, ContentType string
	Metadata               map[string]string
	Encryption             string
}

// CopyRequest selects one full same-source object. Destination conditions are
// explicitly unsupported; source ETag/version observations remain separate.
type CopyRequest struct {
	private
	Source               Address
	Key, MatchETag       string
	IfAbsent             bool
	DestinationMatchETag string
}

func writeRequest(value WriteRequest) native.WriteRequest {
	return native.WriteRequest{Key: value.Key, Size: value.Size, IfAbsent: value.IfAbsent, MatchETag: value.MatchETag, ContentType: value.ContentType, Metadata: value.Metadata, Encryption: value.Encryption}
}

// Put borrows input until public receipt release, never closing it. cleanup
// independently authorizes one abort for known failed multipart allocation.
func (client *Client) Put(ctx, cleanup context.Context, request WriteRequest, input io.Reader) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "put", func(group *family) (*invocation.Receipt[native.Result], error) {
		return group.native.Put(group.lifetime, cleanup, group.rootCorrelation(), writeRequest(request), input)
	})
}

// Copy observes one server-side copy without claiming independent read-back.
func (client *Client) Copy(ctx context.Context, request CopyRequest) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "copy", func(group *family) (*invocation.Receipt[native.Result], error) {
		return group.native.Copy(group.lifetime, group.rootCorrelation(), native.CopyRequest{
			Source: address(request.Source), Key: request.Key, MatchETag: request.MatchETag, IfAbsent: request.IfAbsent, DestinationMatchETag: request.DestinationMatchETag})
	})
}

// Remove retains outcomes for all inputs, including unsubmitted trailing targets.
func (client *Client) Remove(ctx context.Context, targets []Address) (*adapters.Receipt[Result], error) {
	if len(targets) > 1000 {
		return nil, fail(ErrLimit, "remove")
	}
	copied := make([]native.Address, len(targets))
	for index, target := range targets {
		copied[index] = address(target)
	}
	return client.finite(ctx, "remove", func(group *family) (*invocation.Receipt[native.Result], error) {
		return group.native.Remove(group.lifetime, group.rootCorrelation(), copied)
	})
}
