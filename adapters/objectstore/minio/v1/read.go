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

// ReadRequest selects full or exact bounded range consumption. Length zero
// requires Offset zero. ExpectedSHA256 is lowercase hex over the requested bytes.
type ReadRequest struct {
	private
	Address                   Address
	Offset, Length            int64
	MatchETag, ExpectedSHA256 string
}

func readRequest(value ReadRequest) native.ReadRequest {
	return native.ReadRequest{Address: address(value.Address), Offset: value.Offset, Length: value.Length, MatchETag: value.MatchETag, ExpectedSHA256: value.ExpectedSHA256}
}

// Stat admits an asynchronous metadata observation.
func (client *Client) Stat(ctx context.Context, target Address) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "stat", func(group *family) (*invocation.Receipt[native.Result], error) {
		return group.native.Stat(group.lifetime, group.rootCorrelation(), address(target))
	})
}

// Read retains bounded full/partial payload and verification facts independently.
func (client *Client) Read(ctx context.Context, request ReadRequest) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "read", func(group *family) (*invocation.Receipt[native.Result], error) {
		return group.native.Read(group.lifetime, group.rootCorrelation(), readRequest(request))
	})
}

// Download borrows output until public receipt release; it never closes it.
// Waiting cancellation does not establish that the external sink was untouched.
func (client *Client) Download(ctx context.Context, request ReadRequest, output io.Writer) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "download", func(group *family) (*invocation.Receipt[native.Result], error) {
		return group.native.Download(group.lifetime, group.rootCorrelation(), readRequest(request), output)
	})
}
