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
	"net/http"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/objectstore/minio/v7"
)

// Method restricts delegation to the three explicitly granted operations.
type Method string

const (
	GET  Method = "GET"
	HEAD Method = "HEAD"
	PUT  Method = "PUT"
)

// SignRequest supports exact versions on GET/HEAD and typed signed conditions.
// Expiry must be positive whole seconds within the frozen source maximum.
type SignRequest struct {
	private
	Method    Method
	Address   Address
	Expiry    time.Duration
	MatchETag string
	IfAbsent  bool
}

// Delegation is a sensitive bearer capability. Explicit extraction is not safe
// diagnostic text. Requested expiry is not credential validity or revocation.
type Delegation struct {
	private
	url     string
	headers http.Header
	method  string
	expiry  time.Duration
	token   bool
}

func (value Delegation) URL() string { return value.url }

func (value Delegation) HeadersCopy() http.Header { return value.headers.Clone() }

func (value Delegation) Method() string { return value.method }

func (value Delegation) Expiry() time.Duration { return value.expiry }

func (value Delegation) StaticToken() bool { return value.token }

// Presign issues a bearer capability, not a read/write acknowledgement. Issuance
// performs no network I/O; Open's readiness request is separate.
func (client *Client) Presign(ctx context.Context, request SignRequest) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "presign", func(group *family) (*invocation.Receipt[native.Result], error) {
		return group.native.Presign(group.lifetime, group.rootCorrelation(), native.SignRequest{
			Method: string(request.Method), Address: address(request.Address), Expiry: request.Expiry, MatchETag: request.MatchETag, IfAbsent: request.IfAbsent})
	})
}
