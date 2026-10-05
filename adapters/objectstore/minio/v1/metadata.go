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

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/objectstore/minio/v7"
)

func (client *Client) GetTags(ctx context.Context, target Address) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "get-tags", func(group *family) (*invocation.Receipt[native.Result], error) {
		return group.native.GetTags(group.lifetime, group.rootCorrelation(), address(target))
	})
}

// SetTags replaces tags; empty deletes the tag set, not object data.
func (client *Client) SetTags(ctx context.Context, target Address, tags map[string]string) (*adapters.Receipt[Result], error) {
	return client.finite(ctx, "set-tags", func(group *family) (*invocation.Receipt[native.Result], error) {
		return group.native.SetTags(group.lifetime, group.rootCorrelation(), address(target), tags)
	})
}
