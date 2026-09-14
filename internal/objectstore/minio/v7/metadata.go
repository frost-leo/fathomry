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
	"maps"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/tags"
)

// GetTags returns copied tags; missing/denied is not an empty successful tag set.
func (client *Client) GetTags(ctx context.Context, id fault.Correlation, address Address) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx); err != nil {
		return nil, err
	}
	if err := client.address(address, false); err != nil {
		return nil, err
	}
	if !client.owner.settings.Tags {
		return nil, failure(ErrUnsupported, "tags")
	}
	return client.start(ctx, id, "get-tags", false, func(work context.Context, state *exchange, data *resultData) (error, error) {
		found, err := client.owner.native.GetObjectTagging(work, client.owner.settings.Bucket, address.Key, native.GetObjectTaggingOptions{VersionID: address.VersionID})
		if err == nil && found == nil {
			err = ErrProtocol
		}
		if err != nil {
			return nativeFailure(ErrRead, "tags", work, err), nil
		}
		data.tags = maps.Clone(found.ToMap())
		data.complete = true
		return nil, nil
	})
}

// SetTags atomically replaces the native tag set for the selected target,
// not its content. Empty deletes all tags using the native removal operation.
// No retention, legal hold, policy or permission mutation is authorized.
func (client *Client) SetTags(ctx context.Context, id fault.Correlation, address Address, values map[string]string) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx); err != nil {
		return nil, err
	}
	if err := client.address(address, true); err != nil {
		return nil, err
	}
	if !client.owner.settings.Tags {
		return nil, failure(ErrUnsupported, "tags")
	}
	if len(values) > 10 {
		return nil, failure(ErrLimit, "tags")
	}
	for key, value := range values {
		if !validText(key, 128, false) || !validText(value, 256, true) {
			return nil, failure(ErrInput, "tags")
		}
	}
	frozen, err := tags.NewTags(maps.Clone(values), true)
	if err != nil {
		return nil, failure(ErrInput, "tags", err)
	}
	empty := len(values) == 0
	return client.start(ctx, id, "set-tags", false, func(work context.Context, state *exchange, data *resultData) (error, error) {
		data.transfer.Effect = Unknown
		var err error
		if empty {
			err = client.owner.native.RemoveObjectTagging(work, client.owner.settings.Bucket, address.Key, native.RemoveObjectTaggingOptions{VersionID: address.VersionID})
		} else {
			err = client.owner.native.PutObjectTagging(work, client.owner.settings.Bucket, address.Key, frozen, native.PutObjectTaggingOptions{VersionID: address.VersionID})
		}
		if err == nil {
			data.transfer.Effect = Acknowledged
			data.complete = true
		}
		return nativeFailure(ErrWrite, "tags", work, err), nil
	})
}
