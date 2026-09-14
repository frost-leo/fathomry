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

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/minio/minio-go/v7"
)

// Remove freezes a bounded target list and submits individual DELETEs serially.
// It retains one result for EVERY input, including denied, unknown and trailing
// unsubmitted entries. There is no bulk-delete silence, governance bypass or
// implicit listing/deletion of additional keys or historical versions.
func (client *Client) Remove(ctx context.Context, id fault.Correlation, addresses []Address) (*invocation.Receipt[Result], error) {
	if err := client.valid(ctx); err != nil {
		return nil, err
	}
	if len(addresses) == 0 || len(addresses) > client.owner.settings.MaxEntries {
		return nil, failure(ErrLimit, "remove-targets")
	}
	for _, address := range addresses {
		if err := client.address(address, true); err != nil {
			return nil, err
		}
	}
	frozen := append([]Address{}, addresses...)
	return client.start(ctx, id, "remove", false, func(work context.Context, state *exchange, data *resultData) (error, error) {
		data.removals = make([]Removal, len(frozen))
		var failures []error
		for index, address := range frozen {
			removal := Removal{Address: address}
			err := work.Err()
			if err == nil {
				before := state.requests
				state.mu.Lock()
				state.lastHeader = nil
				state.mu.Unlock()
				err = client.owner.native.RemoveObject(work, client.owner.settings.Bucket, address.Key, native.RemoveObjectOptions{VersionID: address.VersionID})
				if state.requests > before {
					removal.Effect = Unknown
				}
				if err == nil {
					removal.Effect = Acknowledged
				}
				header := state.header()
				removal.DeleteMarker = header.Get("X-Amz-Delete-Marker") == "true"
				removal.DeleteMarkerVersionID = header.Get("X-Amz-Version-Id")
			}
			removal.Err = nativeFailure(ErrRemove, "target", work, err)
			data.removals[index] = removal
			if removal.Err != nil {
				failures = append(failures, removal.Err)
			}
		}
		primary := errors.Join(failures...)
		data.complete = primary == nil
		return primary, nil
	})
}
