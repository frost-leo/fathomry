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

package kafka

import (
	"context"

	native "github.com/frost-leo/fathomry/internal/broker/franz/v1"
)

// Topic is copied metadata, not an authorization or retention guarantee.
type Topic struct {
	private
	Name       string
	ID         [16]byte
	Partitions int
}

func (value Result) TopicsCopy() []Topic {
	input := value.native.TopicsCopy()
	if input == nil {
		return nil
	}
	output := make([]Topic, len(input))
	for index, item := range input {
		output[index] = Topic{Name: item.Name, ID: item.ID, Partitions: item.Partitions}
	}
	return output
}

// Metadata verifies the frozen cluster/topic identities on the native path.
func (client *Client) Metadata(ctx context.Context) (Result, error) {
	return result(client.dispatch(ctx, "metadata", func(op *operation, bound *native.Client) {
		receipt, err := bound.Metadata(op.lifetime, op.correlation())
		op.finish(receipt, err)
	}))
}
