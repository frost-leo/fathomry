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

package nacos

import (
	"context"
	"slices"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	native "github.com/frost-leo/fathomry/internal/configsource/nacos/v2"
)

// Source is a complete original-document profile over a selected Client.
// It retains no application schema, defaults, required/optional or layer policy.
type Source struct {
	private
	client  *Client
	keys    []Key
	options ObserveOptions
}

// Source freezes explicit keys, or selects the client's default set when none are
// supplied. Native permission admission occurs on use. No service I/O happens here.
func (client *Client) Source(options ObserveOptions, keys ...Key) (*Source, error) {
	if client == nil || len(keys) > MaxKeys || options.QueueCapacity < 0 || options.QueueCapacity > 16 {
		return nil, fail(ErrInput, "source")
	}
	return &Source{client: client, keys: slices.Clone(keys), options: options}, nil
}

// Capture uses native ReadRawAll for default keys and ordered independent raw
// reads for explicit keys. Neither route is a same-time transaction. An error
// returns no usable prefix; explicit-key failure identifies that selected call.
func (source *Source) Capture(ctx context.Context) (configsource.Batch, int, error) {
	if source == nil || source.client == nil {
		return configsource.Batch{}, -1, fail(ErrInput, "capture")
	}
	var values []*Document
	if len(source.keys) == 0 {
		var failed int
		var err error
		values, failed, err = source.client.ReadRawAll(ctx)
		if err != nil {
			return configsource.Batch{}, failed, err
		}
	} else {
		failed := -1
		err := source.client.run(ctx, "capture", func(ctx context.Context, client *native.Client) (Evidence, error) {
			total := 0
			for index, key := range source.keys {
				value, err := client.ReadRaw(ctx, nativeKey(key))
				if err != nil {
					failed = index
					return Evidence{FailedIndex: failed}, err
				}
				total += len(value.RawCopy())
				if total > MaxTotalBytes {
					failed = index
					return Evidence{FailedIndex: failed}, fail(ErrLimit, "capture")
				}
				values = append(values, &Document{native: value})
			}
			return Evidence{Documents: len(values), FailedIndex: -1}, nil
		})
		if err != nil {
			return configsource.Batch{}, failed, err
		}
	}
	batch, err := rawBatch(values)
	return batch, -1, err
}
func rawBatch(documents []*Document) (configsource.Batch, error) {
	values := make([]configsource.Raw, len(documents))
	for index, document := range documents {
		values[index] = configsource.Raw{Content: document.RawCopy(), Missing: document.Missing()}
	}
	return configsource.NewBatch(values)
}

// Observe consumes native already-acquired batches; it never starts another
// acquisition, polling or recovery path.
func (source *Source) Observe(ctx context.Context) (configsource.Observer, error) {
	if source == nil || source.client == nil {
		return nil, fail(ErrInput, "observe")
	}
	var observation *Observation
	var err error
	if len(source.keys) == 0 {
		observation, err = source.client.ObserveRaw(ctx, source.options)
	} else {
		observation, err = source.client.ObserveRawKeys(ctx, source.keys, source.options)
	}
	if err != nil {
		return nil, err
	}
	return &sourceObserver{observation: observation}, nil
}

type sourceObserver struct {
	private
	observation *Observation
}

func (observer *sourceObserver) Next(ctx context.Context) (configsource.Observation, error) {
	if observer == nil {
		return configsource.Observation{}, fail(ErrInput, "next")
	}
	value, err := observer.observation.Next(ctx)
	if err != nil {
		return configsource.Observation{}, err
	}
	result := configsource.Observation{FailedIndex: value.FailedIndex(), Err: value.Err(), Gap: value.Gap()}
	if value.Err() == nil {
		result.Batch, result.Err = rawBatch(value.DocumentsCopy())
	}
	return result, nil
}
func (observer *sourceObserver) Close(ctx context.Context) error {
	if observer == nil {
		return fail(ErrInput, "close")
	}
	return observer.observation.Close(ctx)
}

var _ configsource.Source = (*Source)(nil)
var _ configsource.Observer = (*sourceObserver)(nil)
