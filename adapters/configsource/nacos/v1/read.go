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

	native "github.com/frost-leo/fathomry/internal/configsource/nacos/v2"
)

// Document owns immutable native content/metadata. Accessors are deliberate
// sensitive inspection, not safe diagnostics or proof of cross-key atomicity.
type Document struct {
	private
	native *native.Document
}

// Read rejects missing or empty required content and never uses cached fallback.
func (client *Client) Read(ctx context.Context, key Key) (*Document, error) {
	return client.readOne(ctx, key, false)
}

// ReadRaw preserves positive missing and present-empty content separately.
func (client *Client) ReadRaw(ctx context.Context, key Key) (*Document, error) {
	return client.readOne(ctx, key, true)
}
func (client *Client) readOne(ctx context.Context, key Key, raw bool) (*Document, error) {
	var result *Document
	operation := "read"
	if raw {
		operation = "read_raw"
	}
	err := client.run(ctx, operation, func(ctx context.Context, selected *native.Client) (Evidence, error) {
		var value *native.Document
		var err error
		if raw {
			value, err = selected.ReadRaw(ctx, nativeKey(key))
		} else {
			value, err = selected.Read(ctx, nativeKey(key))
		}
		evidence := Evidence{FailedIndex: -1}
		if err == nil {
			result = &Document{native: value}
			evidence.Documents = 1
		}
		return evidence, err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// ReadAll returns the default key set in input order or no usable prefix.
func (client *Client) ReadAll(ctx context.Context) ([]*Document, error) {
	values, _, err := client.readAll(ctx, false)
	return values, err
}

// ReadRawAll preserves raw presence and the native failed-document index.
// -1 means unknown or no failing slot; indices are not remote sequence numbers.
func (client *Client) ReadRawAll(ctx context.Context) ([]*Document, int, error) {
	return client.readAll(ctx, true)
}
func (client *Client) readAll(ctx context.Context, raw bool) ([]*Document, int, error) {
	var result []*Document
	failed := -1
	operation := "read_all"
	if raw {
		operation = "read_raw_all"
	}
	err := client.run(ctx, operation, func(ctx context.Context, selected *native.Client) (Evidence, error) {
		var values []*native.Document
		var err error
		if raw {
			values, failed, err = selected.ReadRawAll(ctx)
		} else {
			values, err = selected.ReadAll(ctx)
		}
		if err == nil {
			result = documents(values)
		}
		return Evidence{Documents: len(result), FailedIndex: failed}, err
	})
	if err != nil {
		return nil, failed, err
	}
	return result, failed, nil
}
func documents(values []*native.Document) []*Document {
	result := make([]*Document, len(values))
	for index, value := range values {
		result[index] = &Document{native: value}
	}
	return result
}

// Missing reports positive native absence. Nil/zero handles are not missing proof.
func (value *Document) Missing() bool { return value != nil && value.native.Missing() }

// RawCopy returns detached original content, not an accepted typed value.
func (value *Document) RawCopy() []byte {
	if value == nil {
		return nil
	}
	return value.native.RawCopy()
}

// Key and Namespace are requested identity, not fields echoed by a query reply.
func (value *Document) Key() Key {
	if value == nil {
		return Key{}
	}
	return publicKey(value.native.Key())
}
func (value *Document) Namespace() string {
	if value == nil {
		return ""
	}
	return value.native.Namespace()
}

// MD5 is native content evidence, not authenticity or an ordered revision.
func (value *Document) MD5() string {
	if value == nil {
		return ""
	}
	return value.native.MD5()
}

// ContentType is a native hint; consumers still explicitly select decoding format.
func (value *Document) ContentType() string {
	if value == nil {
		return ""
	}
	return value.native.ContentType()
}

// LastModifiedMillis is native Unix milliseconds; zero means unavailable.
func (value *Document) LastModifiedMillis() int64 {
	if value == nil {
		return 0
	}
	return value.native.LastModifiedMillis()
}
