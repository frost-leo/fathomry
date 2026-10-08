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

package surf

import (
	"context"
	"io"

	native "github.com/frost-leo/fathomry/internal/httpclient/surf/v1"
)

// Multipart preserves native ordered unique fields followed by ordered files.
// Duplicate field names replace their value at the original position. At most 128
// combined entries are admitted; metadata and encoded reads use source bounds.
// No local filesystem path or native mutable Multipart/Builder is exposed.
type Multipart struct {
	private
	Fields []Field
	Parts  []Part
}
type Field struct {
	private
	Name, Value string
}

// Part borrows exactly one Input or Open after admission. Open must return a
// fresh independent reader yielding the same logical bytes on replay. A reader
// plus error is still closed. Factories are lazy initially and acquired before a
// replay can dispatch, without reading or buffering the complete input.
// Mixed inputs are one-shot; static native retries then reject before borrowing.
type Part struct {
	private
	Name, FileName, ContentType string
	Input                       io.ReadCloser
	Open                        func(context.Context) (io.ReadCloser, error)
}

// MaxMultipartParts bounds the combined field/file count, before public admission.
const MaxMultipartParts = native.MaxMultipartParts

func nativeMultipart(value *Multipart) (*native.Multipart, error) {
	if value == nil {
		return nil, nil
	}
	if len(value.Fields) > MaxMultipartParts || len(value.Parts) > MaxMultipartParts-len(value.Fields) {
		return nil, fail(ErrLimit, "multipart")
	}
	result := &native.Multipart{Fields: make([]native.Field, len(value.Fields)), Parts: make([]native.Part, len(value.Parts))}
	for index, field := range value.Fields {
		result.Fields[index] = native.Field{Name: field.Name, Value: field.Value}
	}
	for index, part := range value.Parts {
		result.Parts[index] = native.Part{Name: part.Name, FileName: part.FileName, ContentType: part.ContentType, Input: part.Input, Open: part.Open}
	}
	return result, nil
}
