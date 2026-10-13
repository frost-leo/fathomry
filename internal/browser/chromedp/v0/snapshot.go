/*
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

package chromedp

import (
	"encoding/json/jsontext"
	jsonstd "encoding/json/v2"
	"io"

	"github.com/chromedp/cdproto/runtime"
)

type remoteSnapshotFields runtime.RemoteObject

var nativeResultMarshalers = jsonstd.MarshalToFunc(func(encoder *jsontext.Encoder, remote *runtime.RemoteObject) error {
	return jsonstd.MarshalEncode(encoder, struct {
		*remoteSnapshotFields
		Value jsontext.Value `json:"value,omitzero"`
	}{remoteSnapshotFields: (*remoteSnapshotFields)(remote), Value: jsontext.Value(remote.Value)})
})

// WriteNativeResult serializes an admitted result with the Go 1.27 JSON encoder.
// RemoteObject values keep empty JSON distinct from absent values, recursively.
// The caller must bound the writer and keep result data immutable until return.
// This output-only helper neither admits work nor changes native command codecs.
func WriteNativeResult(writer io.Writer, value any) error {
	if writer == nil {
		return failure(ErrInput, "snapshot-writer")
	}
	var encoder jsontext.Encoder
	encoder.Reset(writer, jsonstd.DefaultOptionsV2(), jsontext.AllowInvalidUTF8(true))
	return jsonstd.MarshalEncode(&encoder, value, jsonstd.WithMarshalers(nativeResultMarshalers))
}
