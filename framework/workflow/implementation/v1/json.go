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

package implementation

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"

	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

type bindingList []BindingInfo
type callList []CallInfo

// UnmarshalJSON routes legacy JSON callers through the same bounded data decoder.
func (value *Manifest) UnmarshalJSON(raw []byte) error {
	if value == nil {
		return problem(ErrHandle, "manifest", "")
	}
	if len(raw) > MaxManifestBytes {
		return problem(ErrLimit, "manifest_bytes", "")
	}
	if err := json.Unmarshal(raw, value, json.RejectUnknownMembers(true)); err != nil {
		return decodeFailure(err)
	}
	return nil
}

func decodeFailure(err error) error {
	code := ErrInvalid
	switch {
	case errors.Is(err, ErrLimit):
		code = ErrLimit
	case errors.Is(err, ErrHandle):
		code = ErrHandle
	}
	return problem(code, "manifest_encoding", "", err)
}

func decodeList[T any](decoder *jsontext.Decoder, output *[]T) error {
	token, err := decoder.ReadToken()
	if err != nil {
		return decodeFailure(err)
	}
	if token.Kind() != '[' {
		return problem(ErrInvalid, "manifest_array", "")
	}
	values := []T{}
	total := 0
	for decoder.PeekKind() != ']' {
		if len(values) >= 4096 {
			return problem(ErrLimit, "manifest_count", "")
		}
		raw, err := decoder.ReadValue()
		if err != nil {
			return decodeFailure(err)
		}
		if len(raw) > MaxManifestBytes-total {
			return problem(ErrLimit, "manifest_bytes", "")
		}
		total += len(raw)
		var value T
		if err = json.Unmarshal(raw, &value, json.RejectUnknownMembers(true)); err != nil {
			return decodeFailure(err)
		}
		values = append(values, value)
	}
	if _, err = decoder.ReadToken(); err != nil {
		return decodeFailure(err)
	}
	*output = values
	return nil
}
func (values *bindingList) UnmarshalJSONFrom(decoder *jsontext.Decoder) error {
	var decoded []BindingInfo
	if err := decodeList(decoder, &decoded); err != nil {
		return err
	}
	*values = decoded
	return nil
}
func (values *callList) UnmarshalJSONFrom(decoder *jsontext.Decoder) error {
	var decoded []CallInfo
	if err := decodeList(decoder, &decoded); err != nil {
		return err
	}
	*values = decoded
	return nil
}

// UnmarshalJSONFrom bounds owned data arrays before allocation. Decoding does
// not verify the declaration or reconstruct a callable-bearing Registry.
func (value *Manifest) UnmarshalJSONFrom(decoder *jsontext.Decoder) error {
	if value == nil || decoder == nil {
		return problem(ErrHandle, "manifest", "")
	}
	raw, err := decoder.ReadValue()
	if err != nil {
		return decodeFailure(err)
	}
	if len(raw) > MaxManifestBytes {
		return problem(ErrLimit, "manifest_bytes", "")
	}
	var wire struct {
		Format        string            `json:"format"`
		ABIProfile    string            `json:"abi_profile"`
		Specification declaration.Entry `json:"specification"`
		Bindings      bindingList       `json:"bindings"`
		Calls         callList          `json:"calls"`
	}
	if err := json.Unmarshal(raw, &wire, json.RejectUnknownMembers(true)); err != nil {
		return decodeFailure(err)
	}
	if wire.Bindings == nil || wire.Calls == nil {
		return problem(ErrInvalid, "manifest_array", "")
	}
	result := Manifest{wire.Format, wire.ABIProfile, wire.Specification, wire.Bindings, wire.Calls}
	if err := manifestBound(result); err != nil {
		return err
	}
	*value = result
	return nil
}
