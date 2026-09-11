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
	"encoding/json"
	"errors"
	wire "github.com/nacos-group/nacos-sdk-go/v2/api/grpc"
	response "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_response"
	"google.golang.org/protobuf/types/known/anypb"
	"strings"
	"testing"
)

func payloadBytes(raw []byte) *anypb.Any { return &anypb.Any{Value: raw} }
func TestResponseSemanticsAndBounds(t *testing.T) {
	for _, raw := range []string{
		`{"resultCode":200,"content":"{}","md5":"","requestId":"1"}`,
		`{"resultCode":200,"success":true,"content":"{}","md5":"","future":{"key":1}}`,
	} {
		value, err := decodeResponse(&wire.Payload{Metadata: &wire.Metadata{Type: "ConfigQueryResponse"}, Body: payloadBytes([]byte(raw))}, "ConfigQueryResponse")
		if err != nil || value.(*response.ConfigQueryResponse).Content != "{}" {
			t.Fatal("native successful response rejected", err)
		}
	}
	for _, raw := range []string{`{"resultCode":200,"content":null}`, `{"resultCode":200}`, `{"resultCode":200,"content":"{}","content":"[]"}`, `{"resultCode":200,"content":"{}","md5":"bad"}`} {
		if _, err := decodeResponse(&wire.Payload{Metadata: &wire.Metadata{Type: "ConfigQueryResponse"}, Body: payloadBytes([]byte(raw))}, "ConfigQueryResponse"); !errors.Is(err, ErrDecode) {
			t.Fatal("invalid raw response accepted", err)
		}
	}
	if _, err := decodeResponse(encoded(&response.ConfigQueryResponse{Response: &response.Response{ResultCode: 500, ErrorCode: 300}}), "ConfigQueryResponse"); !errors.Is(err, ErrMissing) {
		t.Fatal("missing became empty success")
	}
	for _, test := range []struct {
		content, key string
		want         error
	}{
		{strings.Repeat("x", MaxDocumentBytes+1), "", ErrLimit},
		{"ciphertext", "encrypted-key", ErrUnsupported},
	} {
		raw := encoded(&response.ConfigQueryResponse{Response: &response.Response{ResultCode: 200, Success: true}, Content: test.content, EncryptedDataKey: test.key})
		if _, err := decodeResponse(raw, "ConfigQueryResponse"); !errors.Is(err, test.want) {
			t.Fatal("unsupported content profile accepted", err)
		}
	}
}
func FuzzProtocol(f *testing.F) {
	f.Add([]byte(`{"resultCode":200,"content":"{}"}`))
	f.Add([]byte("{}"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 64<<10 {
			return
		}
		value, err := decodeResponse(&wire.Payload{Metadata: &wire.Metadata{Type: "ConfigQueryResponse"}, Body: payloadBytes(raw)}, "ConfigQueryResponse")
		if err == nil {
			var decoded map[string]json.RawMessage
			if json.Unmarshal(raw, &decoded) != nil || value == nil {
				t.Fatal("invented valid response")
			}
		}
	})
}
