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
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	wire "github.com/nacos-group/nacos-sdk-go/v2/api/grpc"
	request "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_request"
	response "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_response"
	"google.golang.org/protobuf/types/known/anypb"
)

func (client *Client) envelope(value request.IRequest, accessToken string) *wire.Payload {
	stamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	digest := md5.Sum([]byte(stamp))
	value.PutAllHeaders(map[string]string{"Client-AppName": client.settings.AppName, "Client-RequestTS": stamp,
		"Client-RequestToken": hex.EncodeToString(digest[:]), "exConfigInfo": "true", "charset": "utf-8"})
	if accessToken != "" {
		value.PutAllHeaders(map[string]string{"accessToken": accessToken})
	}
	return &wire.Payload{Metadata: &wire.Metadata{Type: value.GetRequestType(), Headers: value.GetHeaders()}, Body: &anypb.Any{Value: []byte(value.GetBody(value))}}
}
func acknowledgement(value response.IResponse) *wire.Payload {
	raw, _ := json.Marshal(value)
	return &wire.Payload{Metadata: &wire.Metadata{Type: value.GetResponseType()}, Body: &anypb.Any{Value: raw}}
}
func payloadBody(value *wire.Payload) ([]byte, error) {
	if value == nil || value.Metadata == nil || value.Body == nil || value.Body.TypeUrl != "" || len(value.Metadata.Type) > 128 || len(value.Metadata.Headers) > 32 {
		return nil, fail(ErrDecode, "payload")
	}
	headerBytes := 0
	for name, content := range value.Metadata.Headers {
		headerBytes += len(name) + len(content)
	}
	if headerBytes > 32<<10 {
		return nil, fail(ErrLimit, "headers")
	}
	if err := validateJSON(value.Body.Value); err != nil {
		return nil, fail(ErrDecode, "payload", err)
	}
	return value.Body.Value, nil
}
func decodeResponse(payload *wire.Payload, expected string) (response.IResponse, error) {
	raw, err := payloadBody(payload)
	if err != nil {
		return nil, err
	}
	var create func() response.IResponse
	switch payload.Metadata.Type {
	case "ErrorResponse":
		create = func() response.IResponse { return &response.ErrorResponse{Response: &response.Response{}} }
	case expected:
		switch expected {
		case "ServerCheckResponse":
			create = func() response.IResponse { return &response.ServerCheckResponse{Response: &response.Response{}} }
		case "HealthCheckResponse":
			create = func() response.IResponse { return &response.HealthCheckResponse{Response: &response.Response{}} }
		case "ConfigQueryResponse":
			create = func() response.IResponse { return &response.ConfigQueryResponse{Response: &response.Response{}} }
		case "ConfigChangeBatchListenResponse":
			create = func() response.IResponse {
				return &response.ConfigChangeBatchListenResponse{Response: &response.Response{}}
			}
		}
	}
	if create == nil {
		return nil, fail(ErrDecode, "response-type")
	}
	value, err := response.InnerResponseJsonUnmarshal(raw, create)
	if err != nil {
		return nil, fail(ErrDecode, "response", err)
	}
	if !value.IsSuccess() || value.GetResultCode() != 200 || value.GetErrorCode() != 0 {
		native := &RemoteError{resultCode: value.GetResultCode(), errorCode: value.GetErrorCode(), message: strings.Clone(value.GetMessage())}
		switch {
		case expected == "ConfigQueryResponse" && value.GetErrorCode() == 300:
			return nil, fail(ErrMissing, "query", native)
		case value.GetErrorCode() == 401 || value.GetErrorCode() == 403:
			return nil, fail(ErrDenied, "response", native)
		default:
			return nil, fail(ErrUnavailable, "response", native)
		}
	}
	if payload.Metadata.Type != expected {
		return nil, fail(ErrDecode, "response-type")
	}
	if query, ok := value.(*response.ConfigQueryResponse); ok {
		var present struct {
			Content *string `json:"content"`
		}
		if json.Unmarshal(raw, &present) != nil || present.Content == nil {
			return nil, fail(ErrDecode, "content")
		}
		if query.EncryptedDataKey != "" {
			return nil, fail(ErrUnsupported, "encrypted-content")
		}
		if len(query.Content) > MaxDocumentBytes {
			return nil, fail(ErrLimit, "content")
		}
		if !utf8.ValidString(query.Content) || len(query.ContentType) > 32 || query.LastModified < 0 {
			return nil, fail(ErrDecode, "content")
		}
		if query.Md5 != "" {
			decoded, err := hex.DecodeString(query.Md5)
			digest := md5.Sum([]byte(query.Content))
			if err != nil || len(decoded) != md5.Size || !bytes.Equal(decoded, digest[:]) {
				return nil, fail(ErrDecode, "content-md5")
			}
		}
	}
	return value, nil
}
func validateJSON(raw []byte) error {
	if len(raw) > MaxWireBytes || !utf8.Valid(raw) {
		return errors.New("invalid bounded JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	nodes := 0
	var visit func(int) error
	visit = func(depth int) error {
		nodes++
		if depth > 64 || nodes > 32768 {
			return errors.New("JSON structure bound exceeded")
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		if delimiter, ok := token.(json.Delim); ok {
			switch delimiter {
			case '{':
				seen := make(map[string]bool)
				for decoder.More() {
					item, err := decoder.Token()
					if err != nil {
						return err
					}
					name, ok := item.(string)
					if !ok || seen[name] {
						return errors.New("duplicate JSON key")
					}
					seen[name] = true
					if err := visit(depth + 1); err != nil {
						return err
					}
				}
			case '[':
				for decoder.More() {
					if err := visit(depth + 1); err != nil {
						return err
					}
				}
			default:
				return errors.New("unexpected JSON delimiter")
			}
			_, err = decoder.Token()
			return err
		}
		return nil
	}
	if err := visit(0); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON input")
	}
	return nil
}
