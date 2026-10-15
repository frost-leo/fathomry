/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package lark

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

// Operation selects one pinned notification endpoint, never an arbitrary URL.
type Operation string

type operationSpec struct {
	method, path                         string
	query                                []string
	user, tenant, data, download, upload bool
}

func userRoute(method, path string) bool {
	for _, route := range operations {
		if !route.user || route.method != method {
			continue
		}
		pattern, actual := strings.Split(route.path, "/"), strings.Split(path, "/")
		if len(pattern) != len(actual) {
			continue
		}
		matched := true
		for index, part := range pattern {
			if !strings.HasPrefix(part, ":") && part != actual[index] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

// Request is an immutable native request. Path values follow placeholder order;
// query values preserve repeated, empty and omitted values. Body preserves all
// native JSON fields and exact numbers. Native pure-value builders can be encoded
// by the caller; no Client, serializer, dispatcher or arbitrary URL is borrowed.
type Request struct {
	private
	operation Operation
	path      string
	query     larkcore.QueryParams
	body      JSON
}

// RequestOptions contains bounded non-authority request metadata. Source-bound
// credentials are selected separately, not smuggled through headers.
type RequestOptions struct {
	private
	RequestID string
	Headers   http.Header
}

// NewRequest freezes a closed operation and its native path/query/body values.
// Platform semantic validation and permissions are not established by preparation.
func NewRequest(operation Operation, path []string, query url.Values, body JSON) (Request, error) {
	spec, ok := operations[operation]
	if !ok {
		return Request{}, failure(ErrUnsupported, "operation")
	}
	if len(path) > 4 || len(query) > 32 {
		return Request{}, failure(ErrLimit, "request")
	}
	parts := strings.Split(spec.path, "/")
	index := 0
	for pos, part := range parts {
		if !strings.HasPrefix(part, ":") {
			continue
		}
		if index >= len(path) || !identifier(path[index]) {
			return Request{}, failure(ErrInput, "path")
		}
		parts[pos] = url.PathEscape(path[index])
		index++
	}
	if index != len(path) {
		return Request{}, failure(ErrInput, "path")
	}
	frozen := larkcore.QueryParams{}
	total := 0
	for name, values := range query {
		if !slices.Contains(spec.query, name) || len(values) > 128 {
			return Request{}, failure(ErrInput, "query")
		}
		for _, value := range values {
			total += len(value)
			if !shortText(value, 4096) || strings.ContainsAny(value, "\r\n") || total > 32<<10 {
				return Request{}, failure(ErrLimit, "query")
			}
		}
		frozen[name] = slices.Clone(values)
	}
	if body.value != "" && !object(body) {
		return Request{}, failure(ErrInput, "body")
	}
	return Request{operation: operation, path: strings.Join(parts, "/"), query: frozen, body: body}, nil
}
func (value Request) Operation() Operation { return value.operation }

func freezeRequestOptions(input RequestOptions) (RequestOptions, error) {
	if !shortText(input.RequestID, 256) || strings.ContainsAny(input.RequestID, "\r\n") || len(input.Headers) > 32 {
		return RequestOptions{}, failure(ErrInput, "request-options")
	}
	total := 0
	headers := http.Header{}
	for name, values := range input.Headers {
		canonical := http.CanonicalHeaderKey(name)
		// Routing, authentication, framing and native request IDs are source-owned.
		switch strings.ToLower(name) {
		case "authorization", "proxy-authorization", "host", "cookie", "connection", "content-length", "content-type", "transfer-encoding", "trailer", "upgrade", "expect", "accept-encoding", "x-request-id", "request-id", "x-tt-logid", "x-target-service":
			return RequestOptions{}, failure(ErrInput, "reserved-header")
		}
		if len(name) == 0 || len(name) > 128 || len(values) > 16 || headers[canonical] != nil {
			return RequestOptions{}, failure(ErrInput, "header")
		}
		for _, char := range name {
			if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("!#$%&'*+-.^_\x60|~", char)) {
				return RequestOptions{}, failure(ErrInput, "header")
			}
		}
		for _, value := range values {
			total += len(name) + len(value)
			if !shortText(value, 4096) || strings.ContainsAny(value, "\r\n") || total > 16<<10 {
				return RequestOptions{}, failure(ErrInput, "header")
			}
		}
		headers[canonical] = slices.Clone(values)
	}
	input.Headers = headers
	return input, nil
}

// Do executes exactly one catalog operation after optional source-owned token
// acquisition. It never pages, refreshes-and-retries, or follows redirects.
func (client *Client) Do(ctx context.Context, id fault.Correlation, input Request, options RequestOptions) (*invocation.Receipt[Result], error) {
	spec, ok := operations[input.operation]
	if !ok || input.path == "" {
		return nil, failure(ErrInput, "request")
	}
	if err := client.ready(string(input.operation), false); err != nil {
		return nil, err
	}
	if spec.upload {
		return nil, failure(ErrInput, "use-upload")
	}
	user := client.owner.settings.Profile == "user-token"
	if user && !spec.user || !user && !spec.tenant {
		return nil, failure(ErrUnsupported, "token-type")
	}
	options, err := freezeRequestOptions(options)
	if err != nil {
		return nil, err
	}
	request := requestSpec{operation: strings.ToLower(string(input.operation)), path: input.path, method: spec.method, query: input.query,
		bodyBytes: len(input.body.value), download: spec.download, dataRequired: spec.data, requestID: options.RequestID, headers: options.Headers, target: input.path}
	request.scopeFields = responseScopes[input.operation]
	if input.body.value != "" {
		request.body = json.RawMessage(input.body.value)
	}
	switch input.operation {
	case LegacyBatchSend:
		request.ackKey = "message_id"
		request.partialKey = "legacy-batch"
		if err := validateLegacyBatch(input.body); err != nil {
			return nil, err
		}
	case IMV1MessageCreate, IMV1MessageReply, IMV1MessageForward, IMV1ThreadForward:
		request.ackKey = "message_id"
	case CardKitV1CardCreate, CardKitV1CardIdConvert:
		request.ackKey = "card_id"
	case IMV1ChatCreate:
		request.ackKey = "chat_id"
	case IMV1MessageReactionCreate:
		request.ackKey = "reaction_id"
	case IMV1MessageUrgentApp, IMV1MessageUrgentPhone, IMV1MessageUrgentSms:
		request.partialKey = "invalid_user_id_list"
	case IMV1MessageMergeForward:
		fields, err := exactFields(input.body.Bytes(), "message_id_list")
		if err != nil {
			return nil, err
		}
		if json.Unmarshal(fields["message_id_list"], &request.related) != nil {
			return nil, failure(ErrInput, "message-ids")
		}
		request.ackKey = "merged_message"
	case BotInfo:
		request.botInfo = true
		request.ackKey = "open_id"
	}
	if input.body.value != "" {
		fields, _ := exactFields(input.body.Bytes(), "uuid")
		_ = json.Unmarshal(fields["uuid"], &request.uuid)
	}
	if values := input.query["uuid"]; len(values) > 0 {
		request.uuid = values[0]
	}
	return client.execute(ctx, id, request)
}

func (value *Result) readPartial(key string) error {
	if key == "legacy-batch" {
		return value.readBatchPartial()
	}
	fields, err := exactFields([]byte(value.data), key)
	if err != nil {
		return failure(ErrResponse, "partial", err)
	}
	raw, ok := fields[key]
	if !ok || string(raw) == "null" {
		return nil
	}
	var ids []string
	if json.Unmarshal(raw, &ids) != nil {
		return failure(ErrResponse, "partial")
	}
	value.invalidUserIDs = ids
	return nil
}
