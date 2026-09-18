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

package lark

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"sync"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
)

// Effect concerns the requested API operation only, not authentication, rendering,
// human reading, callback processing, business completion or rollback.
type Effect uint8

const (
	NotAttempted Effect = iota
	Unknown
	Rejected
	Accepted
	// Partial means a merged message was acknowledged with invalid input IDs.
	Partial
)

// Exchange is an immutable transport/platform observation. CodeKnown separates a
// missing code from explicit zero. RetryAfter is untrusted server advice, never
// an automatic retry policy. Zero HTTP status means no response headers arrived.
type Exchange struct {
	private
	status, code          int
	codeKnown             bool
	requestID, retryAfter string
}

func (value Exchange) HTTPStatus() int      { return value.status }
func (value Exchange) APICode() (int, bool) { return value.code, value.codeKnown }
func (value Exchange) RequestID() string    { return value.requestID }
func (value Exchange) RetryAfter() string   { return value.retryAfter }

// Result is immutable and retained independently by the inbox. JSONData/Bytes
// return copies of private data. IDs are inspection data, never metric labels.
// Authentication exchange evidence never contains a token, body or credential.
type Result struct {
	private
	effect                                           Effect
	exchange                                         Exchange
	authentication                                   Exchange
	authAttempted                                    bool
	target, requestUUID, messageID, cardID, assetKey string
	data                                             string
	binary                                           []byte
	contentType                                      string
	event                                            *Event
	resolvedRecipient                                Recipient
	messages                                         []MessageInfo
	pageToken                                        string
	hasMore                                          bool
	related, invalidMessageIDs                       []string
}

func (value Result) Effect() Effect     { return value.effect }
func (value Result) Exchange() Exchange { return value.exchange }
func (value Result) Authentication() (Exchange, bool) {
	return value.authentication, value.authAttempted
}
func (value Result) Target() string       { return value.target }
func (value Result) UUID() string         { return value.requestUUID }
func (value Result) MessageID() string    { return value.messageID }
func (value Result) CardID() string       { return value.cardID }
func (value Result) AssetKey() string     { return value.assetKey }
func (value Result) JSONData() []byte     { return []byte(value.data) }
func (value Result) Bytes() []byte        { return bytes.Clone(value.binary) }
func (value Result) ContentType() string  { return value.contentType }
func (value Result) RelatedIDs() []string { return append([]string(nil), value.related...) }
func (value Result) InvalidMessageIDs() []string {
	return append([]string(nil), value.invalidMessageIDs...)
}

type requestSpec struct {
	operation, method, path string
	query                   larkcore.QueryParams
	body                    any
	bodyBytes               int
	target, uuid, ackKey    string
	download, webhook       bool
	wireLimit               int
	upload                  *Upload
	webhookContent          Content
	related                 []string
	responseLimit           int
}
type attemptCounter interface{ Attempt() (uint64, error) }
type requestKey struct{}
type requestState struct {
	owner                   *owner
	call                    attemptCounter
	method, url             string
	limit, requestLimit     int
	entered                 bool
	response                *larkcore.ApiResp
	cleanup                 error
	dialMu                  sync.Mutex
	dialStarted, dialClosed bool
	dialDone                chan struct{}
	wireContext             context.Context
	connection              *ownedConn
}

func (client *Client) ready(operation string, webhook bool) error {
	if client == nil || client.owner == nil {
		return failure(ErrInput, operation)
	}
	if (client.owner.settings.Profile == "webhook") != webhook {
		return failure(ErrUnsupported, operation)
	}
	return nil
}

// Do is private SDK plumbing: the owner itself is never exposed. Every HTTP
// request must match a one-shot, call-associated permit, including auth traffic.
// Direct fresh HTTP/1 connections and RoundTrip avoid redirect and replay logic.
func (owned *owner) Do(request *http.Request) (*http.Response, error) {
	state, ok := request.Context().Value(requestKey{}).(*requestState)
	if !ok || state.owner != owned || state.entered || request.Method != state.method || request.URL.String() != state.url {
		return nil, failure(ErrUnsupported, "native-request")
	}
	if request.ContentLength < 0 || request.ContentLength > int64(state.requestLimit) {
		return nil, failure(ErrLimit, "request")
	}
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	if _, err := state.call.Attempt(); err != nil {
		return nil, err
	}
	state.entered = true
	wire, cancel := context.WithCancel(request.Context())
	state.wireContext = wire
	request = request.Clone(wire)
	defer func() {
		cancel()
		state.dialMu.Lock()
		state.dialClosed = true
		done := state.dialDone
		state.dialMu.Unlock()
		if done != nil {
			<-done
		}
		if state.connection != nil {
			state.cleanup = errors.Join(state.cleanup, state.connection.Close())
		}
	}()
	request.GetBody = nil
	response, err := owned.transport.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	captured := &larkcore.ApiResp{StatusCode: response.StatusCode, Header: response.Header.Clone()}
	state.response = captured
	data, readErr := io.ReadAll(io.LimitReader(response.Body, int64(state.limit)+1))
	state.cleanup = response.Body.Close()
	if len(data) > state.limit {
		return nil, failure(ErrLimit, "response")
	}
	captured.RawBody = data
	if readErr != nil {
		return nil, readErr
	}
	response.Body = io.NopCloser(bytes.NewReader(data))
	response.ContentLength = int64(len(data))
	return response, nil
}
func (owned *owner) nativeRequest(ctx context.Context, call attemptCounter, spec requestSpec, token string) (*requestState, error) {
	address := owned.settings.BaseURL + spec.path
	if spec.webhook {
		address = owned.settings.WebhookURL
	}
	if query := spec.query.Encode(); query != "" {
		address += "?" + query
	}
	limit := owned.settings.MaxRequestBytes
	if spec.upload != nil {
		limit = owned.settings.MaxAssetBytes + 16<<10
	}
	if spec.wireLimit > 0 {
		limit = min(limit, spec.wireLimit)
	}
	if spec.webhook && limit > 20<<10 {
		limit = 20 << 10
	}
	state := &requestState{owner: owned, call: call, method: spec.method, url: address, limit: owned.settings.MaxResponseBytes, requestLimit: limit}
	if spec.responseLimit > 0 {
		state.limit = min(state.limit, spec.responseLimit)
	}
	work := context.WithValue(ctx, requestKey{}, state)
	access := larkcore.AccessTokenTypeNone
	var options []larkcore.RequestOptionFunc
	if token != "" {
		access = larkcore.AccessTokenTypeTenant
		options = append(options, larkcore.WithTenantAccessToken(token))
	}
	if spec.download {
		options = append(options, larkcore.WithFileDownload())
	}
	path := spec.path
	if spec.webhook {
		path = owned.settings.WebhookURL
	}
	if spec.upload != nil {
		upload := spec.upload
		form := larkcore.NewFormdata()
		if spec.operation == "upload-image" {
			form.AddField("image_type", "message").AddFileWithName("image", upload.Name, bytes.NewReader(upload.Data))
		} else {
			form.AddField("file_type", upload.Type).AddField("file_name", upload.Name).AddFileWithName("file", upload.Name, bytes.NewReader(upload.Data))
			if upload.Duration != 0 {
				form.AddField("duration", upload.Duration)
			}
		}
		spec.body = form
	}
	_, err := larkcore.Request(work, &larkcore.ApiReq{HttpMethod: spec.method, ApiPath: path, QueryParams: spec.query,
		Body: spec.body, SupportedAccessTokenTypes: []larkcore.AccessTokenType{access}}, owned.native, options...)
	if err != nil && state.entered {
		err = failure(ErrHTTP, "transport", err)
	}
	return state, err
}
func exchange(state *requestState) Exchange {
	var value Exchange
	if state == nil || state.response == nil {
		return value
	}
	raw := state.response
	value.status = raw.StatusCode
	value.requestID = raw.RequestId()
	if !shortText(value.requestID, 512) {
		value.requestID = ""
	}
	value.retryAfter = raw.Header.Get("Retry-After")
	if !shortText(value.retryAfter, 128) {
		value.retryAfter = ""
	}
	return value
}
func decodeResponse(state *requestState, webhook bool) (Exchange, json.RawMessage, error) {
	value := exchange(state)
	if state.response == nil {
		return value, nil, failure(ErrHTTP, "response")
	}
	raw := state.response
	httpErr := error(nil)
	if value.status < 200 || value.status >= 300 {
		httpErr = failure(ErrHTTP, "status")
	}
	media, _, err := mime.ParseMediaType(raw.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return value, nil, failure(ErrResponse, "content-type", httpErr)
	}
	if err := checkJSON(raw.RawBody, state.limit); err != nil {
		return value, nil, failure(ErrResponse, "json", err, httpErr)
	}
	if _, err := exactFields(raw.RawBody, "code", "StatusCode", "data", "msg", "error"); err != nil {
		return value, nil, failure(ErrResponse, "envelope", err, httpErr)
	}
	var envelope struct {
		Code       *int            `json:"code"`
		LegacyCode *int            `json:"StatusCode"`
		Data       json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw.RawBody, &envelope); err != nil {
		return value, nil, failure(ErrResponse, "envelope", err, httpErr)
	}
	if webhook && envelope.Code == nil {
		envelope.Code = envelope.LegacyCode
	}
	if envelope.Code == nil {
		return value, nil, failure(ErrResponse, "missing-code", httpErr)
	}
	if envelope.LegacyCode != nil && webhook && *envelope.LegacyCode != *envelope.Code {
		return value, nil, failure(ErrResponse, "conflicting-code", httpErr)
	}
	value.code, value.codeKnown = *envelope.Code, true
	if value.code != 0 {
		// Native causes are deliberately inspectable, never formatted by fault.
		var native larkcore.CodeError
		_ = json.Unmarshal(raw.RawBody, &native)
		native.Code = value.code
		return value, nil, failure(ErrAPI, "platform", &native, httpErr)
	}
	if httpErr != nil {
		return value, nil, httpErr
	}
	return value, envelope.Data, nil
}
func (client *Client) execute(ctx context.Context, id fault.Correlation, spec requestSpec) (*invocation.Receipt[Result], error) {
	if err := client.ready(spec.operation, spec.webhook); err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, failure(ErrInput, spec.operation)
	}
	value := client.owner.settings
	if spec.bodyBytes > value.MaxRequestBytes && (spec.operation != "upload-image" && spec.operation != "upload-file") {
		return nil, failure(ErrLimit, spec.operation)
	}
	call, err := invocation.Begin(ctx, client.access, invocation.Request{Name: spec.operation, Correlation: id, Shape: invocation.Finite,
		Bytes: value.reservation(), EvidenceBytes: value.evidenceBytes(), Admission: invocation.Budget{Limit: value.Timeout}, AttemptsKnown: true, MaxAttempts: 2}, client.inbox, client.observer)
	if err != nil {
		return nil, err
	}
	result := Result{target: spec.target, requestUUID: spec.uuid, related: append([]string(nil), spec.related...)}
	work, cancel, err := (invocation.Budget{Limit: value.Timeout}).Context(ctx, invocation.Execute)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Present: true, Value: result, Primary: err})
		return call.Receipt(), nil
	}
	defer cancel()
	var primary, cleanup error
	if spec.webhook {
		spec.body = client.owner.webhookBody(spec.webhookContent)
	}
	if spec.upload == nil && spec.body != nil {
		encoded, encodeErr := json.Marshal(spec.body)
		limit := value.MaxRequestBytes
		if spec.wireLimit > 0 {
			limit = min(limit, spec.wireLimit)
		}
		if spec.webhook {
			limit = min(limit, 20<<10)
		}
		if encodeErr != nil {
			primary = failure(ErrInput, "encode", encodeErr)
		} else if len(encoded) > limit {
			primary = failure(ErrLimit, "request")
		} else {
			spec.body = json.RawMessage(encoded)
		}
	}
	token := ""
	if primary == nil && !spec.webhook {
		token, primary, cleanup = client.owner.accessToken(work, call, &result)
	}
	if primary == nil && cleanup == nil {
		state, requestErr := client.owner.nativeRequest(work, call, spec, token)
		if state.entered {
			result.effect = Unknown
		}
		result.exchange = exchange(state)
		cleanup = errors.Join(cleanup, state.cleanup)
		if spec.download && state.response != nil && state.response.StatusCode == http.StatusOK && requestErr == nil &&
			downloadResponse(state.response) {
			result.binary = bytes.Clone(state.response.RawBody)
			result.contentType = state.response.Header.Get("Content-Type")
			result.effect = Accepted
		} else if state.response != nil {
			observed, data, decodeErr := decodeResponse(state, spec.webhook)
			result.exchange = observed
			primary = errors.Join(requestErr, decodeErr)
			if primary == nil {
				result.data = string(data)
				if spec.download {
					primary = failure(ErrResponse, "download-envelope")
				} else {
					primary = result.readIdentity(spec.ackKey)
				}
				if primary == nil {
					result.effect = Accepted
					if len(result.invalidMessageIDs) > 0 {
						result.effect = Partial
					}
				}
			} else if requestErr == nil && observed.codeKnown && observed.code != 0 && observed.status < 500 {
				result.effect = Rejected
			}
		} else {
			primary = requestErr
		}
		if primary != nil && result.exchange.codeKnown {
			switch result.exchange.code {
			case 99991671, 99991664, 99991663:
				client.owner.invalidateToken(work, token)
			}
		}
	}
	if primary != nil {
		primary = failure(ErrCall, spec.operation, primary, work.Err(), context.Cause(work))
	}
	if cleanup != nil {
		cleanup = failure(ErrCleanup, spec.operation, cleanup)
	}
	call.Complete(invocation.Outcome[Result]{Present: true, Value: result, Primary: primary, Cleanup: cleanup})
	return call.Receipt(), nil
}
func (value *Result) readIdentity(key string) error {
	if key == "email-recipient" {
		return value.readRecipient()
	}
	if key == "" {
		return nil
	}
	fields, err := exactFields([]byte(value.data), key)
	if err != nil {
		return failure(ErrResponse, "data", err)
	}
	if key == "items" || key == "page" || key == "get-message" || key == "message-page" {
		if _, err := exactFields([]byte(value.data), "items", "has_more", "page_token"); err != nil {
			return failure(ErrResponse, "page", err)
		}
		var items []json.RawMessage
		if len(fields["items"]) == 0 || fields["items"][0] != '[' || json.Unmarshal(fields["items"], &items) != nil {
			return failure(ErrResponse, "items")
		}
		var messages []MessageInfo
		if key == "get-message" || key == "message-page" {
			target := ""
			if key == "get-message" {
				target = value.target
			}
			var err error
			messages, err = messageSnapshots(items, target)
			if err != nil {
				return err
			}
		}
		if key == "page" || key == "message-page" {
			var more *bool
			if json.Unmarshal(fields["has_more"], &more) != nil || more == nil {
				return failure(ErrResponse, "page")
			}
			var token string
			if raw, present := fields["page_token"]; present {
				var decoded *string
				if json.Unmarshal(raw, &decoded) != nil || decoded == nil || !shortText(*decoded, 4096) {
					return failure(ErrResponse, "page-token")
				}
				token = *decoded
			}
			if *more && token == "" {
				return failure(ErrResponse, "page-token")
			}
			value.pageToken = token
			value.hasMore = *more
		}
		value.messages = messages
		return nil
	}
	if key == "merged_message" {
		if _, err := exactFields([]byte(value.data), "message", "invalid_message_id_list"); err != nil {
			return failure(ErrResponse, "merged-message", err)
		}
		if _, err := exactFields(fields["message"], "message_id"); err != nil {
			return failure(ErrResponse, "merged-message", err)
		}
		var data struct {
			Message *struct {
				ID string `json:"message_id"`
			} `json:"message"`
			Invalid []string `json:"invalid_message_id_list"`
		}
		if json.Unmarshal([]byte(value.data), &data) != nil || data.Message == nil || !identifier(data.Message.ID) || len(data.Invalid) > len(value.related) {
			return failure(ErrResponse, "merged-message")
		}
		seen := map[string]bool{}
		for _, invalid := range data.Invalid {
			found := false
			for _, related := range value.related {
				if related == invalid {
					found = true
				}
			}
			if !found || seen[invalid] {
				return failure(ErrResponse, "invalid-message-id")
			}
			seen[invalid] = true
		}
		value.messageID = data.Message.ID
		value.invalidMessageIDs = data.Invalid
		return nil
	}
	var id string
	if json.Unmarshal(fields[key], &id) != nil || !identifier(id) {
		return failure(ErrResponse, "identity")
	}
	switch key {
	case "message_id":
		value.messageID = id
	case "card_id":
		value.cardID = id
	case "image_key", "file_key":
		value.assetKey = id
	}
	return nil
}
func downloadResponse(response *larkcore.ApiResp) bool {
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil {
		return false
	}
	disposition, _, _ := mime.ParseMediaType(response.Header.Get("Content-Disposition"))
	return disposition == "attachment" || media != "application/json"
}

// Page is one explicit page request. No iterator or hidden pagination is started.
// Size defaults to 20 and is bounded at 50; Token is private continuation data.
type Page struct {
	private
	Size  int
	Token string
}

func (value Page) query() (larkcore.QueryParams, error) {
	if value.Size == 0 {
		value.Size = 20
	}
	if value.Size < 1 || value.Size > 50 || !shortText(value.Token, 4096) {
		return nil, failure(ErrInput, "page")
	}
	query := larkcore.QueryParams{"page_size": {strconv.Itoa(value.Size)}}
	if value.Token != "" {
		query.Set("page_token", value.Token)
	}
	return query, nil
}

// NextPage reports the API's continuation without performing another request.
func (value Result) NextPage() (token string, more bool) {
	return value.pageToken, value.hasMore
}
