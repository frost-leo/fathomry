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

package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	"github.com/frost-leo/fathomry/internal/invocation"
)

type exchangeKey struct{}
type requestContextKey struct{}
type strippedContext struct{ context.Context }

func (strippedContext) Value(any) any { return nil }
func withExchange(ctx context.Context, state *exchange) context.Context {
	return context.WithValue(context.WithValue(strippedContext{ctx}, exchangeKey{}, state), requestContextKey{}, ctx)
}

type exchange struct {
	owner           *connection
	call            *invocation.Call[Result]
	mu              sync.Mutex
	requests        int
	storageRequests int
	catalogBytes    int64
	fileOps         int
	fileGate        chan struct{}
	ioBytes         int64
	effect          Effect
	files           []FileEffect
	primary         []error
	cleanup         []error
	closing         bool
	dials           sync.WaitGroup
}

func newExchange(owner *connection, call *invocation.Call[Result]) *exchange {
	return &exchange{owner: owner, call: call, fileGate: make(chan struct{}, 1)}
}
func (state *exchange) note(err error, cleanup bool) {
	if err == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if cleanup {
		state.cleanup = append(state.cleanup, err)
	} else {
		state.primary = append(state.primary, err)
	}
}
func (state *exchange) errors() (error, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	return errors.Join(state.primary...), errors.Join(state.cleanup...)
}

func (state *exchange) beginDial() bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closing {
		return false
	}
	state.dials.Add(1)
	return true
}
func (state *exchange) finish() (error, error) {
	state.mu.Lock()
	state.closing = true
	state.mu.Unlock()
	state.dials.Wait()
	return state.errors()
}

type catalogTransport struct{ owner *connection }

func (transport *catalogTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	state, _ := request.Context().Value(exchangeKey{}).(*exchange)
	if state == nil || state.owner != transport.owner {
		return nil, failure(ErrAuthority, "transport")
	}
	s := transport.owner.settings
	endpoint, _ := url.Parse(s.CatalogURI)
	if request.URL.Scheme != endpoint.Scheme || request.URL.Host != endpoint.Host ||
		!strings.HasPrefix(request.URL.Path, endpoint.Path+"/v1/") || request.URL.User != nil {
		return nil, failure(ErrAuthority, "catalog-endpoint")
	}
	state.mu.Lock()
	state.requests++
	allowed := state.requests <= s.MaxFileOps && request.ContentLength >= 0 &&
		request.ContentLength <= int64(s.MaxMetadataBytes)-state.catalogBytes
	if allowed {
		state.catalogBytes += request.ContentLength
	}
	state.mu.Unlock()
	if !allowed {
		return nil, failure(ErrLimit, "catalog-requests")
	}
	if err := request.Context().Err(); err != nil {
		return nil, err
	}
	if request.ContentLength > int64(s.MaxMetadataBytes) {
		return nil, failure(ErrLimit, "catalog-request")
	}
	request = request.Clone(request.Context())
	request.Header.Del("X-Iceberg-Access-Delegation")
	mutation := request.Method == http.MethodPost || request.Method == http.MethodDelete
	if mutation {
		state.mu.Lock()
		state.effect = Unknown
		state.mu.Unlock()
	}
	response, err := transport.owner.transport.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	state.mu.Lock()
	remaining := int64(s.MaxMetadataBytes) - state.catalogBytes
	state.mu.Unlock()
	data, readErr := io.ReadAll(io.LimitReader(response.Body, remaining+1))
	state.note(response.Body.Close(), true)
	if readErr != nil {
		return nil, readErr
	}
	if int64(len(data)) > remaining {
		return nil, failure(ErrLimit, "catalog-response")
	}
	state.mu.Lock()
	state.catalogBytes += int64(len(data))
	state.mu.Unlock()
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		return nil, failure(ErrAuthority, "redirect")
	}
	if response.StatusCode >= 400 {
		if response.StatusCode == http.StatusNotFound {
			if strings.Contains(request.URL.Path, "/tables/") {
				state.note(catalog.ErrNoSuchTable, false)
			} else {
				state.note(catalog.ErrNoSuchNamespace, false)
			}
		}
		if mutation {
			switch response.StatusCode {
			case http.StatusConflict:
				state.note(table.ErrCommitFailed, false)
				state.mu.Lock()
				state.effect = Rejected
				state.mu.Unlock()
			case 500, 502, 503, 504:
				state.note(rest.ErrCommitStateUnknown, false)
			}
		}
	} else {
		if err = validateResponse(s, request, response.StatusCode, data); err != nil {
			return nil, err
		}
		if mutation {
			state.mu.Lock()
			state.effect = Acknowledged
			state.mu.Unlock()
		}
	}
	response.Body = io.NopCloser(bytes.NewReader(data))
	response.ContentLength = int64(len(data))
	return response, nil
}
func validateResponse(s settings, request *http.Request, status int, data []byte) error {
	if request.Method == http.MethodDelete && status == http.StatusNoContent {
		return nil
	}
	if status != http.StatusOK {
		return failure(ErrProtocol, "catalog-status")
	}
	var payload map[string]json.RawMessage
	object, err := decodeObject(data)
	if err != nil {
		return err
	}
	if err = exactControlNames(object, "metadata", "metadata-location", "config", "storage-credentials", "defaults", "overrides", "namespace", "properties", "identifiers", "next-page-token"); err != nil {
		return err
	}
	if len(data) == 0 || json.Unmarshal(data, &payload) != nil || payload == nil {
		return failure(ErrProtocol, "catalog-json")
	}
	if strings.HasSuffix(request.URL.Path, "/config") {
		for _, group := range []string{"defaults", "overrides"} {
			var props map[string]string
			if raw := payload[group]; raw != nil && json.Unmarshal(raw, &props) != nil {
				return failure(ErrProtocol, "config")
			}
			for key, value := range props {
				switch key {
				case "uri":
					if value != s.CatalogURI {
						return failure(ErrAuthority, "catalog-uri")
					}
				case "prefix":
					if value != s.CatalogPrefix {
						return failure(ErrAuthority, "catalog-prefix")
					}
				case "rest-page-size", "idempotency-key-lifetime":
				default:
					return failure(ErrUnsupported, "catalog-config")
				}
			}
		}
	}
	if request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/tables") {
		identifiers, ok := object["identifiers"].([]any)
		if !ok {
			return failure(ErrProtocol, "table-list")
		}
		for _, value := range identifiers {
			entry, ok := value.(map[string]any)
			if !ok {
				return failure(ErrProtocol, "table-identifier")
			}
			if err := exactControlNames(entry, "namespace", "name"); err != nil {
				return err
			}
		}
	}
	if request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/namespaces") {
		namespace, ok := object["namespace"].([]any)
		if !ok || len(namespace) != 1 || namespace[0] != s.Namespace {
			return failure(ErrProtocol, "namespace-reply")
		}
	}
	tableResponse := strings.Contains(request.URL.Path, "/tables/") &&
		(request.Method == http.MethodGet || request.Method == http.MethodPost) ||
		strings.HasSuffix(request.URL.Path, "/tables") && request.Method == http.MethodPost
	if tableResponse {
		metadata, err := parseMetadata(s, payload["metadata"])
		if err != nil || metadata == nil {
			return failure(ErrProtocol, "table-metadata", err)
		}
		if err = validateMetadata(s, metadata); err != nil {
			return err
		}
		var location string
		if json.Unmarshal(payload["metadata-location"], &location) != nil || !strings.HasPrefix(location, s.Location) {
			return failure(ErrAuthority, "metadata-location")
		}
		var credentials []json.RawMessage
		if raw := payload["storage-credentials"]; raw != nil {
			if json.Unmarshal(raw, &credentials) != nil || len(credentials) > 16 {
				return failure(ErrProtocol, "storage-credentials")
			}
		}
		var config map[string]string
		if raw := payload["config"]; raw != nil {
			if json.Unmarshal(raw, &config) != nil {
				return failure(ErrProtocol, "storage-config")
			}
			// The selected FileIO is authoritative. Catalog-facing and client-facing
			// endpoints can differ; load verifies the immutable metadata object
			// through the selected object store instead of trusting these settings.
		}
	}
	return nil
}
