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
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
)

type managedTransport struct {
	source *managedSource
	kind   string
}

type managedRequestContextKey struct{}
type managedCleanupContextKey struct{}
type managedDialTargetKey struct{}
type managedOAuthTLSKey struct{}

// Classify opaque path segments before unescaping identifiers. A table called
// "metrics", or containing "/plan", is not a metrics/planning endpoint.
func (t *managedTransport) catalogRoute(request *http.Request) string {
	if request.URL == nil {
		return ""
	}
	base, _ := url.Parse(t.source.prepared.settings.CatalogURI)
	path, found := strings.CutPrefix(request.URL.EscapedPath(), base.EscapedPath()+"/v1/")
	if !found {
		return ""
	}
	if path == "config" {
		return path
	}
	if prefix := t.source.prepared.settings.CatalogPrefix; prefix != "" {
		path, found = strings.CutPrefix(path, url.PathEscape(prefix)+"/")
		if !found {
			return ""
		}
	}
	parts := strings.Split(path, "/")
	if parts[0] != "namespaces" {
		return path
	}
	if len(parts) == 1 {
		return "namespaces"
	}
	if len(parts) == 2 {
		return "namespace"
	}
	if len(parts) == 3 {
		return parts[2]
	}
	if len(parts) == 4 {
		return strings.TrimSuffix(parts[2], "s")
	}
	if parts[2] == "tables" {
		return "table-" + parts[4]
	}
	return ""
}

func (t *managedTransport) RoundTrip(request *http.Request) (response *http.Response, err error) {
	s := t.source.prepared.settings
	family, _ := request.Context().Value(managedFamilyKey{}).(*ManagedSession)
	setup, _ := request.Context().Value(managedSetupKey{}).(*managedSource)
	route := t.catalogRoute(request)
	telemetry := t.kind == "catalog" && request.Method == http.MethodPost && route == "table-metrics"
	if telemetry {
		if !managedRESTMetrics(s.Properties) {
			return nil, failure(ErrAuthority, "metrics-not-selected")
		}
		if family == nil || family.source != t.source {
			return nil, failure(ErrAuthority, "metrics-origin")
		}
		request = request.Clone(context.WithValue(context.WithValue(request.Context(), managedFamilyKey{}, (*ManagedSession)(nil)), managedSetupKey{}, t.source))
		family = nil
		setup = t.source
	}
	cleanupIO, _ := request.Context().Value(managedIOCleanupKey{}).(*managedIO)
	cleanup := cleanupIO != nil && family != nil && cleanupIO.ctx.Value(managedFamilyKey{}) == family && t.kind == "storage" && request.Method == http.MethodDelete && request.URL != nil && request.URL.Query().Get("uploadId") != ""
	var planCleanup *managedRemotePlan
	if family != nil && family.source == t.source && t.kind == "catalog" {
		planCleanup = family.cleanupPlan(request)
		if planCleanup != nil {
			index, first, previous := planCleanup.beginCancel()
			if !first {
				if previous != nil {
					return nil, previous
				}
				return &http.Response{StatusCode: http.StatusNoContent, Header: make(http.Header), Body: http.NoBody, Request: request}, nil
			}
			ctx, cancel := context.WithTimeout(context.WithoutCancel(request.Context()), min(s.Timeout, 5*time.Second))
			defer cancel()
			request = request.Clone(ctx)
			cleanup = true
			defer func() { planCleanup.finishCancel(index, response, err) }()
		}
	}
	if (family == nil || family.source != t.source) && setup != t.source {
		return nil, failure(ErrAuthority, "transport-context")
	}
	if request.Context().Err() != nil {
		return nil, errors.Join(request.Context().Err(), contextCause(request))
	}
	t.source.mu.Lock()
	closed := t.source.closed
	t.source.mu.Unlock()
	if closed {
		return nil, failure(ErrAuthority, "source-closed")
	}
	endpoint := s.CatalogURI
	if t.kind == "storage" {
		endpoint = s.StorageEndpoint
	}
	if t.kind == "oauth" {
		endpoint = s.OAuthURI
	}
	base, _ := url.Parse(endpoint)
	if request.URL == nil || base == nil || request.URL.Scheme != base.Scheme || request.URL.Host != base.Host || request.URL.User != nil || request.URL.Fragment != "" || request.URL.Opaque != "" {
		return nil, failure(ErrAuthority, "transport-target")
	}
	if t.kind == "catalog" && !strings.HasPrefix(request.URL.Path, base.Path+"/v1/") {
		return nil, failure(ErrAuthority, "catalog-path")
	}
	if t.kind == "catalog" {
		delegation := request.Header.Get("X-Iceberg-Access-Delegation")
		if delegation != "" && (!s.VendedCredentials || delegation != "vended-credentials") {
			return nil, failure(ErrAuthority, "access-delegation")
		}
	}
	if t.kind == "oauth" && (request.URL.Path != base.Path || request.URL.RawQuery != "") {
		return nil, failure(ErrAuthority, "oauth-target")
	}
	if t.kind == "storage" {
		root, _ := url.Parse(s.Location)
		bucketPath := "/" + root.Host
		if request.URL.Path != bucketPath && request.URL.Path != bucketPath+"/" && !strings.HasPrefix(request.URL.Path, bucketPath+root.Path) {
			return nil, failure(ErrAuthority, "storage-path")
		}
		if (request.URL.Path == bucketPath || request.URL.Path == bucketPath+"/") && request.Method == http.MethodGet {
			prefix := request.URL.Query().Get("prefix")
			if !strings.HasPrefix(prefix, strings.TrimPrefix(root.Path, "/")) {
				return nil, failure(ErrAuthority, "storage-list")
			}
		}
	}
	var requestBody []byte
	if t.kind != "storage" {
		if request.ContentLength < 0 || request.ContentLength > s.MaxMetadataBytes {
			return nil, failure(ErrLimit, "request-bytes")
		}
		if request.Body != nil {
			if request.GetBody == nil {
				return nil, failure(ErrProtocol, "request-body")
			}
			body, err := request.GetBody()
			if err != nil {
				return nil, err
			}
			requestBody, err = io.ReadAll(io.LimitReader(body, s.MaxMetadataBytes+1))
			closeErr := body.Close()
			if family != nil {
				family.noteCleanup(closeErr)
			}
			if err != nil {
				return nil, err
			}
			if int64(len(requestBody)) > s.MaxMetadataBytes {
				return nil, failure(ErrLimit, "request-bytes")
			}
		}
	}
	staging, _ := stagedCreateRequest(request, requestBody)
	mutation := t.kind == "catalog" && (request.Method == http.MethodPost || request.Method == http.MethodDelete) && route != "table-plan" && route != "table-tasks" && route != "table-metrics" && !staging
	var submission *ManagedCommitSubmission
	if family != nil && mutation && request.Method == http.MethodPost && route == "table" {
		submission, err = family.commitSubmission(request.URL.Path, requestBody)
		if err != nil {
			return nil, err
		}
	}
	if family != nil {
		family.mu.Lock()
		if family.closed && !cleanup {
			family.mu.Unlock()
			return nil, failure(ErrAuthority, "session-closed")
		}
		if !cleanup && (family.data.requests >= int(s.MaxFileOps) || int64(len(requestBody)) > s.MaxIOBytes-family.wireBytes) {
			family.mu.Unlock()
			return nil, failure(ErrLimit, "requests")
		}
		family.data.requests++
		family.wireBytes += int64(len(requestBody))
		if staging && family.current >= 0 {
			family.data.effects[family.current].Staged = true
			family.data.effects[family.current].Effect = Unknown
		}
		if mutation && family.current >= 0 {
			effect := &family.data.effects[family.current]
			effect.Submitted = true
			effect.Effect = Unknown
			if submission != nil {
				submission.Effect = Unknown
				index, operation := len(effect.Submissions), family.current
				effect.Submissions = append(effect.Submissions, *submission)
				defer func() {
					family.mu.Lock()
					family.data.effects[operation].Submissions[index] = *submission
					family.mu.Unlock()
				}()
			}
		}
		family.mu.Unlock()
	}
	ctx := request.Context()
	baseTransport := t.source.base
	if t.kind == "oauth" && t.source.oauthTransport != nil {
		baseTransport = t.source.oauthTransport
		ctx = context.WithValue(ctx, managedOAuthTLSKey{}, t.source)
	}
	if cleanup {
		ctx = context.WithValue(ctx, managedCleanupContextKey{}, family)
	}
	port := request.URL.Port()
	if port == "" {
		port = "80"
		if request.URL.Scheme == "https" {
			port = "443"
		}
	}
	ctx = context.WithValue(ctx, managedDialTargetKey{}, net.JoinHostPort(request.URL.Hostname(), port))
	request = request.Clone(context.WithValue(ctx, managedRequestContextKey{}, ctx))
	response, err = baseTransport.RoundTrip(request)
	if err != nil {
		return response, err
	}
	if submission != nil {
		submission.HTTPStatus = response.StatusCode
		switch response.StatusCode {
		case http.StatusConflict, http.StatusBadRequest, http.StatusForbidden, http.StatusUnauthorized, http.StatusNotFound:
			submission.Effect = Rejected
		}
	}
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		closeErr := response.Body.Close()
		if family != nil {
			family.noteCleanup(closeErr)
		}
		return nil, failure(ErrAuthority, "redirect")
	}
	if t.kind == "storage" {
		maximum := s.MaxMetadataBytes
		if request.Method == http.MethodGet && request.Header.Get("Range") != "" {
			maximum = s.MaxObjectBytes
		}
		response.Body = &managedBody{ReadCloser: response.Body, remaining: maximum, family: family}
		return response, nil
	}
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, s.MaxMetadataBytes+1))
	closeErr := response.Body.Close()
	if family != nil {
		family.noteCleanup(closeErr)
	} else if closeErr != nil {
		t.source.mu.Lock()
		t.source.closeErrors = append(t.source.closeErrors, closeErr)
		t.source.mu.Unlock()
	}
	if readErr != nil {
		return nil, readErr
	}
	if int64(len(raw)) > s.MaxMetadataBytes {
		return nil, failure(ErrLimit, "response-bytes")
	}
	if family != nil {
		family.mu.Lock()
		family.wireBytes += int64(len(raw))
		over := family.wireBytes > s.MaxIOBytes
		family.mu.Unlock()
		if over && !cleanup {
			return nil, failure(ErrLimit, "response-bytes")
		}
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 && t.kind == "catalog" {
		if family != nil && len(raw) != 0 && (route == "table-plan" || route == "table-tasks") {
			if err := family.trackPlanningReply(request, requestBody, raw); err != nil {
				return nil, err
			}
		}
		if err := t.validateReply(request, requestBody, raw, family); err != nil {
			return nil, err
		}
		if submission != nil {
			var reply struct {
				Location string `json:"metadata-location"`
			}
			if err := json.Unmarshal(raw, &reply); err != nil {
				return nil, err
			}
			submission.Effect = Acknowledged
			submission.MetadataLocation = reply.Location
			if submission.IntendedSnapshot != submission.BaseSnapshot {
				submission.CommittedSnapshot = submission.IntendedSnapshot
			}
		}
		if !s.VendedCredentials && bytes.Contains(raw, []byte("storage-credentials")) {
			var payload map[string]json.RawMessage
			if err := json.Unmarshal(raw, &payload); err != nil {
				return nil, err
			}
			delete(payload, "storage-credentials")
			var err error
			raw, err = json.Marshal(payload)
			if err != nil {
				return nil, err
			}
		}
	}
	if (mutation || staging) && family != nil {
		family.mu.Lock()
		if family.current >= 0 {
			effect := &family.data.effects[family.current]
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				effect.Effect = Acknowledged
			}
			if response.StatusCode == http.StatusConflict || response.StatusCode == http.StatusBadRequest || response.StatusCode == http.StatusForbidden || response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusNotFound {
				effect.Effect = Rejected
			}
		}
		family.mu.Unlock()
	}
	response.Body = io.NopCloser(bytes.NewReader(raw))
	response.ContentLength = int64(len(raw))
	return response, nil
}

func (f *ManagedSession) commitSubmission(path string, raw []byte) (*ManagedCommitSubmission, error) {
	var input struct {
		Updates      table.Updates
		Requirements []struct {
			Type       string `json:"type"`
			Ref        string `json:"ref"`
			SnapshotID *int64 `json:"snapshot-id"`
		}
	}
	if err := json.Unmarshal(raw, &input); err != nil {
		return nil, err
	}
	f.mu.Lock()
	base := f.bases[path]
	f.mu.Unlock()
	if base == nil {
		return nil, failure(ErrProtocol, "commit-base")
	}
	wanted, err := table.UpdateTableMetadata(base, input.Updates, "")
	if err != nil {
		return nil, err
	}
	if err = f.validateTable(wanted); err != nil {
		return nil, err
	}
	if f.creationIntent != nil {
		matches, err := managedCreationMatches(f.creationIntent, wanted)
		if err != nil || !matches {
			return nil, failure(ErrProtocol, "staged-create-intent", err)
		}
	}
	ref := table.MainBranch
	changes := referenceChanges(base, wanted)
	if len(changes) == 1 {
		ref = changes[0].Name
	}
	submission := &ManagedCommitSubmission{Ref: ref, TableUUID: base.TableUUID().String()}
	if snapshot := base.SnapshotByName(ref); snapshot != nil {
		submission.BaseSnapshot = snapshot.SnapshotID
	}
	for _, requirement := range input.Requirements {
		if requirement.Type == "assert-ref-snapshot-id" && requirement.Ref == ref {
			submission.BaseSnapshot = 0
			if requirement.SnapshotID != nil {
				submission.BaseSnapshot = *requirement.SnapshotID
			}
		}
	}
	if snapshot := wanted.SnapshotByName(ref); snapshot != nil {
		submission.IntendedSnapshot = snapshot.SnapshotID
	}
	return submission, nil
}

func stagedCreateRequest(request *http.Request, raw []byte) (bool, string) {
	if request.Method != http.MethodPost || request.URL == nil || !strings.HasSuffix(request.URL.Path, "/tables") {
		return false, ""
	}
	var input struct {
		Stage bool   `json:"stage-create"`
		Name  string `json:"name"`
	}
	if json.Unmarshal(raw, &input) != nil {
		return false, ""
	}
	return input.Stage, input.Name
}

func contextCause(request *http.Request) error { return context.Cause(request.Context()) }

type managedBody struct {
	io.ReadCloser
	remaining int64
	family    *ManagedSession
}

func (b *managedBody) Read(p []byte) (int, error) {
	if b.remaining < 0 {
		return 0, failure(ErrLimit, "response-body")
	}
	if int64(len(p)) > b.remaining+1 {
		p = p[:b.remaining+1]
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	if b.remaining < 0 {
		return n, failure(ErrLimit, "response-body")
	}
	return n, err
}
func (b *managedBody) Close() error {
	err := b.ReadCloser.Close()
	if b.family != nil {
		b.family.noteCleanup(err)
	}
	return err
}

func (t *managedTransport) validateReply(request *http.Request, sent, raw []byte, f *ManagedSession) error {
	route := t.catalogRoute(request)
	if len(raw) == 0 {
		bodyless := request.Method == http.MethodHead || request.Method == http.MethodDelete || route == "transactions/commit" || route == "table-metrics" || route == "tables/rename" || route == "views/rename"
		if bodyless {
			return nil
		}
		return failure(ErrProtocol, "empty-catalog-response")
	}
	object, err := decodeObjectBounded(raw, t.source.prepared.settings.MaxListEntries)
	if err != nil {
		return err
	}
	if err := exactControlNames(object, "metadata", "metadata-location", "config", "defaults", "overrides", "storage-credentials", "identifiers", "namespace", "properties"); err != nil {
		return err
	}
	if request.Method == http.MethodPost && route == "namespaces" {
		var input struct{ Namespace []string }
		if err := json.Unmarshal(sent, &input); err != nil {
			return err
		}
		var output struct{ Namespace []string }
		if err := json.Unmarshal(raw, &output); err != nil {
			return err
		}
		if !reflect.DeepEqual(input.Namespace, output.Namespace) {
			return failure(ErrProtocol, "namespace-reply")
		}
	}
	if request.Method == http.MethodGet && (route == "tables" || route == "views" || route == "functions") {
		if _, ok := object["identifiers"].([]any); !ok {
			return failure(ErrProtocol, "catalog-list")
		}
	}
	if route == "config" {
		for _, field := range []string{"defaults", "overrides"} {
			if values, ok := object[field].(map[string]any); ok {
				for key, value := range values {
					if managedAuthorityProperty(key) {
						want, exists := t.expectedProperty(key)
						if !exists || value != want {
							return failure(ErrAuthority, "server-authority")
						}
					}
				}
			}
		}
	}
	metadata, exists := object["metadata"]
	if !exists || f == nil {
		return nil
	}
	metaObject, ok := metadata.(map[string]any)
	if !ok {
		return failure(ErrProtocol, "metadata")
	}
	if err = metadataNames(metaObject, false); err != nil {
		return err
	}
	location, hasLocation := object["metadata-location"].(string)
	if _, functionMetadata := metaObject["function-uuid"]; functionMetadata {
		return f.validateFunctionReply(metaObject, location)
	}
	staging, tableName := stagedCreateRequest(request, sent)
	if !hasLocation && !staging {
		return failure(ErrProtocol, "metadata-location")
	}
	if location != "" || !staging {
		if err = f.location(location); err != nil {
			return err
		}
	}
	if _, viewMetadata := metaObject["view-uuid"]; viewMetadata {
		return f.validateViewReply(request, sent, metaObject)
	}
	data, err := json.Marshal(metaObject)
	if err != nil {
		return err
	}
	actual, err := table.ParseMetadataBytes(data)
	if err != nil {
		return err
	}
	if err = f.validateTable(actual); err != nil {
		return err
	}
	if tableName != "" {
		f.mu.Lock()
		f.bases[request.URL.Path+"/"+tableName] = actual
		f.mu.Unlock()
	}
	if request.Method == http.MethodPost && route == "table" {
		var payload struct {
			Updates table.Updates `json:"updates"`
		}
		if err = json.Unmarshal(sent, &payload); err != nil {
			return err
		}
		f.mu.Lock()
		base := f.bases[request.URL.Path]
		f.mu.Unlock()
		if base != nil && len(payload.Updates) > 0 {
			builder, err := table.MetadataBuilderFromBase(base, "")
			if err != nil {
				return err
			}
			for _, update := range payload.Updates {
				if err = update.Apply(builder); err != nil {
					return err
				}
			}
			wanted, err := builder.Build()
			if err != nil {
				return err
			}
			if !managedCommitMatches(base, wanted, actual) {
				return failure(ErrProtocol, "commit-intent", rest.ErrCommitStateUnknown)
			}
		}
	}
	f.mu.Lock()
	f.bases[request.URL.Path] = actual
	if request.URL.Query().Get("snapshots") == "refs" {
		f.snapshotSubsets[location] = true
	}
	f.mu.Unlock()
	return nil
}

func (t *managedTransport) expectedProperty(key string) (string, bool) {
	s := t.source.prepared.settings
	values := map[string]string{"uri": s.CatalogURI, "prefix": s.CatalogPrefix, "warehouse": s.Warehouse, "token": s.BearerToken, "credential": s.OAuthCredential,
		"oauth2-server-uri": s.OAuthURI, "rest.authorization-url": s.OAuthURI, "scope": s.OAuthScope, "audience": s.OAuthAudience, "resource": s.OAuthResource,
		"rest.signing-region": s.SigningRegion, "rest.signing-name": s.SigningService, "tls-skip-verify": "false"}
	if s.SigV4 {
		values["rest.sigv4-enabled"] = "true"
	} else {
		values["rest.sigv4-enabled"] = "false"
	}
	value, ok := values[key]
	return value, ok
}

func managedCommitMatches(base, wanted, actual table.Metadata) bool {
	if base == nil || wanted == nil || actual == nil || actual.TableUUID() != wanted.TableUUID() {
		return false
	}
	objects := make([]map[string]any, 3)
	for index, metadata := range []table.Metadata{base, wanted, actual} {
		raw, err := json.Marshal(metadata)
		if err != nil {
			return false
		}
		objects[index], err = decodeObjectBounded(raw, len(raw))
		if err != nil {
			return false
		}
		normalizeMetadataCollections(objects[index])
	}
	for key, want := range objects[1] {
		switch key {
		case "last-updated-ms", "metadata-log", "snapshot-log":
			continue
		}
		if !metadataDeltaMatches(key, objects[0][key], want, objects[2][key]) {
			return false
		}
	}
	for key := range objects[0] {
		if _, ok := objects[1][key]; !ok {
			if _, exists := objects[2][key]; exists {
				return false
			}
		}
	}
	return true
}

func metadataDeltaMatches(key string, base, want, actual any) bool {
	if reflect.DeepEqual(base, want) {
		return true
	}
	if key == "properties" || key == "refs" {
		before, _ := base.(map[string]any)
		after, _ := want.(map[string]any)
		got, _ := actual.(map[string]any)
		for name, value := range after {
			if !reflect.DeepEqual(before[name], value) && !reflect.DeepEqual(got[name], value) {
				return false
			}
		}
		for name := range before {
			if _, kept := after[name]; !kept {
				if _, exists := got[name]; exists {
					return false
				}
			}
		}
		return true
	}
	id := map[string]string{"snapshots": "snapshot-id", "schemas": "schema-id", "partition-specs": "spec-id", "sort-orders": "order-id", "statistics": "snapshot-id", "partition-statistics": "snapshot-id", "versions": "version-id"}[key]
	if id != "" {
		index := func(value any) map[any]any {
			result := map[any]any{}
			items, _ := value.([]any)
			for _, item := range items {
				object, _ := item.(map[string]any)
				result[object[id]] = item
			}
			return result
		}
		before, after, got := index(base), index(want), index(actual)
		for name, value := range after {
			if !reflect.DeepEqual(before[name], value) && !reflect.DeepEqual(got[name], value) {
				return false
			}
		}
		for name := range before {
			if _, kept := after[name]; !kept {
				if _, exists := got[name]; exists {
					return false
				}
			}
		}
		return true
	}
	return reflect.DeepEqual(want, actual)
}
