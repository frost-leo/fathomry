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

package trino_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	trino "github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1"
	"github.com/google/uuid"
)

const (
	faultRequestBytes = 4 << 10
	faultPageBytes    = 2 << 20
	faultWireBytes    = 32 << 20
	faultMaxPages     = 256
)

type lostMutationPage struct {
	ID          string          `json:"id"`
	NextURI     string          `json:"nextUri"`
	UpdateCount *int64          `json:"updateCount"`
	Error       json.RawMessage `json:"error"`
}

type lostMutationObservation struct {
	ReadinessPosts, MutationPosts, BackendMutationPosts int
	Pages                                               int
	WireBytes                                           int64
	Terminal, Dropped, Failed                           bool
	Aggregate                                           int64
}

type lostMutationProxy struct {
	server    *httptest.Server
	backend   *url.URL
	client    *http.Client
	transport *http.Transport
	options   trino.Settings
	statement string
	finished  chan struct{}

	mu        sync.Mutex
	observed  lostMutationObservation
	claimed   bool
	readyID   string
	readyNext string
	seen      map[string]bool
}

func newLostMutationProxy(t *testing.T, options trino.Settings, statement string) *lostMutationProxy {
	t.Helper()
	backend, err := url.Parse(options.Endpoint)
	if err != nil || trino.Validate(options) != nil || backend.Scheme != "http" || backend.User != nil ||
		backend.Path != "" || backend.RawQuery != "" || backend.Fragment != "" || !options.Plaintext ||
		options.Password != "" || options.BearerToken != "" || len(statement) == 0 || len(statement) > faultRequestBytes ||
		!strings.HasPrefix(statement, "INSERT INTO ") {
		t.Fatal("lost-response fixture profile is invalid")
	}
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true, DisableCompression: true, MaxConnsPerHost: 1,
		MaxResponseHeaderBytes: 32 << 10, ResponseHeaderTimeout: 30 * time.Second,
		DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext}
	proxy := &lostMutationProxy{backend: backend, transport: transport, options: options, statement: statement,
		finished: make(chan struct{}), seen: make(map[string]bool),
		client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	proxy.server = httptest.NewUnstartedServer(http.HandlerFunc(proxy.serve))
	proxy.server.Config.ReadHeaderTimeout = 5 * time.Second
	proxy.server.Config.ReadTimeout = 2 * time.Minute
	proxy.server.Config.WriteTimeout = 2 * time.Minute
	proxy.server.Config.MaxHeaderBytes = 32 << 10
	proxy.server.Start()
	t.Cleanup(func() {
		proxy.server.Close()
		transport.CloseIdleConnections()
	})
	return proxy
}

func faultQueryToken(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

func (proxy *lostMutationProxy) continuation(text, query string) (*url.URL, bool) {
	value, err := url.Parse(text)
	if err != nil || len(text) > 2048 || value.Scheme != proxy.backend.Scheme || value.Host != proxy.backend.Host ||
		value.User != nil || value.RawQuery != "" || value.ForceQuery || value.Fragment != "" || value.RawPath != "" ||
		value.Opaque != "" || value.String() != text || strings.ContainsAny(value.Path, "%\\") {
		return nil, false
	}
	parts := strings.Split(value.Path, "/")
	if len(parts) != 7 || parts[0] != "" || parts[1] != "v1" || parts[2] != "statement" ||
		parts[3] != "queued" && parts[3] != "executing" || parts[4] != query || !faultQueryToken(query) || !faultQueryToken(parts[5]) {
		return nil, false
	}
	token, err := strconv.ParseUint(parts[6], 10, 63)
	return value, err == nil && strconv.FormatUint(token, 10) == parts[6]
}

func (proxy *lostMutationProxy) backendRequest(ctx context.Context, method, target string, body []byte) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("fixture request could not be constructed")
	}
	request.GetBody = nil
	request.Header.Set("Content-Type", "text/plain; charset=utf-8")
	request.Header.Set("X-Trino-User", proxy.options.User)
	request.Header.Set("X-Trino-Source", "fathomry")
	request.Header.Set("X-Trino-Catalog", proxy.options.Catalog)
	request.Header.Set("X-Trino-Schema", proxy.options.Schema)
	request.Header.Set("X-Trino-Session", "retry_policy=NONE")
	request.Header.Set("X-Trino-Client-Capabilities", "PARAMETRIC_DATETIME")
	return proxy.client.Do(request)
}

// fetch is called with mu held. It performs exactly one authorized HTTP request,
// not a retry. Response and total budgets include readiness and mutation pages.
func (proxy *lostMutationProxy) fetch(ctx context.Context, method, target string, body []byte) ([]byte, lostMutationPage, error) {
	var page lostMutationPage
	if proxy.observed.Pages >= faultMaxPages || proxy.observed.WireBytes >= faultWireBytes {
		return nil, page, errors.New("fixture response budget exhausted")
	}
	response, err := proxy.backendRequest(ctx, method, target, body)
	if err != nil {
		return nil, page, errors.New("fixture backend request failed")
	}
	defer response.Body.Close()
	proxy.observed.Pages++
	limit := min(int64(faultPageBytes), int64(faultWireBytes)-proxy.observed.WireBytes)
	if response.ContentLength > limit {
		return nil, page, errors.New("fixture response length exceeded")
	}
	data, readErr := io.ReadAll(io.LimitReader(response.Body, limit+1))
	proxy.observed.WireBytes += int64(len(data))
	kind, _, typeErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if readErr != nil || int64(len(data)) > limit || response.StatusCode != http.StatusOK ||
		typeErr != nil || kind != "application/json" || response.Header.Get("Content-Encoding") != "" {
		return nil, page, errors.New("fixture backend response rejected")
	}
	if json.Unmarshal(data, &page) != nil || !faultQueryToken(page.ID) ||
		len(page.Error) != 0 && !bytes.Equal(bytes.TrimSpace(page.Error), []byte("null")) {
		return nil, page, errors.New("fixture backend page rejected")
	}
	for key := range response.Header {
		name := strings.ToLower(key)
		if strings.HasPrefix(name, "x-trino-set-") || strings.HasPrefix(name, "x-trino-clear-") ||
			strings.HasPrefix(name, "x-trino-added-") || strings.HasPrefix(name, "x-trino-deallocated-") ||
			name == "x-trino-started-transaction-id" {
			return nil, page, errors.New("fixture session mutation rejected")
		}
	}
	return data, page, nil
}

func (proxy *lostMutationProxy) cancelBackend(query string) bool {
	if !faultQueryToken(query) {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	response, err := proxy.backendRequest(ctx, http.MethodDelete, proxy.backend.String()+"/v1/query/"+query, nil)
	if err == nil {
		count, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, faultRequestBytes+1))
		closeErr := response.Body.Close()
		return response.StatusCode == http.StatusNoContent && count <= faultRequestBytes && readErr == nil && closeErr == nil
	}
	return false
}

func (proxy *lostMutationProxy) forwardReady(writer http.ResponseWriter, ctx context.Context, method, target string, body []byte) {
	data, page, err := proxy.fetch(ctx, method, target, body)
	if err != nil || proxy.readyID != "" && page.ID != proxy.readyID {
		proxy.observed.Failed = true
		query := proxy.readyID
		if query == "" {
			query = page.ID
		}
		proxy.cancelBackend(query)
		writer.WriteHeader(http.StatusBadGateway)
		return
	}
	proxy.readyID = page.ID
	proxy.readyNext = ""
	if page.NextURI != "" {
		next, ok := proxy.continuation(page.NextURI, page.ID)
		if !ok || proxy.seen[page.NextURI] {
			proxy.observed.Failed = true
			proxy.cancelBackend(page.ID)
			writer.WriteHeader(http.StatusBadGateway)
			return
		}
		proxy.seen[page.NextURI] = true
		proxy.readyNext = page.NextURI
		var fields map[string]json.RawMessage
		if json.Unmarshal(data, &fields) != nil {
			proxy.observed.Failed = true
			proxy.cancelBackend(page.ID)
			writer.WriteHeader(http.StatusBadGateway)
			return
		}
		fields["nextUri"], _ = json.Marshal(proxy.server.URL + next.Path)
		data, err = json.Marshal(fields)
		if err != nil || len(data) > faultPageBytes {
			proxy.observed.Failed = true
			proxy.cancelBackend(page.ID)
			writer.WriteHeader(http.StatusBadGateway)
			return
		}
	}
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write(data)
}

func (proxy *lostMutationProxy) loseMutation(writer http.ResponseWriter, ctx context.Context, body []byte) {
	defer close(proxy.finished)
	proxy.observed.BackendMutationPosts++
	_, page, err := proxy.fetch(ctx, http.MethodPost, proxy.backend.String()+"/v1/statement", body)
	query := page.ID
	for err == nil && page.NextURI != "" {
		next, ok := proxy.continuation(page.NextURI, query)
		if !ok || proxy.seen[page.NextURI] {
			err = errors.New("fixture mutation continuation rejected")
			break
		}
		proxy.seen[page.NextURI] = true
		_, page, err = proxy.fetch(ctx, http.MethodGet, next.String(), nil)
		if err == nil && page.ID != query {
			err = errors.New("fixture mutation query identity changed")
		}
	}
	if err != nil || page.UpdateCount == nil || *page.UpdateCount != 1 {
		proxy.observed.Failed = true
		proxy.cancelBackend(query)
		writer.WriteHeader(http.StatusBadGateway)
		return
	}
	proxy.observed.Terminal, proxy.observed.Aggregate = true, *page.UpdateCount
	hijacker, ok := writer.(http.Hijacker)
	if !ok {
		proxy.observed.Failed = true
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	connection, _, err := hijacker.Hijack()
	if err != nil {
		proxy.observed.Failed = true
		return
	}
	// Nothing was written to the frontend. Closing here loses the response
	// only after independently observed backend terminal acknowledgement.
	if connection.Close() != nil {
		proxy.observed.Failed = true
		return
	}
	proxy.observed.Dropped = true
}

func (proxy *lostMutationProxy) serve(writer http.ResponseWriter, request *http.Request) {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	front, _ := url.Parse(proxy.server.URL)
	if request.Host != front.Host || request.URL.IsAbs() || request.URL.RawPath != "" || request.URL.RawQuery != "" ||
		request.URL.ForceQuery || request.URL.Fragment != "" {
		proxy.observed.Failed = true
		writer.WriteHeader(http.StatusForbidden)
		return
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, faultRequestBytes+1))
	if err != nil || len(body) > faultRequestBytes {
		proxy.observed.Failed = true
		writer.WriteHeader(http.StatusRequestEntityTooLarge)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), 90*time.Second)
	defer cancel()
	if request.Method == http.MethodPost && request.URL.Path == "/v1/statement" {
		switch string(body) {
		case "SELECT version()":
			proxy.observed.ReadinessPosts++
			if proxy.observed.ReadinessPosts != 1 || proxy.claimed {
				break
			}
			proxy.forwardReady(writer, ctx, http.MethodPost, proxy.backend.String()+"/v1/statement", body)
			return
		case proxy.statement:
			proxy.observed.MutationPosts++
			if proxy.claimed || proxy.readyNext != "" || proxy.observed.ReadinessPosts != 1 {
				break
			}
			proxy.claimed = true
			proxy.loseMutation(writer, ctx, body)
			return
		}
	} else if request.Method == http.MethodGet && len(body) == 0 && proxy.readyNext != "" {
		next, ok := proxy.continuation(proxy.readyNext, proxy.readyID)
		if ok && request.URL.Path == next.Path {
			proxy.forwardReady(writer, ctx, http.MethodGet, next.String(), nil)
			return
		}
	} else if request.Method == http.MethodDelete && len(body) == 0 && proxy.readyID != "" {
		next, ok := proxy.continuation(proxy.readyNext, proxy.readyID)
		if request.URL.Path == "/v1/query/"+proxy.readyID || ok && request.URL.Path == next.Path {
			if proxy.cancelBackend(proxy.readyID) {
				writer.WriteHeader(http.StatusNoContent)
			} else {
				proxy.observed.Failed = true
				writer.WriteHeader(http.StatusBadGateway)
			}
			return
		}
	}
	proxy.observed.Failed = true
	writer.WriteHeader(http.StatusForbidden)
}

func (proxy *lostMutationProxy) observation() lostMutationObservation {
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	return proxy.observed
}

func requireLostMutationOutcome(t *testing.T, proxy *lostMutationProxy, value trino.Result, err error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	select {
	case <-proxy.finished:
	case <-ctx.Done():
		t.Fatal("lost-response fixture did not finish")
	}
	observed := proxy.observation()
	_, countPresent := value.UpdateCount()
	if err == nil || !errors.Is(err, trino.ErrOperation) || value.Submissions() != 1 || value.Effect() != trino.Unknown ||
		value.Terminal() || value.Succeeded() || value.Complete() || value.QueryID() != "" || countPresent ||
		observed.Failed || !observed.Terminal || !observed.Dropped || observed.Aggregate != 1 ||
		observed.MutationPosts != 1 || observed.BackendMutationPosts != 1 || observed.ReadinessPosts != 1 {
		t.Fatal("lost-response control did not separate unknown public effects from backend acknowledgement")
	}
}

func TestTrinoPublicLostMutationResponseService(t *testing.T) {
	options := publicServiceSettings(t)
	direct := openPublicService(t, options)
	profile, err := direct.owner.Client().Profile(context.Background())
	if err != nil || profile.ServiceVersion.Kind != "observed" || profile.ServiceVersion.Value != "482" {
		t.Fatal("lost-response service coordinator differs from the qualified profile")
	}
	connector := requirePublicServiceRows(t, direct.require(t, "fault-connector", true,
		"SELECT connector_name FROM system.metadata.catalogs WHERE catalog_name = ?", options.Catalog))
	if !reflect.DeepEqual(connector, [][]any{{"iceberg"}}) {
		t.Fatal("lost-response service connector differs from the qualified profile")
	}
	schema := direct.require(t, "fault-schema", true, "SELECT schema_name FROM "+publicServiceQuote(options.Catalog)+
		".information_schema.schemata WHERE schema_name = ?", options.Schema)
	if schema.Rows() != 1 {
		t.Fatal("lost-response fixture schema is not explicitly available")
	}
	name := "gh110_fault_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if absent, err := direct.absence(name); err != nil || !absent {
		t.Fatal("lost-response fixture table was not independently absent")
	}
	t.Logf("test-owned fault table identifier: %s", name)
	acknowledged := false
	t.Cleanup(func() {
		dropped, err := direct.invoke(false, "DROP TABLE IF EXISTS "+direct.table(name))
		if err != nil || !dropped.Complete() || dropped.Effect() != trino.Acknowledged {
			t.Error("lost-response fixture DROP is unconfirmed")
			return
		}
		if absent, err := direct.absence(name); err != nil || !absent {
			t.Error("lost-response fixture absence is unconfirmed")
			return
		}
		if !acknowledged {
			t.Error("fixture CREATE was unacknowledged; delayed remote creation cannot be excluded")
			return
		}
		t.Log("independent SQL readback confirmed the test-owned fault table is absent")
	})
	direct.require(t, "fault-create", false, "CREATE TABLE "+direct.table(name)+" (value VARCHAR) WITH (format = 'PARQUET', format_version = 2)")
	acknowledged = true
	format := direct.require(t, "fault-format", true, "SHOW CREATE TABLE "+direct.table(name))
	if !bytes.Contains(format.DataCopy(), []byte("format_version = 2")) || !bytes.Contains(format.DataCopy(), []byte("PARQUET")) {
		t.Fatal("lost-response fixture table format is unconfirmed")
	}
	marker := uuid.NewString()
	statement := "INSERT INTO " + direct.table(name) + " (value) VALUES ('" + marker + "')"
	proxy := newLostMutationProxy(t, options, statement)
	proxiedOptions := options
	proxiedOptions.Name, proxiedOptions.Endpoint = "gh110-lost-response", proxy.server.URL
	proxied := openPublicService(t, proxiedOptions)
	value, invokeErr := proxied.invoke(false, statement)
	requireLostMutationOutcome(t, proxy, value, invokeErr)
	readback := requirePublicServiceRows(t, direct.require(t, "fault-independent-readback", true, "SELECT value FROM "+direct.table(name)))
	if !reflect.DeepEqual(readback, [][]any{{marker}}) {
		t.Fatal("independent lost-response readback did not find exactly one unique value")
	}
	t.Log("one backend mutation reached terminal aggregate one; its frontend response was deliberately lost; public evidence remained Unknown with one submission; independent direct readback found exactly one value without replay")
}

func TestLostMutationProxyOfflineControl(t *testing.T) {
	const statement = "INSERT INTO fixture VALUES ('marker')"
	var backend *httptest.Server
	var mutationPosts atomic.Int64
	backend = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(request.Body, faultRequestBytes+1))
		switch {
		case request.Method == http.MethodPost && string(body) == "SELECT version()":
			writePage(t, writer, map[string]any{"id": "ready", "nextUri": backend.URL + "/v1/statement/executing/ready/slug/1"})
		case request.Method == http.MethodGet && strings.Contains(request.URL.Path, "/ready/"):
			writePage(t, writer, map[string]any{"id": "ready", "columns": []any{wireColumn("version", "varchar", "varchar", longArgument(2147483647))}, "data": [][]string{{"482"}}})
		case request.Method == http.MethodPost && string(body) == statement:
			mutationPosts.Add(1)
			writePage(t, writer, map[string]any{"id": "mutation", "nextUri": backend.URL + "/v1/statement/executing/mutation/slug/1"})
		case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/mutation/slug/1"):
			writePage(t, writer, map[string]any{"id": "mutation", "nextUri": backend.URL + "/v1/statement/executing/mutation/slug/2"})
		case request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "/mutation/slug/2"):
			writePage(t, writer, map[string]any{"id": "mutation", "updateCount": 1})
		default:
			t.Error("unexpected offline backend request")
			writer.WriteHeader(http.StatusForbidden)
		}
	}))
	t.Cleanup(backend.Close)
	options := trino.Settings{Name: "fault-offline", Endpoint: backend.URL, User: "fixture", Plaintext: true, Writes: true,
		Timeout: 5 * time.Second, CleanupTimeout: time.Second}
	proxy := newLostMutationProxy(t, options, statement)
	options.Endpoint = proxy.server.URL
	fixture := openPublicService(t, options)
	value, err := fixture.invoke(false, statement)
	requireLostMutationOutcome(t, proxy, value, err)
	if mutationPosts.Load() != 1 || proxy.observation().Pages != 5 {
		t.Fatal("offline control replayed mutation or skipped native pages")
	}
}

func TestLostMutationProxyRejectsForeignAuthority(t *testing.T) {
	for _, redirect := range []bool{false, true} {
		t.Run(strconv.FormatBool(redirect), func(t *testing.T) {
			var foreignRequests atomic.Int64
			foreign := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				foreignRequests.Add(1)
				writer.WriteHeader(http.StatusNoContent)
			}))
			t.Cleanup(foreign.Close)
			backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				body, _ := io.ReadAll(io.LimitReader(request.Body, faultRequestBytes+1))
				switch {
				case request.Method == http.MethodPost && string(body) == "SELECT version()":
					writePage(t, writer, map[string]any{"id": "ready", "columns": []any{wireColumn("version", "varchar", "varchar", longArgument(2147483647))}, "data": [][]string{{"482"}}})
				case request.Method == http.MethodPost:
					if redirect {
						writer.Header().Set("Location", foreign.URL)
						writer.WriteHeader(http.StatusTemporaryRedirect)
					} else {
						writePage(t, writer, map[string]any{"id": "mutation", "nextUri": foreign.URL + "/v1/statement/executing/mutation/slug/1"})
					}
				case request.Method == http.MethodDelete && request.URL.Path == "/v1/query/mutation":
					writer.WriteHeader(http.StatusNoContent)
				default:
					t.Error("unexpected rejecting-control request")
					writer.WriteHeader(http.StatusForbidden)
				}
			}))
			t.Cleanup(backend.Close)
			options := trino.Settings{Name: "fault-authority", Endpoint: backend.URL, User: "fixture", Plaintext: true, Writes: true,
				Timeout: 5 * time.Second, CleanupTimeout: time.Second}
			const statement = "INSERT INTO fixture VALUES ('marker')"
			proxy := newLostMutationProxy(t, options, statement)
			options.Endpoint = proxy.server.URL
			fixture := openPublicService(t, options)
			_, err := fixture.invoke(false, statement)
			observed := proxy.observation()
			if err == nil || !observed.Failed || observed.Terminal || observed.Dropped || foreignRequests.Load() != 0 || observed.BackendMutationPosts != 1 {
				t.Fatal("fault control followed foreign authority or falsely certified a dropped acknowledgement")
			}
		})
	}
}
