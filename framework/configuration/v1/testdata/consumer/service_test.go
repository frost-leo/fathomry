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

package consumer

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	remote "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
	c "github.com/frost-leo/fathomry/framework/configuration/v1"
)

type serviceFixture struct {
	HTTPURL         string `json:"http_url"`
	GRPCAddress     string `json:"grpc_address"`
	Namespace       string `json:"namespace"`
	Username        string `json:"username"`
	Password        string `json:"password"`
	AdminUsername   string `json:"admin_username"`
	AdminPassword   string `json:"admin_password"`
	RootCAPEM       string `json:"root_ca_pem"`
	AllowInsecure   bool   `json:"allow_insecure"`
	AllowWrites     bool   `json:"allow_writes"`
	RecordedVersion string `json:"recorded_version"`
}
type adminReply struct {
	Code int             `json:"code"`
	Data json.RawMessage `json:"data"`
}
type serviceAdmin struct {
	client     *http.Client
	url, token string
}

func (admin serviceAdmin) call(ctx context.Context, method, path string, values url.Values, target any) (int, error) {
	endpoint := admin.url + path
	var body io.Reader
	if method == http.MethodPost {
		body = strings.NewReader(values.Encode())
	} else if len(values) > 0 {
		endpoint += "?" + values.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return 0, errors.New("fixture request invalid")
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if admin.token != "" {
		request.Header.Set("accessToken", admin.token)
	}
	response, err := admin.client.Do(request)
	if err != nil {
		return 0, errors.New("fixture transport failed")
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 65537))
	if err != nil || len(raw) > 65536 {
		return response.StatusCode, errors.New("fixture response bound failed")
	}
	if json.Unmarshal(raw, target) != nil {
		return response.StatusCode, errors.New("fixture response invalid")
	}
	return response.StatusCode, nil
}
func (admin serviceAdmin) login(t testing.TB, ctx context.Context, username, password string) serviceAdmin {
	t.Helper()
	var reply struct {
		Token string `json:"accessToken"`
	}
	status, err := admin.call(ctx, http.MethodPost, "/v1/auth/users/login", url.Values{"username": {username}, "password": {password}}, &reply)
	if err != nil || status != 200 || reply.Token == "" {
		t.Fatalf("fixture login failed (HTTP %d)", status)
	}
	admin.token = reply.Token
	return admin
}

func TestAuthorizedNacosService(t *testing.T) {
	path := os.Getenv("FATHOMRY_CONFIGURATION_NACOS_TEST_CONFIG")
	if path == "" {
		t.Skip("no explicitly authorized isolated Nacos fixture")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("authorized fixture unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 64<<10 {
		t.Fatal("fixture permissions/bounds invalid")
	}
	decoder := json.NewDecoder(io.LimitReader(file, 64<<10+1))
	decoder.DisallowUnknownFields()
	var fixture serviceFixture
	if decoder.Decode(&fixture) != nil || !fixture.AllowWrites {
		t.Fatal("invalid fixture or writes not authorized")
	}
	var extra any
	if !errors.Is(decoder.Decode(&extra), io.EOF) {
		t.Fatal("trailing fixture content")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	trust := &tls.Config{MinVersion: tls.VersionTLS12}
	if fixture.RootCAPEM != "" {
		trust.RootCAs = x509.NewCertPool()
		if !trust.RootCAs.AppendCertsFromPEM([]byte(fixture.RootCAPEM)) {
			t.Fatal("fixture roots invalid")
		}
	}
	transport := &http.Transport{Proxy: nil, TLSClientConfig: trust, MaxConnsPerHost: 2, MaxIdleConnsPerHost: 2, ResponseHeaderTimeout: 5 * time.Second}
	t.Cleanup(transport.CloseIdleConnections)
	controller := serviceAdmin{client: &http.Client{Transport: transport, Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, url: strings.TrimRight(fixture.HTTPURL, "/")}
	admin := controller.login(t, ctx, fixture.AdminUsername, fixture.AdminPassword)
	var state adminReply
	status, err := admin.call(ctx, http.MethodGet, "/v3/admin/core/state", nil, &state)
	var version struct {
		Version string `json:"version"`
	}
	if err != nil || status != 200 || state.Code != 0 || json.Unmarshal(state.Data, &version) != nil || !regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+[-.A-Za-z0-9]*$`).MatchString(version.Version) {
		t.Fatal("service version evidence unavailable")
	}
	t.Logf("Observed Nacos %s; explicit authenticated test-owned-key profile.", version.Version)
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal("test identity unavailable")
	}
	prefix := "fathomry-gh96-" + hex.EncodeToString(nonce[:])
	ids := []string{prefix + ".json", prefix + ".empty", prefix + ".second"}
	settings := remote.Settings{Name: "service", Namespace: fixture.Namespace, Servers: []remote.Server{{HTTPURL: fixture.HTTPURL, GRPCAddress: fixture.GRPCAddress}},
		Username: fixture.Username, Password: fixture.Password, RootCAPEM: fixture.RootCAPEM, AllowInsecure: fixture.AllowInsecure,
		RequestTimeout: 5 * time.Second, RetryDelay: 100 * time.Millisecond, ReconcileInterval: 5 * time.Minute}
	for index, id := range ids {
		settings.Documents = append(settings.Documents, remote.Document{Name: []string{"document", "empty", "second"}[index], DataID: id})
	}
	reader, err := remote.Select(settings)
	if err != nil {
		t.Fatal("service selection rejected", err)
	}
	verifyAbsent := func(ctx context.Context, selected source.Selection) bool {
		batch, err := selected.Capture(ctx)
		if err != nil {
			return false
		}
		for _, document := range batch.Documents() {
			if document.Presence != source.Missing {
				return false
			}
		}
		return true
	}
	if !verifyAbsent(ctx, reader) {
		t.Fatal("test-owned key absence not established; nothing will be written")
	}
	t.Logf("Test-owned keys: %s, %s, %s", ids[0], ids[1], ids[2])
	parameters := func(id string) url.Values {
		return url.Values{"dataId": {id}, "groupName": {"DEFAULT_GROUP"}, "namespaceId": {fixture.Namespace}}
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 35*time.Second)
		defer stop()
		for _, id := range ids {
			var reply adminReply
			_, _ = admin.call(cleanup, http.MethodDelete, "/v3/admin/cs/config", parameters(id), &reply)
		}
		adminSettings := settings
		adminSettings.Name = "cleanup"
		adminSettings.Username = fixture.AdminUsername
		adminSettings.Password = fixture.AdminPassword
		independent, err := remote.Select(adminSettings)
		if err != nil {
			t.Error("cleanup selection failed")
			return
		}
		absent := false
		for cleanup.Err() == nil {
			if verifyAbsent(cleanup, reader) && verifyAbsent(cleanup, independent) {
				absent = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !absent {
			t.Error("test-owned configuration deletion NOT verified")
			return
		}
		t.Log("Cleanup verified: all three generated keys are Missing through fresh reader and admin public Captures.")
	})
	mutate := func(actor serviceAdmin, method, id, content string) (int, int, error) {
		values := parameters(id)
		if method == http.MethodPost {
			values.Set("content", content)
			values.Set("type", "text")
		}
		var reply adminReply
		status, err := actor.call(ctx, method, "/v3/admin/cs/config", values, &reply)
		return status, reply.Code, err
	}
	publish := func(id, content string) {
		t.Helper()
		status, code, err := mutate(admin, http.MethodPost, id, content)
		if err != nil || status != 200 || code != 0 {
			t.Fatalf("authorized test publication refused (HTTP %d, code %d)", status, code)
		}
	}
	publish(ids[0], "format: 1\nproject: {count: 51, labels: {temporary: present}}")
	publish(ids[2], "native second document")
	emptyStatus, emptyCode, emptyErr := mutate(admin, http.MethodPost, ids[1], "")
	if emptyErr != nil {
		t.Fatal("empty publication result unknown")
	}
	if emptyStatus != 200 || emptyCode != 0 {
		t.Logf("Service refuses empty publication (HTTP %d, code %d); present-empty acquisition remains loopback-qualified.", emptyStatus, emptyCode)
	}
	eventual(t, func() bool {
		batch, err := reader.Capture(ctx)
		if err != nil {
			return false
		}
		raw, presence, _ := batch.RawCopy("document")
		return presence == source.Present && strings.Contains(string(raw), "51")
	})
	batch, err := reader.Capture(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if raw, presence, _ := batch.RawCopy("empty"); emptyStatus == 200 && emptyCode == 0 {
		if presence != source.Present || len(raw) != 0 {
			t.Fatal("successful service empty content lost")
		}
	} else if presence != source.Missing {
		t.Fatal("refused empty publication changed source")
	}
	finiteSettings := settings
	finiteSettings.Documents = settings.Documents[:1]
	finite, err := remote.Select(finiteSettings)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := c.Load(ctx, schema(), remotePlan(finite))
	if err != nil {
		t.Fatal("service Load failed", err)
	}
	value, _ := snapshot.ValueCopy()
	if value.Project.Count != 51 {
		t.Fatal("service Load data mismatched")
	}

	// The forwarding fixture affects only this test's owned gRPC connections.
	// No remote listener, service configuration, account or process is changed.
	if !fixture.AllowInsecure {
		t.Fatal("this reconnect fixture requires the explicitly selected plaintext service profile")
	}
	relay := newServiceRelay(t, fixture.GRPCAddress)
	liveSettings := finiteSettings
	liveSettings.Servers = append([]remote.Server(nil), finiteSettings.Servers...)
	liveSettings.Servers[0].GRPCAddress = relay.listener.Addr().String()
	selected, err := remote.Select(liveSettings)
	if err != nil {
		t.Fatal(err)
	}
	observer, err := selected.Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeOwner(t, observer.Close) })
	live, err := c.Watch(ctx, schema(), remotePlan(selected))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeOwner(t, live.Close) })
	awaitRaw(t, observer, func(state source.State) bool { return state.Status == source.Available })
	first := await(t, live, func(state c.State[project]) bool { return state.Status == c.Ready })
	publish(ids[0], "format: 1\nproject: {count: 52}")
	awaitRaw(t, observer, func(state source.State) bool {
		if state.Batch == nil {
			return false
		}
		raw, _, _ := state.Batch.RawCopy("document")
		return state.Status == source.Available && strings.Contains(string(raw), "52")
	})
	await(t, live, func(state c.State[project]) bool {
		value, err := state.Snapshot.ValueCopy()
		return err == nil && state.Status == c.Ready && value.Project.Count == 52
	})
	t.Log("Public direct Observe and Framework Watch received native push before the five-minute reconciliation interval.")
	publish(ids[0], "{malformed")
	invalid := await(t, live, func(state c.State[project]) bool { return state.Status == c.Degraded })
	retained, _ := invalid.Snapshot.ValueCopy()
	if retained.Project.Count != 52 {
		t.Fatal("service invalid update replaced last accepted value")
	}
	publish(ids[0], "format: 1\nproject: {count: 53}")
	await(t, live, func(state c.State[project]) bool {
		value, err := state.Snapshot.ValueCopy()
		return err == nil && state.Status == c.Ready && value.Project.Count == 53 && len(value.Project.Labels) == 1
	})
	relay.disconnect()
	await(t, live, func(state c.State[project]) bool { return state.Status == c.Degraded })
	awaitRaw(t, observer, func(state source.State) bool { return state.Status == source.Degraded })
	publish(ids[0], "format: 1\nproject: {count: 54}")
	relay.resume()
	await(t, live, func(state c.State[project]) bool {
		value, err := state.Snapshot.ValueCopy()
		return err == nil && state.Status == c.Ready && value.Project.Count == 54
	})
	awaitRaw(t, observer, func(state source.State) bool {
		if state.Batch == nil {
			return false
		}
		raw, _, _ := state.Batch.RawCopy("document")
		return state.Status == source.Available && strings.Contains(string(raw), "54")
	})
	t.Log("Owned disconnection recovered by native re-registration/resynchronization without another publication.")
	status, code, err := mutate(admin, http.MethodDelete, ids[0], "")
	if err != nil || status != 200 || code != 0 {
		t.Fatal("test deletion failed")
	}
	missing := await(t, live, func(state c.State[project]) bool {
		return state.Status == c.Degraded && errors.Is(state.Failure, c.ErrMissing)
	})
	retained, _ = missing.Snapshot.ValueCopy()
	if retained.Project.Count != 54 {
		t.Fatal("required service deletion erased last value")
	}
	optionalPlan := remotePlan(finite)
	optionalPlan.Inputs[0].Documents[0].Optional = true
	optional, err := c.Watch(ctx, schema(), optionalPlan)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeOwner(t, optional.Close) })
	inherited := await(t, optional, func(state c.State[project]) bool { return state.Status == c.Ready })
	inheritedValue, _ := inherited.Snapshot.ValueCopy()
	if inheritedValue.Project.Count != 7 {
		t.Fatal("optional missing did not inherit defaults")
	}
	publish(ids[0], "format: 1\nproject: {count: 55}")
	await(t, live, func(state c.State[project]) bool {
		value, err := state.Snapshot.ValueCopy()
		return err == nil && state.Status == c.Ready && value.Project.Count == 55
	})
	anonymous := finiteSettings
	anonymous.Name = "anonymous"
	anonymous.Username = ""
	anonymous.Password = ""
	untrusted, err := remote.Select(anonymous)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := untrusted.Capture(ctx); !errors.Is(err, remote.ErrDenied) {
		t.Fatal("unauthenticated service read not denied", err)
	}
	readerAdmin := controller.login(t, ctx, fixture.Username, fixture.Password)
	status, code, err = mutate(readerAdmin, http.MethodPost, ids[0], "unauthorized-test-content")
	if err != nil || status == 200 && code == 0 {
		t.Fatal("read-only credential unexpectedly published or denial unknown")
	}
	t.Log("Anonymous native read and read-only admin-API write were denied.")
	original, _ := first.Snapshot.ValueCopy()
	if original.Project.Count != 51 {
		t.Fatal("service updates changed old snapshot")
	}
	closeOwner(t, optional.Close)
	closeOwner(t, live.Close)
	closeOwner(t, observer.Close)
	t.Log("Real-service public Capture/Observe/Load/Watch and explicit cleanup ownership passed.")
}

type relayPair struct{ incoming, outgoing net.Conn }
type serviceRelay struct {
	listener net.Listener
	target   string
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	blocked  bool
	pairs    map[*relayPair]bool
	workers  sync.WaitGroup
}

func newServiceRelay(t testing.TB, target string) *serviceRelay {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("relay bind failed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	relay := &serviceRelay{listener: listener, target: target, ctx: ctx, cancel: cancel, pairs: make(map[*relayPair]bool)}
	relay.workers.Go(func() {
		for {
			incoming, err := listener.Accept()
			if err != nil {
				return
			}
			relay.mu.Lock()
			if relay.blocked || len(relay.pairs) >= 16 || ctx.Err() != nil {
				relay.mu.Unlock()
				incoming.Close()
				continue
			}
			pair := &relayPair{incoming: incoming}
			relay.pairs[pair] = true
			relay.mu.Unlock()
			relay.workers.Go(func() { relay.forward(pair) })
		}
	})
	t.Cleanup(func() { cancel(); listener.Close(); relay.disconnect(); relay.workers.Wait() })
	return relay
}
func (relay *serviceRelay) forward(pair *relayPair) {
	defer func() { relay.mu.Lock(); delete(relay.pairs, pair); relay.mu.Unlock(); pair.incoming.Close() }()
	outgoing, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(relay.ctx, "tcp", relay.target)
	if err != nil {
		return
	}
	defer outgoing.Close()
	relay.mu.Lock()
	pair.outgoing = outgoing
	blocked := relay.blocked || relay.ctx.Err() != nil
	relay.mu.Unlock()
	if blocked {
		return
	}
	copied := make(chan struct{})
	go func() {
		_, _ = io.Copy(outgoing, pair.incoming)
		outgoing.Close()
		pair.incoming.Close()
		close(copied)
	}()
	_, _ = io.Copy(pair.incoming, outgoing)
	outgoing.Close()
	pair.incoming.Close()
	<-copied
}
func (relay *serviceRelay) disconnect() {
	relay.mu.Lock()
	defer relay.mu.Unlock()
	relay.blocked = true
	for pair := range relay.pairs {
		pair.incoming.Close()
		if pair.outgoing != nil {
			pair.outgoing.Close()
		}
	}
}
func (relay *serviceRelay) resume() { relay.mu.Lock(); relay.blocked = false; relay.mu.Unlock() }
