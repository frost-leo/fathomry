//go:build nacos_service

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
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
	request "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_request"
	response "github.com/nacos-group/nacos-sdk-go/v2/common/remote/rpc/rpc_response"
)

type serviceFixture struct {
	HTTPURL       string `json:"http_url"`
	GRPCAddress   string `json:"grpc_address"`
	Namespace     string `json:"namespace"`
	Username      string `json:"username"`
	Password      string `json:"password"`
	AdminUsername string `json:"admin_username"`
	AdminPassword string `json:"admin_password"`
	RootCAPEM     string `json:"root_ca_pem"`
	AllowInsecure bool   `json:"allow_insecure"`
	AllowWrites   bool   `json:"allow_writes"`
}

func loadServiceFixture(t *testing.T) serviceFixture {
	t.Helper()
	path := os.Getenv("FATHOMRY_NACOS_TEST_CONFIG")
	if path == "" {
		t.Fatal("set FATHOMRY_NACOS_TEST_CONFIG to the authorized private fixture")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("private service fixture unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 64<<10 {
		t.Fatal("service fixture permissions or bounds invalid")
	}
	decoder := json.NewDecoder(io.LimitReader(file, 64<<10+1))
	decoder.DisallowUnknownFields()
	var fixture serviceFixture
	if decoder.Decode(&fixture) != nil {
		t.Fatal("invalid service fixture")
	}
	var extra any
	if !errors.Is(decoder.Decode(&extra), io.EOF) || !fixture.AllowWrites {
		t.Fatal("isolated service writes were not explicitly authorized")
	}
	return fixture
}

func TestNacosServiceReadWriteWatchAndCleanup(t *testing.T) {
	fixture := loadServiceFixture(t)
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal("fixture identity failed")
	}
	selected := KeyV1{Group: "DEFAULT_GROUP", DataID: "gh21-" + hex.EncodeToString(nonce[:]) + ".json"}
	additional := KeyV1{Group: "DEFAULT_GROUP", DataID: selected.DataID + ".extra"}
	input := OptionsV1{Name: "service-reader", Namespace: fixture.Namespace, Servers: []ServerV1{{HTTPURL: fixture.HTTPURL, GRPCAddress: fixture.GRPCAddress}},
		Keys: []KeyV1{selected, additional}, Username: fixture.Username, Password: fixture.Password, RootCAPEM: fixture.RootCAPEM,
		AllowInsecure: fixture.AllowInsecure, RequestTimeout: 10 * time.Second, ReconcileInterval: 5 * time.Minute, RetryDelay: 100 * time.Millisecond}
	adminInput := input
	adminInput.Name = "service-admin"
	adminInput.Username, adminInput.Password = fixture.AdminUsername, fixture.AdminPassword
	reader, admin := openClient(t, input), openClient(t, adminInput)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	mutateAs := func(ctx context.Context, actor *Client, value request.IRequest, kind string) error {
		work, end, err := actor.enter(ctx)
		if err != nil {
			return err
		}
		defer end()
		current, err := actor.newSession(work, 0, nil)
		if err != nil {
			return err
		}
		defer current.close()
		token, err := actor.token(work, 0)
		if err != nil {
			return err
		}
		payload, err := current.unary.Request(work, actor.envelope(value, token))
		if err != nil {
			return current.failure("fixture-write", err)
		}
		raw, err := payloadBody(payload)
		if err != nil {
			return err
		}
		if payload.Metadata.Type != kind && payload.Metadata.Type != "ErrorResponse" {
			return fail(ErrDecode, "fixture-write-type")
		}
		var decoded response.Response
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return err
		}
		if decoded.ResultCode != 200 || decoded.ErrorCode != 0 {
			if decoded.ErrorCode == 401 || decoded.ErrorCode == 403 {
				return fail(ErrDenied, "fixture-write", &RemoteError{resultCode: decoded.ResultCode, errorCode: decoded.ErrorCode, message: decoded.Message})
			}
			return fail(ErrUnavailable, "fixture-write", &RemoteError{resultCode: decoded.ResultCode, errorCode: decoded.ErrorCode, message: decoded.Message})
		}
		if payload.Metadata.Type != kind {
			return fail(ErrDecode, "fixture-write-type")
		}
		return nil
	}
	mutate := func(ctx context.Context, value request.IRequest, kind string) error {
		return mutateAs(ctx, admin, value, kind)
	}
	publish := func(selected KeyV1, content string) {
		t.Helper()
		value := request.NewConfigPublishRequest(selected.Group, selected.DataID, fixture.Namespace, content, "")
		value.AdditionMap["type"] = "json"
		if err := mutate(ctx, value, "ConfigPublishResponse"); err != nil {
			t.Fatal("authorized fixture publication failed", err)
		}
		converged, stop := context.WithTimeout(ctx, 20*time.Second)
		defer stop()
		for converged.Err() == nil {
			observed, err := reader.Read(converged, selected)
			if err == nil && string(observed.RawCopy()) == content {
				return
			}
			if !pause(converged, 100*time.Millisecond) {
				break
			}
		}
		t.Fatal("published fixture did not become observable through the reader")
	}
	// Establish absence before making the generated keys this test's cleanup target.
	for _, target := range []KeyV1{selected, additional} {
		if value, err := admin.Read(ctx, target); value != nil || !errors.Is(err, ErrMissing) {
			t.Fatal("fixture key absence not established", err)
		}
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		for _, target := range []KeyV1{selected, additional} {
			if err := mutate(cleanup, request.NewConfigRemoveRequest(target.Group, target.DataID, fixture.Namespace), "ConfigRemoveResponse"); err != nil {
				t.Error("fixture deletion failed", err)
				continue
			}
			absent := false
			for cleanup.Err() == nil {
				value, err := admin.Read(cleanup, target)
				if value == nil && errors.Is(err, ErrMissing) {
					absent = true
					break
				}
				if !pause(cleanup, 100*time.Millisecond) {
					break
				}
			}
			if !absent {
				t.Error("fixture deletion not observed within cleanup budget")
			}
		}
		t.Log("Generated fixture deletion and absence checks completed.")
	})
	initial := `{"host":"initial","labels":{"X-Case":"kept"},"optional":null,"items":[],"large":18446744073709551615}`
	publish(selected, initial)
	publish(additional, `{"host":"second"}`)
	document, err := reader.Read(ctx, selected)
	if err != nil || string(document.RawCopy()) != initial {
		t.Fatal("real raw read differed from published content", err)
	}
	prepared, err := resource.Prepare(resource.Schema[applicationSettings]{Format: 1}, resource.Input{Identity: resource.Identity{Provider: "service.fixture", Name: "application"}, Format: 1,
		Layers: []resource.Layer{{Kind: resource.Base, Content: document.RawCopy()}}})
	if err != nil || inspectPrepared(t, prepared).Large != ^uint64(0) {
		t.Fatal("real content did not preserve preparation semantics", err)
	}
	if values, err := reader.ReadAll(ctx); err != nil || len(values) != 2 {
		t.Fatal("multiple required keys failed", err)
	}
	unauthorized := request.NewConfigPublishRequest(selected.Group, selected.DataID, fixture.Namespace, "unauthorized-write-canary", "")
	if err := mutateAs(ctx, reader, unauthorized, "ConfigPublishResponse"); !errors.Is(err, ErrDenied) {
		t.Fatal("read-only credential was allowed to publish", err)
	}
	anonymousInput := input
	anonymousInput.Name, anonymousInput.Username, anonymousInput.Password = "service-anonymous", "", ""
	if value, err := openClient(t, anonymousInput).Read(ctx, selected); value != nil || !errors.Is(err, ErrDenied) {
		t.Fatal("native gRPC accepted an unauthenticated query", err)
	}
	t.Log("Native gRPC anonymous-read and read-only-writer denial checks passed.")
	t.Log("Real authenticated raw reads, multiple keys and exact preparation passed.")
	subscription, err := reader.Watch(ctx)
	if err != nil {
		t.Fatal("real watch setup failed", err)
	}
	waitChange := func(predicate func(Change) bool) {
		t.Helper()
		wait, stop := context.WithTimeout(ctx, 30*time.Second)
		defer stop()
		for {
			change, err := subscription.Next(wait)
			if err != nil {
				t.Fatal("real notification not observed", err)
			}
			if change.Err() != nil {
				t.Fatal("real observation failed", change.Err())
			}
			if predicate(change) {
				return
			}
		}
	}
	waitChange(func(change Change) bool { return change.Resync() })
	updated := `{"host":"updated","labels":{},"optional":"new","items":["one"],"large":0}`
	publish(selected, updated)
	waitChange(func(change Change) bool { return change.Key().DataID == selected.DataID })
	latest, err := reader.Read(ctx, selected)
	if err != nil || string(latest.RawCopy()) != updated || string(document.RawCopy()) != initial || inspectPrepared(t, prepared).Host != "initial" {
		t.Fatal("change acquisition or frozen preparation failed", err)
	}
	t.Log("Real native change delivery and frozen-preparation checks passed.")
	// Drop only this client's connection to the real service, not the remote node.
	reader.mu.Lock()
	var sessions []*session
	for current := range reader.sessions {
		sessions = append(sessions, current)
	}
	reader.mu.Unlock()
	for _, current := range sessions {
		_ = current.conn.Close()
	}
	wait, stop := context.WithTimeout(ctx, 30*time.Second)
	gapSeen, recovered := false, false
	for !recovered {
		change, err := subscription.Next(wait)
		if err != nil {
			stop()
			t.Fatal("real session recovery failed", err)
		}
		if change.Err() != nil {
			if !change.Resync() {
				stop()
				t.Fatal("stream gap was concealed")
			}
			gapSeen = true
		} else if change.Resync() && gapSeen {
			recovered = true
		}
	}
	stop()
	publish(selected, initial)
	waitChange(func(change Change) bool { return change.Key().DataID == selected.DataID })
	if !gapSeen {
		t.Fatal("connection replacement hid observation gap")
	}
	if err := subscription.Close(ctx); err != nil {
		t.Fatal("real subscription cleanup failed", err)
	}
	t.Log("Real stream interruption, explicit gap and re-registration passed.")
	badInput := input
	badInput.Name = "service-denied"
	badInput.Password = "deliberately-invalid-password"
	bad := openClient(t, badInput)
	if value, err := bad.Read(ctx, selected); value != nil || !errors.Is(err, ErrDenied) {
		t.Fatal("invalid credentials were not denied", err)
	}
	empty := request.NewConfigPublishRequest(selected.Group, selected.DataID, fixture.Namespace, "", "")
	empty.AdditionMap["type"] = "json"
	if err := mutate(ctx, empty, "ConfigPublishResponse"); err != nil {
		var native *RemoteError
		if !errors.As(err, &native) {
			t.Fatal("empty-publication result was not observed", err)
		}
		t.Logf("Native empty-publication refusal: result=%d error=%d", native.ResultCode(), native.ErrorCode())
		if native.ErrorCode() != 400 {
			t.Fatal("unexpected native empty-publication refusal")
		}
		if value, err := reader.Read(ctx, selected); err != nil || string(value.RawCopy()) != initial {
			t.Fatal("rejected empty publication changed previous content", err)
		}
		t.Log("Service rejected empty publication; successful-empty query handling remains a local-fixture check.")
	} else if value, err := reader.Read(ctx, selected); value != nil || !errors.Is(err, ErrEmpty) {
		t.Fatal("empty content became missing or usable", err)
	}
	publish(selected, "{malformed")
	invalid, err := reader.Read(ctx, selected)
	if err != nil {
		t.Fatal("raw acquisition normalized malformed input", err)
	}
	if string(invalid.RawCopy()) != "{malformed" {
		t.Fatal("malformed publication was not the acquired input")
	}
	rejected, err := resource.Prepare(resource.Schema[applicationSettings]{Format: 1}, resource.Input{Identity: resource.Identity{Provider: "service.fixture", Name: "invalid"}, Format: 1,
		Layers: []resource.Layer{{Kind: resource.Base, Content: invalid.RawCopy()}}})
	if !errors.Is(err, resource.ErrConfiguration) || rejected.Description().Revision != "" {
		t.Fatal("malformed remote input produced usable preparation")
	}
	publish(selected, initial)
	if err := mutate(ctx, request.NewConfigRemoveRequest(additional.Group, additional.DataID, fixture.Namespace), "ConfigRemoveResponse"); err != nil {
		t.Fatal("required-key deletion failed", err)
	}
	deleted, stopDelete := context.WithTimeout(ctx, 20*time.Second)
	defer stopDelete()
	for deleted.Err() == nil {
		if value, err := reader.Read(deleted, additional); value == nil && errors.Is(err, ErrMissing) {
			break
		}
		if !pause(deleted, 100*time.Millisecond) {
			break
		}
	}
	if values, err := reader.ReadAll(ctx); values != nil || !errors.Is(err, ErrMissing) {
		t.Fatal("deleted required key exposed stale/partial success", err)
	}
	if strings.TrimSpace(document.MD5()) == "" {
		t.Fatal("native content marker absent")
	}
	t.Log("Real denial, malformed content, deletion and fail-closed batch checks passed.")
	t.Log("Service fixture keys will be removed and checked absent before client cleanup.")
}

func TestNacosManagementService(t *testing.T) {
	fixture := loadServiceFixture(t)
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal("fixture identity failed")
	}
	selected := KeyV1{Group: "DEFAULT_GROUP", DataID: "gh98-capability-" + hex.EncodeToString(nonce[:]) + ".toml"}
	options := OptionsV1{Name: "capability-admin", Namespace: fixture.Namespace,
		Servers:     []ServerV1{{HTTPURL: fixture.HTTPURL, GRPCAddress: fixture.GRPCAddress}},
		DynamicKeys: true, Writable: true, Username: fixture.AdminUsername, Password: fixture.AdminPassword,
		RootCAPEM: fixture.RootCAPEM, AllowInsecure: fixture.AllowInsecure, RequestTimeout: 10 * time.Second, ReconcileInterval: 30 * time.Second}
	client := openClient(t, options)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	existing, err := client.ReadRaw(ctx, selected)
	if err != nil || !existing.Missing() {
		t.Fatal("unique test key was not confirmed absent")
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		_, removeErr := client.Delete(cleanup, selected)
		for cleanup.Err() == nil {
			document, readErr := client.ReadRaw(cleanup, selected)
			if readErr == nil && document.Missing() {
				t.Log("Isolated management fixture cleanup confirmed.")
				return
			}
			if !pause(cleanup, 100*time.Millisecond) {
				break
			}
		}
		if removeErr != nil {
			t.Error("isolated cleanup deletion did not complete")
		}
		t.Error("isolated management fixture absence not confirmed")
	})
	initial := "value=1\n"
	result, err := client.Publish(ctx, PublishInputV1{Key: selected, Content: initial, ContentType: "toml", ConfigTags: "capability-proof"})
	if err != nil || result.State() != MutationAcknowledged {
		t.Fatal("real publication not acknowledged", err)
	}
	await := func(content string) *Document {
		t.Helper()
		for ctx.Err() == nil {
			value, err := client.ReadRaw(ctx, selected)
			if err == nil && !value.Missing() && string(value.RawCopy()) == content {
				return value
			}
			if !pause(ctx, 100*time.Millisecond) {
				break
			}
		}
		t.Fatal("expected isolated content not observed")
		return nil
	}
	first := await(initial)
	subscription, err := client.WatchKeys(ctx, []KeyV1{selected})
	if err != nil {
		t.Fatal("dynamic watch failed", err)
	}
	defer subscription.Close(context.Background())
	ready, stop := context.WithTimeout(ctx, 15*time.Second)
	change, err := subscription.Next(ready)
	stop()
	if err != nil || change.Err() != nil || !change.Resync() {
		t.Fatal("initial dynamic watch registration failed")
	}
	updated := "value=2\n"
	result, err = client.Publish(ctx, PublishInputV1{Key: selected, Content: updated, ContentType: "toml", CASMD5: first.MD5(), ConfigTags: "capability-proof"})
	if err != nil || result.State() != MutationAcknowledged {
		t.Fatal("real CAS update failed", err)
	}
	await(updated)
	wait, stop := context.WithTimeout(ctx, 15*time.Second)
	change, err = subscription.Next(wait)
	stop()
	if err != nil || change.Err() != nil || change.Key().DataID != selected.DataID {
		t.Fatal("real dynamic invalidation not observed")
	}
	result, err = client.Publish(ctx, PublishInputV1{Key: selected, Content: "value=3\n", CASMD5: first.MD5(), ConfigTags: "capability-proof"})
	var remote *RemoteError
	if err == nil || !errors.As(err, &remote) {
		t.Fatal("stale CAS was not refused with native evidence")
	}
	t.Logf("Stale CAS native code=%d, mutation state=%d.", remote.ErrorCode(), result.State())
	await(updated)
	page, err := client.Search(ctx, SearchInputV1{Mode: "accurate", DataID: selected.DataID, Group: selected.Group, ConfigTags: "capability-proof", PageSize: 5})
	if err != nil || page.Total() != 1 || len(page.ItemsCopy()) != 1 {
		token, tokenErr := client.token(ctx, 0)
		if tokenErr == nil {
			query := url.Values{"search": {"accurate"}, "dataId": {selected.DataID}, "groupName": {selected.Group}, "namespaceId": {fixture.Namespace}, "pageNo": {"1"}, "pageSize": {"5"}, "accessToken": {token}}
			raw, _, probeErr := client.searchRequest(ctx, 0, "/v3/admin/cs/config/list", query)
			if probeErr == nil {
				var shape struct {
					Data struct {
						PageItems []map[string]json.RawMessage `json:"pageItems"`
					} `json:"data"`
				}
				if json.Unmarshal(raw, &shape) == nil && len(shape.Data.PageItems) > 0 {
					var names []string
					for name := range shape.Data.PageItems[0] {
						names = append(names, name)
					}
					slices.Sort(names)
					t.Logf("Isolated search item field names: %v", names)
				}
			}
		}
		t.Fatal("real isolated search failed", err)
	}
	item := page.ItemsCopy()[0]
	if item.Key().DataID != selected.DataID || item.Key().Group != selected.Group || item.MD5() != checksum(updated) {
		t.Fatal("search returned unexpected isolated metadata")
	}
	if item.ContentPresent() && string(item.RawCopy()) != updated {
		t.Fatal("search returned unexpected isolated content")
	}
	page, err = client.Search(ctx, SearchInputV1{Mode: "accurate", DataID: selected.DataID, Group: selected.Group, ConfigTags: "unmatched-capability-proof", PageSize: 5})
	if err != nil || page.Total() != 0 || len(page.ItemsCopy()) != 0 {
		t.Fatal("native search ignored config-tag filter", err)
	}
	result, err = client.Delete(ctx, selected)
	if err != nil || result.State() != MutationAcknowledged {
		t.Fatal("real deletion not acknowledged", err)
	}
	t.Log("Native publish, CAS, dynamic read/watch, exact search and delete passed.")
}
