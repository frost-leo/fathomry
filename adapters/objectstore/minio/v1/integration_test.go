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

package minio

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/objectstore/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	sdk "github.com/minio/minio-go/v7"
)

func TestDirectCapabilitiesAndIndependentEvidence(t *testing.T) {
	peer := newPeer(t)
	test := newFixture(t, peer.options)
	ctx := testContext(t)
	client, err := test.client.WithID("public-id")
	if err != nil {
		t.Fatal(err)
	}
	test.client = client
	put := test.result(client.Put(ctx, ctx, WriteRequest{Key: "owned/a", Size: -1, IfAbsent: true, Metadata: map[string]string{"name": "value"}}, strings.NewReader("payload")))
	if put.Transfer().Effect != objectstore.Acknowledged || !bytes.Equal(peer.content("owned/a"), []byte("payload")) || put.Attribution().ID != "public-id" || put.Source().Revision == "" {
		t.Fatal("write evidence")
	}
	read := test.result(client.Read(ctx, ReadRequest{Address: Address{Key: "owned/a"}}))
	if !bytes.Equal(read.DataCopy(), []byte("payload")) {
		t.Fatal("read")
	}
	changed := read.DataCopy()
	changed[0] = 'X'
	if bytes.Equal(changed, read.DataCopy()) {
		t.Fatal("result alias")
	}
	var output bytes.Buffer
	test.result(client.Download(ctx, ReadRequest{Address: Address{Key: "owned/a"}}, &output))
	if output.String() != "payload" {
		t.Fatal("download")
	}
	info := test.result(client.Stat(ctx, Address{Key: "owned/a"}))
	object, present := info.Object()
	if !present || object.Size != 7 {
		t.Fatal("stat")
	}
	test.result(client.Copy(ctx, CopyRequest{Source: Address{Key: "owned/a"}, Key: "owned/b"}))
	test.result(client.SetTags(ctx, Address{Key: "owned/a"}, map[string]string{"purpose": "test"}))
	tags := test.result(client.GetTags(ctx, Address{Key: "owned/a"}))
	if tags.TagsCopy()["purpose"] != "test" {
		t.Fatal("tags")
	}
	test.result(client.SetTags(ctx, Address{Key: "owned/a"}, nil))
	listing := test.result(client.List(ctx, ListRequest{Prefix: "owned/"}))
	if len(listing.ObjectsCopy()) != 2 || !listing.Complete() {
		t.Fatal("list")
	}
	before := peer.requests()
	signed := test.result(client.Presign(ctx, SignRequest{Method: GET, Address: Address{Key: "owned/a"}, Expiry: time.Minute}))
	capability, present := signed.Delegation()
	if !present || capability.URL() == "" || signed.Transfer().Effect != objectstore.NotSubmitted || peer.requests() != before {
		t.Fatal("delegation")
	}
	receipt, err := client.Put(ctx, ctx, WriteRequest{Key: "owned/a", Size: 1, IfAbsent: true}, strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil || !errors.Is(snapshot.Err(), ErrCondition) {
		t.Fatal("condition", snapshot.Err())
	}
	var response sdk.ErrorResponse
	if !errors.As(snapshot.Err(), &response) || response.Code != "PreconditionFailed" {
		t.Fatal("native error identity")
	}
	delivery, err := test.inbox.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := delivery.Retry(); err != nil {
		t.Fatal(err)
	}
	requests := peer.requests()
	retried, err := test.inbox.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	observed, _ := retried.Receipt()
	again, _ := observed.WaitReleased(ctx)
	if again.Info().Sequence != snapshot.Info().Sequence || peer.requests() != requests {
		t.Fatal("evidence retry dispatched I/O")
	}
	if err := retried.Ack(); err != nil {
		t.Fatal(err)
	}
	removals := test.result(client.Remove(ctx, []Address{{Key: "owned/a"}, {Key: "owned/b"}}))
	if len(removals.RemovalsCopy()) != 2 {
		t.Fatal("removals")
	}
}

func TestPublicMultipartInspectionAndIncrementalListing(t *testing.T) {
	peer := newPeer(t)
	test := newFixture(t, peer.options)
	ctx := testContext(t)
	setup, cancel := context.WithCancel(ctx)
	session, root, err := test.client.BeginMultipart(setup, ctx, ctx, WriteRequest{Key: "owned/session", Size: 3, IfAbsent: true})
	if err != nil || session == nil {
		t.Fatal("begin", err)
	}
	cancel()
	test.result(session.Part(ctx, 1, 3, strings.NewReader("abc")))
	uploads := test.result(test.client.ListUploads(ctx, UploadQuery{Prefix: "owned/"})).UploadsCopy()
	if len(uploads) != 1 {
		t.Fatal("uploads")
	}
	parts := test.result(test.client.ListParts(ctx, uploads[0], 0))
	if len(parts.PartsCopy()) != 1 {
		t.Fatal("parts")
	}
	result := test.result(session.Complete(ctx))
	if result.Transfer().Effect != objectstore.Acknowledged || string(peer.content("owned/session")) != "abc" {
		t.Fatal("complete")
	}
	if root != session.Receipt() {
		first, _ := root.Snapshot()
		second, _ := session.Receipt().Snapshot()
		if first.Info().Sequence != second.Info().Sequence {
			t.Fatal("root changed")
		}
	}
	cursor, _, err := test.client.Enumerate(ctx, ctx, ListRequest{Prefix: "owned/"})
	if err != nil || cursor == nil {
		t.Fatal("enumerate", err)
	}
	page := test.result(cursor.Next(ctx))
	if !page.Complete() || len(page.ObjectsCopy()) != 1 {
		t.Fatal("incremental list")
	}
	final, err := cursor.Receipt().WaitReleased(ctx)
	if err != nil || final.Err() != nil {
		t.Fatal("list final", err)
	}
	session, _, err = test.client.BeginMultipart(ctx, ctx, ctx, WriteRequest{Key: "owned/abort", Size: -1})
	if err != nil || session == nil {
		t.Fatal(err)
	}
	aborted := test.result(session.Abort(ctx))
	if !aborted.Transfer().AbortAcknowledged {
		t.Fatal("abort")
	}
	uploads = test.result(test.client.ListUploads(ctx, UploadQuery{Prefix: "owned/"})).UploadsCopy()
	if len(uploads) != 0 {
		t.Fatal("orphan")
	}
}

func TestFrameworkFixedFollowAndRetainedGeneration(t *testing.T) {
	firstPeer, secondPeer := newPeer(t), newPeer(t)
	first, second := firstPeer.options, secondPeer.options
	first.Name = "first"
	second.Name = "second"
	policy, err := Recommend(first)
	if err != nil {
		t.Fatal(err)
	}
	policy, err = policy.ForGenerations(4)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := framework.New(context.Background(), framework.Options{Operations: policy.Runtime})
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	var ownersMu sync.Mutex
	var owners []*Owner
	released := make(chan *Owner, 8)
	t.Cleanup(func() {
		if err := runtime.Close(testContext(t)); err != nil {
			t.Error(err)
		}
		ownersMu.Lock()
		defer ownersMu.Unlock()
		for _, owner := range owners {
			if !owner.ShutdownComplete() {
				t.Error("source not released")
			}
		}
		drain(t, inbox)
	})
	refusal := errors.New("candidate-refused")
	bind := func(name string, policy resource.Policy) resource.Ref[Handle] {
		ref, err := resource.Bind(runtime.Resources(), resource.Binding[Settings, Handle]{
			Name: name, Policy: policy, MaxGenerations: 3,
			Select: func(view settings.View) (Settings, error) {
				snapshot, err := settings.As[Settings](view)
				if err != nil {
					return Settings{}, err
				}
				return snapshot.ValueCopy()
			},
			Clone: func(value Settings) Settings { return value }, Equal: func(a, b Settings) bool { return a == b },
			Build: func(ctx context.Context, value Settings) (*resource.Instance[Handle], error) {
				owner, err := Open(ctx, value, dependencies)
				if owner == nil {
					return nil, err
				}
				ownersMu.Lock()
				owners = append(owners, owner)
				ownersMu.Unlock()
				var once sync.Once
				instance := &resource.Instance[Handle]{Value: owner.Handle(), Release: func(ctx context.Context) resource.ReleaseResult {
					result := owner.Release(ctx)
					if result.Complete {
						once.Do(func() { released <- owner })
					}
					return result
				}}
				if value.Name == "refused" {
					return instance, errors.Join(err, refusal)
				}
				return instance, err
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	fixedRef, followRef := bind("fixed", resource.Fixed), bind("follow", resource.Follow)
	apply := func(value Settings) error {
		snapshot, err := settings.New(value, func(value Settings) Settings { return value })
		if err != nil {
			return err
		}
		update, err := runtime.Resources().Apply(testContext(t), snapshot.View())
		if err != nil {
			return err
		}
		return update.Wait(testContext(t))
	}
	if err := apply(first); err != nil {
		t.Fatal(err)
	}
	fixed, err := Using(context.Background(), fixedRef, policy.Budget, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	follow, err := Using(context.Background(), followRef, policy.Budget, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	follow, err = follow.WithID("frozen-root")
	if err != nil {
		t.Fatal(err)
	}
	setup, cancel := context.WithCancel(testContext(t))
	session, root, err := follow.BeginMultipart(setup, testContext(t), testContext(t), WriteRequest{Key: "owned/old", Size: 3})
	if err != nil || session == nil {
		t.Fatal("old begin", err)
	}
	cancel()
	if err := apply(second); err != nil {
		t.Fatal(err)
	}
	fixture := &fixture{t: t, inbox: inbox}
	fixture.result(session.Part(testContext(t), 1, 3, strings.NewReader("old")))
	fixture.result(follow.Put(testContext(t), testContext(t), WriteRequest{Key: "owned/new", Size: 3}, strings.NewReader("new")))
	fixture.result(fixed.Put(testContext(t), testContext(t), WriteRequest{Key: "owned/fixed", Size: 5}, strings.NewReader("fixed")))
	rejected := second
	rejected.Name = "refused"
	if err := apply(rejected); !errors.Is(err, refusal) {
		t.Fatal("failed candidate adopted", err)
	}
	value := fixture.result(follow.Stat(testContext(t), Address{Key: "owned/new"}))
	if value.Source().Name != "second" || value.Attribution().Source.Name != "follow" || value.Attribution().Source.Generation != 2 {
		t.Fatal("last-good source lost")
	}
	fixture.result(session.Complete(testContext(t)))
	snapshot, err := root.WaitReleased(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	final, _ := snapshot.ValueCopy()
	if final.Source().Name != "first" || final.Attribution().Source.Generation != 1 || final.Attribution().ID != "frozen-root" {
		t.Fatal("active session relabeled")
	}
	if string(firstPeer.content("owned/old")) != "old" || string(firstPeer.content("owned/fixed")) != "fixed" || string(secondPeer.content("owned/new")) != "new" || len(secondPeer.content("owned/old")) != 0 {
		t.Fatal("source migration")
	}
}

func TestIndependentPublicConsumer(t *testing.T) {
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	source, err := os.ReadFile("testdata/consumer/main.go")
	if err != nil {
		t.Fatal(err)
	}
	syntax, err := parser.ParseFile(token.NewFileSet(), "main.go", source, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, imported := range syntax.Imports {
		path, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(path, "/internal/") || strings.HasPrefix(path, "github.com/minio/") {
			t.Fatal("consumer bypassed public contracts")
		}
	}
	sums, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		"main.go": source, "go.sum": sums,
		"go.mod": []byte(fmt.Sprintf("module example.org/objectstore-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root))),
	} {
		if err := os.WriteFile(filepath.Join(directory, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	run := func(offline bool, args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		command := exec.CommandContext(ctx, "go", args...)
		command.Dir = directory
		command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local")
		if offline {
			command.Env = append(command.Env, "GOPROXY=off", "GOSUMDB=off")
		}
		return command.CombinedOutput()
	}
	if output, err := run(false, "mod", "tidy"); err != nil {
		t.Fatalf("consumer preparation: %v\n%s", err, output)
	}
	if output, err := run(true, "mod", "tidy", "-diff"); err != nil {
		t.Fatalf("consumer graph: %v\n%s", err, output)
	}
	binary := filepath.Join(directory, "consumer")
	if output, err := run(true, "build", "-mod=readonly", "-race", "-o", binary, "."); err != nil {
		t.Fatalf("consumer build: %v\n%s", err, output)
	}
	build, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal("consumer build metadata unavailable")
	}
	foundSDK := false
	for _, dependency := range build.Deps {
		if dependency.Path == "github.com/minio/minio-go/v7" {
			foundSDK = dependency.Version == "v7.3.0" && dependency.Replace == nil
		}
	}
	if !foundSDK {
		t.Fatal("consumer did not select the qualified SDK")
	}
	peer := newPeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary)
	command.Env = append(os.Environ(), "FATHOMRY_CONSUMER_ENDPOINT="+peer.options.Endpoint)
	output, err := command.CombinedOutput()
	if err != nil || string(output) != "objectstore direct and Fixed consumers passed\n" {
		t.Fatalf("consumer execution: %v\n%s", err, output)
	}
	if string(peer.content("owned/external")) != "direct" || string(peer.content("owned/external-fixed")) != "fixed" {
		t.Fatal("consumer native effect oracle")
	}
	notice, _, _ := strings.Cut(string(source), "package main")
	if err := os.WriteFile(filepath.Join(directory, "forbidden.go"), []byte(notice+"package main\nimport _ \"github.com/frost-leo/fathomry/internal/objectstore/minio/v7\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if output, err := run(true, "build", "-mod=readonly", "-o", filepath.Join(directory, "forbidden"), "."); err == nil || !strings.Contains(string(output), "use of internal package") {
		t.Fatal("Internal compiler boundary not enforced")
	}
}

type testObject struct {
	body []byte
	etag string
	tags map[string]string
}

type testUpload struct {
	key   string
	parts map[int][]byte
}

type protocolPeer struct {
	mu      sync.Mutex
	objects map[string]testObject
	uploads map[string]*testUpload
	count   int
	serial  int
	hook    func(http.ResponseWriter, *http.Request) bool
	options Settings
}

func newPeer(t *testing.T) *protocolPeer {
	t.Helper()
	peer := &protocolPeer{objects: map[string]testObject{}, uploads: map[string]*testUpload{}}
	server := httptest.NewServer(http.HandlerFunc(peer.serve))
	t.Cleanup(server.Close)
	peer.options = Settings{Name: "fixture", Endpoint: server.URL, Plaintext: true, Region: "us-east-1", Bucket: "fixture", Prefix: "owned/", AccessKey: "fixture-access", SecretKey: "fixture-secret", Writes: true, Tags: true, Versions: true,
		PresignGET: true, PresignHEAD: true, PresignPUT: true}
	return peer
}

func (peer *protocolPeer) requests() int { peer.mu.Lock(); defer peer.mu.Unlock(); return peer.count }

func (peer *protocolPeer) content(key string) []byte {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return bytes.Clone(peer.objects[key].body)
}

func peerError(writer http.ResponseWriter, status int, code string) {
	writer.WriteHeader(status)
	_, _ = fmt.Fprintf(writer, "<Error><Code>%s</Code><Message>private-service-canary</Message></Error>", code)
}

func (peer *protocolPeer) store(key string, body []byte) testObject {
	sum := sha256.Sum256(body)
	value := testObject{body: bytes.Clone(body), etag: hex.EncodeToString(sum[:]), tags: map[string]string{}}
	peer.objects[key] = value
	return value
}

func (peer *protocolPeer) serve(writer http.ResponseWriter, request *http.Request) {
	peer.mu.Lock()
	peer.count++
	hook := peer.hook
	peer.mu.Unlock()
	if hook != nil && hook(writer, request) {
		return
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	key := strings.TrimPrefix(request.URL.Path, "/fixture/")
	query := request.URL.Query()
	if request.URL.Path == "/fixture/" || request.URL.Path == "/fixture" {
		if request.Method == "HEAD" {
			return
		}
		if query.Has("uploads") {
			_, _ = io.WriteString(writer, "<ListMultipartUploadsResult><Bucket>fixture</Bucket><IsTruncated>false</IsTruncated>")
			for id, upload := range peer.uploads {
				_, _ = fmt.Fprintf(writer, "<Upload><Key>%s</Key><UploadId>%s</UploadId></Upload>", upload.key, id)
			}
			_, _ = io.WriteString(writer, "</ListMultipartUploadsResult>")
			return
		}
		prefix := query.Get("prefix")
		keys := []string{}
		for name := range peer.objects {
			if strings.HasPrefix(name, prefix) {
				keys = append(keys, name)
			}
		}
		sort.Strings(keys)
		_, _ = io.WriteString(writer, "<ListBucketResult><Name>fixture</Name><IsTruncated>false</IsTruncated>")
		for _, name := range keys {
			value := peer.objects[name]
			_, _ = fmt.Fprintf(writer, "<Contents><Key>%s</Key><ETag>%s</ETag><Size>%d</Size></Contents>", name, value.etag, len(value.body))
		}
		_, _ = io.WriteString(writer, "</ListBucketResult>")
		return
	}
	if query.Has("uploads") && request.Method == "POST" {
		peer.serial++
		id := fmt.Sprint("upload-", peer.serial)
		peer.uploads[id] = &testUpload{key: key, parts: map[int][]byte{}}
		_, _ = fmt.Fprintf(writer, "<InitiateMultipartUploadResult><Bucket>fixture</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>", key, id)
		return
	}
	if id := query.Get("uploadId"); id != "" {
		upload := peer.uploads[id]
		if request.Method == "DELETE" {
			delete(peer.uploads, id)
			writer.WriteHeader(204)
			return
		}
		if upload == nil {
			peerError(writer, 404, "NoSuchUpload")
			return
		}
		switch request.Method {
		case "PUT":
			number, _ := strconv.Atoi(query.Get("partNumber"))
			body, _ := io.ReadAll(request.Body)
			upload.parts[number] = body
			writer.Header().Set("ETag", "part-etag")
			return
		case "GET":
			_, _ = fmt.Fprintf(writer, "<ListPartsResult><Bucket>fixture</Bucket><Key>%s</Key><UploadId>%s</UploadId><IsTruncated>false</IsTruncated>", key, id)
			for number, body := range upload.parts {
				_, _ = fmt.Fprintf(writer, "<Part><PartNumber>%d</PartNumber><ETag>part-etag</ETag><Size>%d</Size></Part>", number, len(body))
			}
			_, _ = io.WriteString(writer, "</ListPartsResult>")
			return
		case "POST":
			if _, exists := peer.objects[key]; exists && request.Header.Get("If-None-Match") == "*" {
				peerError(writer, 412, "PreconditionFailed")
				return
			}
			var body []byte
			for index := 1; index <= len(upload.parts); index++ {
				body = append(body, upload.parts[index]...)
			}
			value := peer.store(key, body)
			delete(peer.uploads, id)
			_, _ = fmt.Fprintf(writer, "<CompleteMultipartUploadResult><Bucket>fixture</Bucket><Key>%s</Key><ETag>%s</ETag></CompleteMultipartUploadResult>", key, value.etag)
			return
		}
	}
	value, exists := peer.objects[key]
	if query.Has("tagging") {
		if !exists {
			peerError(writer, 404, "NoSuchKey")
			return
		}
		switch request.Method {
		case "PUT":
			var tags struct {
				Tags []struct{ Key, Value string } `xml:"TagSet>Tag"`
			}
			_ = xml.NewDecoder(request.Body).Decode(&tags)
			value.tags = map[string]string{}
			for _, tag := range tags.Tags {
				value.tags[tag.Key] = tag.Value
			}
			peer.objects[key] = value
		case "DELETE":
			value.tags = map[string]string{}
			peer.objects[key] = value
			writer.WriteHeader(http.StatusNoContent)
		default:
			_, _ = io.WriteString(writer, "<Tagging><TagSet>")
			for key, value := range value.tags {
				_, _ = fmt.Fprintf(writer, "<Tag><Key>%s</Key><Value>%s</Value></Tag>", key, value)
			}
			_, _ = io.WriteString(writer, "</TagSet></Tagging>")
		}
		return
	}
	switch request.Method {
	case "PUT":
		if exists && request.Header.Get("If-None-Match") == "*" {
			peerError(writer, 412, "PreconditionFailed")
			return
		}
		if source := request.Header.Get("X-Amz-Copy-Source"); source != "" {
			original, ok := peer.objects[strings.TrimPrefix(strings.TrimPrefix(source, "/"), "fixture/")]
			if !ok {
				peerError(writer, 404, "NoSuchKey")
				return
			}
			copied := peer.store(key, original.body)
			_, _ = fmt.Fprintf(writer, "<CopyObjectResult><ETag>%s</ETag></CopyObjectResult>", copied.etag)
			return
		}
		body, _ := io.ReadAll(request.Body)
		value := peer.store(key, body)
		writer.Header().Set("ETag", value.etag)
	case "GET", "HEAD":
		if !exists {
			peerError(writer, 404, "NoSuchKey")
			return
		}
		writer.Header().Set("ETag", value.etag)
		writer.Header().Set("Content-Length", strconv.Itoa(len(value.body)))
		writer.Header().Set("Last-Modified", time.Unix(1700000000, 0).UTC().Format(http.TimeFormat))
		if request.Method == "GET" {
			_, _ = writer.Write(value.body)
		}
	case "DELETE":
		delete(peer.objects, key)
		writer.WriteHeader(204)
	default:
		peerError(writer, 400, "InvalidRequest")
	}
}

func testContext(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

type fixture struct {
	t       *testing.T
	owner   *Owner
	client  *Client
	inbox   *adapters.Inbox[Result]
	runtime *adapters.Runtime
}

func newFixture(t *testing.T, options Settings) *fixture {
	t.Helper()
	policy, err := Recommend(options)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := Open(context.Background(), options, Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		if owner != nil {
			_ = owner.Close(testContext(t))
		}
		t.Fatal(err)
	}
	result := &fixture{t, owner, owner.Client(), inbox, runtime}
	t.Cleanup(func() {
		if err := owner.Close(testContext(t)); err != nil {
			t.Error("close", err)
		}
		if err := runtime.Close(testContext(t)); err != nil {
			t.Error("runtime", err)
		}
		drain(t, inbox)
	})
	return result
}

func drain(t *testing.T, inbox *adapters.Inbox[Result]) {
	t.Helper()
	for state, _ := inbox.Inspect(); state.Outstanding > 0; state, _ = inbox.Inspect() {
		delivery, err := inbox.NextReleased(testContext(t))
		if err != nil {
			t.Error(err)
			return
		}
		if err := delivery.Ack(); err != nil {
			t.Error(err)
			return
		}
	}
}

func (test *fixture) result(receipt *adapters.Receipt[Result], err error) Result {
	test.t.Helper()
	if err != nil {
		test.t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(testContext(test.t))
	if err != nil {
		test.t.Fatal(err)
	}
	if snapshot.Err() != nil {
		logTechnicalKinds(test.t, snapshot.Err(), 32)
		test.t.Fatal("primary", snapshot.Primary(), "cleanup", snapshot.Cleanup())
	}
	value, present := snapshot.ValueCopy()
	if !present {
		test.t.Fatal("missing public facts")
	}
	delivery, err := test.inbox.NextReleased(testContext(test.t))
	if err != nil {
		test.t.Fatal(err)
	}
	if err := delivery.Ack(); err != nil {
		test.t.Fatal(err)
	}
	return value
}

func logTechnicalKinds(t *testing.T, err error, remaining int) {
	if err == nil || remaining == 0 {
		return
	}
	if value, ok := err.(*fault.Error); ok {
		diagnostic := value.Diagnostic()
		t.Log("technical category", diagnostic.Kind, "operation", diagnostic.Context.Operation)
	}
	switch value := err.(type) {
	case interface{ Unwrap() []error }:
		for _, child := range value.Unwrap() {
			logTechnicalKinds(t, child, remaining-1)
		}
	case interface{ Unwrap() error }:
		logTechnicalKinds(t, value.Unwrap(), remaining-1)
	}
}
