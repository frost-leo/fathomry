//go:build minio_service

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
	"crypto/rand"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/objectstore/v1"
	sdk "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.yaml.in/yaml/v3"
)

// The private inventory is read only here; credentials never enter diagnostics,
// subprocess arguments, generated fixtures, source files or recorded artifacts.
func serviceSettings(t *testing.T) Settings {
	t.Helper()
	path := os.Getenv("FATHOMRY_MINIO_TEST_CONFIG")
	if path == "" {
		t.Fatal("explicit private service fixture required by minio_service")
	}
	if os.Getenv("FATHOMRY_MINIO_TEST_WRITES") != "1" || os.Getenv("FATHOMRY_MINIO_TEST_BUCKET_ADMIN") != "1" {
		t.Fatal("explicit isolated versioned bucket create/write/cleanup authorization required")
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("private fixture unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 64<<10 {
		t.Fatal("private fixture permissions or size invalid")
	}
	var config struct {
		MinIO struct {
			Endpoint string `yaml:"api_endpoint"`
			Root     struct {
				Username string `yaml:"username"`
				Password string `yaml:"password"`
			} `yaml:"root"`
			Fathomry struct {
				Region string `yaml:"region"`
			} `yaml:"fathomry"`
		} `yaml:"minio"`
	}
	decoder := yaml.NewDecoder(io.LimitReader(file, 64<<10+1))
	if decoder.Decode(&config) != nil || decoder.Decode(new(any)) != io.EOF {
		t.Fatal("private fixture decoding failed")
	}
	token := make([]byte, 12)
	if _, err := rand.Read(token); err != nil {
		t.Fatal("fixture identity unavailable")
	}
	value := Settings{Name: "service", Endpoint: config.MinIO.Endpoint, Plaintext: strings.HasPrefix(config.MinIO.Endpoint, "http://"),
		Region: config.MinIO.Fathomry.Region, Bucket: "fathomry-gh104-" + hex.EncodeToString(token), Prefix: "owned/",
		AccessKey: config.MinIO.Root.Username, SecretKey: config.MinIO.Root.Password, Writes: true, Versions: true, Tags: true,
		PresignGET: true, PresignHEAD: true, PresignPUT: true, MaxEntries: 1, Timeout: 30 * time.Second}
	if err := Validate(value); err != nil {
		t.Fatal("fixture outside qualified HTTP literal-IP profile")
	}
	t.Logf("test-owned fixture nonce: %s", hex.EncodeToString(token))
	return value
}

func TestMinIOServiceCoreExtensions(t *testing.T) {
	value := serviceSettings(t)
	endpoint, err := url.Parse(value.Endpoint)
	if err != nil {
		t.Fatal("invalid fixture endpoint")
	}
	transport := &http.Transport{Proxy: nil, ResponseHeaderTimeout: 10 * time.Second, MaxResponseHeaderBytes: 32 << 10, DisableCompression: true}
	t.Cleanup(transport.CloseIdleConnections)
	independent, err := sdk.NewCore(endpoint.Host, &sdk.Options{Creds: credentials.NewStaticV4(value.AccessKey, value.SecretKey, ""), Secure: !value.Plaintext,
		Region: value.Region, BucketLookup: sdk.BucketLookupPath, Transport: transport, MaxRetries: 1})
	if err != nil {
		t.Fatal("independent service client unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	exists, err := independent.BucketExists(ctx, value.Bucket)
	if err != nil || exists {
		t.Fatal("unique fixture absence unconfirmed")
	}
	keys := map[string]bool{}
	// Ownership is established by observed absence and a unique attempted creation,
	// including a lost creation reply. Cleanup is restricted to this exact bucket.
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		page, err := independent.ListMultipartUploads(cleanup, value.Bucket, value.Prefix, "", "", "", 1000)
		if err != nil || page.IsTruncated {
			t.Error("owned multipart cleanup enumeration failed")
			return
		}
		for _, upload := range page.Uploads {
			if !keys[upload.Key] {
				t.Error("unexpected fixture upload; not deleting")
				return
			}
			if err := independent.AbortMultipartUpload(cleanup, value.Bucket, upload.Key, upload.UploadID); err != nil {
				t.Error("owned multipart cleanup failed")
				return
			}
		}
		count := 0
		for object := range independent.ListObjectsIter(cleanup, value.Bucket, sdk.ListObjectsOptions{Prefix: value.Prefix, Recursive: true, WithVersions: true, MaxKeys: 1000}) {
			if object.Err != nil || !keys[object.Key] || object.VersionID == "" || count >= 64 {
				t.Error("exact version cleanup not established")
				return
			}
			count++
			if err := independent.RemoveObject(cleanup, value.Bucket, object.Key, sdk.RemoveObjectOptions{VersionID: object.VersionID}); err != nil {
				t.Error("owned version cleanup failed")
				return
			}
		}
		if err := independent.RemoveBucket(cleanup, value.Bucket); err != nil {
			t.Error("owned empty bucket cleanup failed")
			return
		}
		if exists, err := independent.BucketExists(cleanup, value.Bucket); err != nil || exists {
			t.Error("owned bucket absence not confirmed")
			return
		}
		t.Log("isolated versioned fixture: uploads, exact versions/delete markers and empty bucket removed; absence confirmed")
	})
	if err := independent.MakeBucket(ctx, value.Bucket, sdk.MakeBucketOptions{Region: value.Region}); err != nil {
		t.Fatal("isolated bucket creation failed")
	}
	if err := independent.SetBucketVersioning(ctx, value.Bucket, sdk.BucketVersioningConfiguration{Status: "Enabled"}); err != nil {
		t.Fatal("isolated bucket versioning failed")
	}
	versioning, err := independent.GetBucketVersioning(ctx, value.Bucket)
	if err != nil || versioning.Status != "Enabled" {
		t.Fatal("versioning read-back failed")
	}
	t.Log("fresh isolated service fixture created; only this test-owned bucket is version-enabled")
	test := newFixture(t, value)
	client := test.client
	readBack := func(key string, want []byte) {
		t.Helper()
		reader, _, _, err := independent.GetObject(ctx, value.Bucket, key, sdk.GetObjectOptions{})
		if err != nil {
			t.Fatal("independent object read failed")
		}
		actual, readErr := io.ReadAll(io.LimitReader(reader, int64(len(want))+1))
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(actual, want) {
			t.Fatal("independent object read-back mismatch")
		}
	}
	key := value.Prefix + "data"
	keys[key] = true
	test.result(client.Put(ctx, ctx, WriteRequest{Key: key, Size: 7, IfAbsent: true}, strings.NewReader("payload")))
	readBack(key, []byte("payload"))
	info := test.result(client.Stat(ctx, Address{Key: key}))
	object, present := info.Object()
	if !present || object.Address.VersionID == "" {
		t.Fatal("versioned stat missing")
	}
	test.result(client.SetTags(ctx, Address{Key: key}, map[string]string{"purpose": "gh104"}))
	if tags := test.result(client.GetTags(ctx, Address{Key: key})).TagsCopy(); tags["purpose"] != "gh104" {
		t.Fatal("service tags")
	}
	test.result(client.SetTags(ctx, Address{Key: key}, nil))
	copyKey := value.Prefix + "copy"
	keys[copyKey] = true
	test.result(client.Copy(ctx, CopyRequest{Source: Address{Key: key, VersionID: object.Address.VersionID}, Key: copyKey, MatchETag: object.ETag}))
	readBack(copyKey, []byte("payload"))
	var output bytes.Buffer
	test.result(client.Download(ctx, ReadRequest{Address: Address{Key: key}, Offset: 1, Length: 3}, &output))
	if output.String() != "ayl" {
		t.Fatal("range download mismatch")
	}
	multipartKey := value.Prefix + "multipart"
	keys[multipartKey] = true
	body := bytes.Repeat([]byte("x"), 5<<20)
	session, root, err := client.BeginMultipart(ctx, ctx, ctx, WriteRequest{Key: multipartKey, Size: int64(len(body)) + 3, IfAbsent: true})
	if err != nil || session == nil {
		t.Fatal("service multipart setup")
	}
	test.result(session.Part(ctx, 1, int64(len(body)), bytes.NewReader(body)))
	test.result(session.Part(ctx, 2, 3, strings.NewReader("end")))
	// The inspection marker is reused without deriving an upload ID.
	uploads := test.result(client.ListUploads(ctx, UploadQuery{Prefix: multipartKey})).UploadsCopy()
	if len(uploads) != 1 {
		t.Fatal("service upload inspection")
	}
	firstParts := test.result(client.ListParts(ctx, uploads[0], 0))
	if len(firstParts.PartsCopy()) != 1 || firstParts.NextPart() != 1 {
		t.Fatal("service part page")
	}
	secondParts := test.result(client.ListParts(ctx, uploads[0], firstParts.NextPart()))
	if len(secondParts.PartsCopy()) != 1 || !secondParts.Complete() {
		t.Fatal("service part continuation")
	}
	final := test.result(session.Complete(ctx))
	if final.Transfer().Effect != objectstore.Acknowledged || !final.Transfer().CompletionAttempted {
		t.Fatal("service completion facts")
	}
	if _, err := root.WaitReleased(ctx); err != nil {
		t.Fatal("service root release")
	}
	readBack(multipartKey, append(bytes.Clone(body), []byte("end")...))
	unknownKey := value.Prefix + "unknown"
	keys[unknownKey] = true
	test.result(client.Put(ctx, ctx, WriteRequest{Key: unknownKey, Size: -1}, bytes.NewReader(append(bytes.Clone(body), byte('y')))))
	readBack(unknownKey, append(bytes.Clone(body), byte('y')))
	abortKey := value.Prefix + "abort"
	keys[abortKey] = true
	session, _, err = client.BeginMultipart(ctx, ctx, ctx, WriteRequest{Key: abortKey, Size: -1})
	if err != nil || session == nil {
		t.Fatal("service abort setup")
	}
	test.result(session.Part(ctx, 1, 1, strings.NewReader("x")))
	if result := test.result(session.Abort(ctx)); !result.Transfer().AbortAcknowledged {
		t.Fatal("service abort unconfirmed")
	}
	if uploads := test.result(client.ListUploads(ctx, UploadQuery{Prefix: abortKey})).UploadsCopy(); len(uploads) != 0 {
		t.Fatal("service abort left upload")
	}
	versionKey := value.Prefix + "versions"
	keys[versionKey] = true
	var selectedVersion string
	for _, payload := range []string{"first", "second", "third"} {
		result := test.result(client.Put(ctx, ctx, WriteRequest{Key: versionKey, Size: int64(len(payload))}, strings.NewReader(payload)))
		if payload == "first" {
			object, _ := result.Object()
			selectedVersion = object.Address.VersionID
		}
	}
	test.result(client.Remove(ctx, []Address{{Key: versionKey}}))
	cursor, _, err := client.Enumerate(ctx, ctx, ListRequest{Prefix: versionKey, Versions: true})
	if err != nil || cursor == nil {
		t.Fatal("service version enumeration setup")
	}
	seen := map[string]bool{}
	markers := 0
	for page := 0; page < 8; page++ {
		result := test.result(cursor.Next(ctx))
		for _, object := range result.ObjectsCopy() {
			if object.Address.Key != versionKey || object.Address.VersionID == "" || seen[object.Address.VersionID] {
				t.Fatal("same-key version duplicate/loss")
			}
			seen[object.Address.VersionID] = true
			if object.DeleteMarker {
				markers++
			}
		}
		if result.Complete() {
			break
		}
	}
	if len(seen) != 4 || markers != 1 || !seen[selectedVersion] {
		t.Fatal("same-key versions or delete marker omitted")
	}
	if final, err := cursor.Receipt().WaitReleased(ctx); err != nil || final.Err() != nil {
		t.Fatal("version iterator not joined")
	}
	signed := func(request SignRequest) Delegation {
		t.Helper()
		result := test.result(client.Presign(ctx, request))
		delegation, ok := result.Delegation()
		if !ok || result.Transfer().Effect != objectstore.NotSubmitted {
			t.Fatal("issuance facts")
		}
		return delegation
	}
	httpClient := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	use := func(delegation Delegation, method, urlText string, headers http.Header, payload string) (int, []byte) {
		t.Helper()
		if method == "" {
			method = delegation.Method()
		}
		if urlText == "" {
			urlText = delegation.URL()
		}
		if headers == nil {
			headers = delegation.HeadersCopy()
		}
		request, err := http.NewRequestWithContext(ctx, method, urlText, strings.NewReader(payload))
		if err != nil {
			t.Fatal("delegated request construction")
		}
		request.Header = headers
		response, err := httpClient.Do(request)
		if err != nil {
			t.Fatal("delegated request execution")
		}
		content, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		closeErr := response.Body.Close()
		if err != nil || closeErr != nil {
			t.Fatal("delegated response read")
		}
		return response.StatusCode, content
	}
	get := signed(SignRequest{Method: GET, Address: Address{Key: versionKey, VersionID: selectedVersion}, Expiry: time.Minute})
	if status, content := use(get, "", "", nil, ""); status != 200 || string(content) != "first" {
		t.Fatal("exact-version delegated GET")
	}
	head := signed(SignRequest{Method: HEAD, Address: Address{Key: key}, MatchETag: object.ETag, Expiry: time.Minute})
	if status, _ := use(head, "", "", nil, ""); status != 200 {
		t.Fatal("conditional delegated HEAD")
	}
	signedKey := value.Prefix + "delegated"
	keys[signedKey] = true
	put := signed(SignRequest{Method: PUT, Address: Address{Key: signedKey}, IfAbsent: true, Expiry: time.Minute})
	if status, _ := use(put, "", "", nil, "delegated"); status != 200 {
		t.Fatal("conditional delegated PUT")
	}
	readBack(signedKey, []byte("delegated"))
	if status, _ := use(put, "", "", nil, "again"); status != 412 {
		t.Fatal("delegated condition not enforced")
	}
	// Empty invalid PUTs avoid racing an intentionally early rejection with the
	// external HTTP client's body writer. Read-back still rejects any overwrite.
	if status, _ := use(put, "", "", make(http.Header), ""); status != 400 && status != 403 {
		t.Fatalf("unsigned condition rejection status: %d", status)
	}
	readBack(signedKey, []byte("delegated"))
	changedHeaders := put.HeadersCopy()
	changedHeaders.Set("If-None-Match", "changed")
	if status, _ := use(put, "", "", changedHeaders, ""); status != 403 {
		t.Fatal("altered signed condition accepted")
	}
	readBack(signedKey, []byte("delegated"))
	if status, _ := use(put, "GET", "", nil, ""); status != 403 {
		t.Fatal("delegated method tampering accepted")
	}
	changed, _ := url.Parse(get.URL())
	changed.Path = "/" + value.Bucket + "/" + copyKey
	if status, _ := use(get, "", changed.String(), nil, ""); status != 403 {
		t.Fatal("delegated path tampering accepted")
	}
	expires := signed(SignRequest{Method: GET, Address: Address{Key: key}, Expiry: time.Second})
	time.Sleep(2 * time.Second)
	if status, _ := use(expires, "", "", nil, ""); status != 403 {
		t.Fatal("expired capability accepted")
	}
	if err := test.owner.Close(ctx); err != nil {
		t.Fatal("issuing owner cleanup")
	}
	if status, content := use(get, "", "", nil, ""); status != 200 || string(content) != "first" {
		t.Fatal("local closure incorrectly treated as capability revocation")
	}
	t.Log("fresh public service writes, independent read-back, serial multipart/abort, same-key versions and consumed/denied delegation passed")
}
