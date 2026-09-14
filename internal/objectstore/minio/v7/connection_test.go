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
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func deadline(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func correlation(name string) fault.Correlation {
	return fault.Correlation{Call: strings.ReplaceAll(name, "/", "-")}
}

type fixture struct {
	client   *Client
	selected resource.Selection[Source]
	assembly *resource.Assembly
	inbox    *invocation.Inbox[Result]
}

func bindFixture(t testing.TB, options OptionsV1, capacity int) fixture {
	t.Helper()
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(deadline(t), deadline(t), "test", selected)
	if err != nil {
		if assembly != nil {
			_ = assembly.Close(deadline(t))
		}
		t.Fatal(err)
	}
	inbox, err := invocation.NewInbox[Result](capacity, int64(capacity)*defaults(options).evidenceReservation())
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for inbox.Usage().Outstanding > 0 {
			delivery, err := inbox.Next(deadline(t))
			if err != nil {
				t.Error(err)
				break
			}
			if _, err := delivery.Receipt().WaitReleased(deadline(t)); err != nil {
				t.Error(err)
				break
			}
			if err := delivery.Release(); err != nil {
				t.Error(err)
				break
			}
		}
		if err := assembly.Close(deadline(t)); err != nil {
			t.Error(err)
		}
	})
	return fixture{client, selected, assembly, inbox}
}
func settle(t testing.TB, receipt *invocation.Receipt[Result], err error) invocation.Result[Result] {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	result, err := receipt.WaitReleased(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func put(t testing.TB, client *Client, key string, body []byte) Result {
	t.Helper()
	receipt, err := client.Put(deadline(t), deadline(t), correlation("put-"+key), WriteRequest{Key: key, Size: int64(len(body))}, bytes.NewReader(body))
	result := settle(t, receipt, err)
	if result.Err() != nil {
		t.Fatal(result.Err())
	}
	return result.Outcome.Value
}

type storedObject struct {
	body     []byte
	etag     string
	version  string
	metadata map[string]string
	tags     map[string]string
}
type pendingUpload struct {
	key      string
	metadata map[string]string
	parts    map[int][]byte
}
type observedRequest struct {
	method string
	path   string
	query  url.Values
	header http.Header
}
type peer struct {
	mu          sync.Mutex
	objects     map[string]storedObject
	versions    map[string]storedObject
	uploads     map[string]*pendingUpload
	requests    []observedRequest
	serial      int
	unversioned bool
	hook        func(http.ResponseWriter, *http.Request) bool
}

func newPeer(t testing.TB) (*peer, OptionsV1) {
	t.Helper()
	server := &peer{objects: map[string]storedObject{}, versions: map[string]storedObject{}, uploads: map[string]*pendingUpload{}}
	httpServer := httptest.NewServer(http.HandlerFunc(server.serve))
	t.Cleanup(httpServer.Close)
	return server, OptionsV1{Name: "objects", Endpoint: httpServer.URL, Plaintext: true, Region: "us-east-1", Bucket: "fixture", Prefix: "owned/",
		AccessKey: "fixture-access", SecretKey: "fixture-secret", Writes: true, Versions: true, Tags: true}
}
func (server *peer) count() int {
	server.mu.Lock()
	defer server.mu.Unlock()
	return len(server.requests)
}
func (server *peer) content(key string) []byte {
	server.mu.Lock()
	defer server.mu.Unlock()
	return bytes.Clone(server.objects[key].body)
}
func (server *peer) store(key string, body []byte, metadata map[string]string) storedObject {
	server.serial++
	hash := sha256.Sum256(body)
	value := storedObject{body: bytes.Clone(body), etag: hex.EncodeToString(hash[:]), version: fmt.Sprintf("v%d", server.serial), metadata: maps.Clone(metadata)}
	if server.unversioned {
		value.version = ""
	}
	server.objects[key] = value
	server.versions[key+"?"+value.version] = value
	return value
}
func errorResponse(writer http.ResponseWriter, status int, code string) {
	writer.Header().Set("Content-Type", "application/xml")
	writer.WriteHeader(status)
	fmt.Fprintf(writer, "<Error><Code>%s</Code><Message>private-native-canary</Message><RequestId>fixture</RequestId></Error>", code)
}
func objectHeaders(writer http.ResponseWriter, value storedObject) {
	writer.Header().Set("ETag", strconv.Quote(value.etag))
	writer.Header().Set("Last-Modified", time.Unix(1700000000, 0).UTC().Format(http.TimeFormat))
	writer.Header().Set("Content-Type", "application/octet-stream")
	writer.Header().Set("Content-Length", strconv.Itoa(len(value.body)))
	writer.Header().Set("X-Amz-Version-Id", value.version)
	for key, value := range value.metadata {
		writer.Header().Set(key, value)
	}
}
func requestMetadata(request *http.Request) map[string]string {
	result := map[string]string{}
	for key, values := range request.Header {
		if strings.HasPrefix(strings.ToLower(key), "x-amz-meta-") {
			result[key] = values[0]
		}
	}
	return result
}
func xmlText(value string) string {
	var buffer bytes.Buffer
	_ = xml.EscapeText(&buffer, []byte(value))
	return buffer.String()
}
func (server *peer) serve(writer http.ResponseWriter, request *http.Request) {
	server.mu.Lock()
	server.requests = append(server.requests, observedRequest{request.Method, request.URL.Path, request.URL.Query(), request.Header.Clone()})
	hook := server.hook
	server.mu.Unlock()
	if hook != nil && hook(writer, request) {
		return
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	key := strings.TrimPrefix(request.URL.Path, "/fixture/")
	query := request.URL.Query()
	if request.Method == "HEAD" && request.URL.Path == "/fixture/" || request.Method == "HEAD" && request.URL.Path == "/fixture" {
		writer.WriteHeader(200)
		return
	}
	if _, ok := query["uploads"]; ok {
		if request.Method == "POST" {
			server.serial++
			id := fmt.Sprintf("upload-%d", server.serial)
			server.uploads[id] = &pendingUpload{key: key, metadata: requestMetadata(request), parts: map[int][]byte{}}
			fmt.Fprintf(writer, "<InitiateMultipartUploadResult><Bucket>fixture</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>", xmlText(key), id)
		} else {
			fmt.Fprint(writer, "<ListMultipartUploadsResult><Bucket>fixture</Bucket><IsTruncated>false</IsTruncated>")
			for id, upload := range server.uploads {
				if strings.HasPrefix(upload.key, query.Get("prefix")) {
					fmt.Fprintf(writer, "<Upload><Key>%s</Key><UploadId>%s</UploadId><Initiated>2026-09-14T00:00:00Z</Initiated></Upload>", xmlText(upload.key), id)
				}
			}
			fmt.Fprint(writer, "</ListMultipartUploadsResult>")
		}
		return
	}
	if id := query.Get("uploadId"); id != "" {
		upload, found := server.uploads[id]
		if !found {
			errorResponse(writer, 404, "NoSuchUpload")
			return
		}
		switch request.Method {
		case "PUT":
			body, _ := io.ReadAll(io.LimitReader(request.Body, 17<<20))
			md5sum, sha := hashes(body)
			if request.Header.Get("Content-Md5") != md5sum || request.Header.Get("X-Amz-Content-Sha256") != sha {
				errorResponse(writer, 400, "BadDigest")
				return
			}
			part, _ := strconv.Atoi(query.Get("partNumber"))
			upload.parts[part] = bytes.Clone(body)
			writer.Header().Set("ETag", strconv.Quote(sha))
		case "GET":
			fmt.Fprintf(writer, "<ListPartsResult><Bucket>fixture</Bucket><Key>%s</Key><UploadId>%s</UploadId><IsTruncated>false</IsTruncated>", xmlText(key), id)
			for part := 1; part <= len(upload.parts); part++ {
				fmt.Fprintf(writer, "<Part><PartNumber>%d</PartNumber><Size>%d</Size><ETag>part</ETag></Part>", part, len(upload.parts[part]))
			}
			fmt.Fprint(writer, "</ListPartsResult>")
		case "DELETE":
			delete(server.uploads, id)
			writer.WriteHeader(204)
		case "POST":
			if !server.condition(writer, request, key) {
				return
			}
			var input struct {
				Parts []struct {
					Number int `xml:"PartNumber"`
				} `xml:"Part"`
			}
			if xml.NewDecoder(request.Body).Decode(&input) != nil {
				errorResponse(writer, 400, "MalformedXML")
				return
			}
			var body []byte
			for _, part := range input.Parts {
				body = append(body, upload.parts[part.Number]...)
			}
			object := server.store(key, body, upload.metadata)
			delete(server.uploads, id)
			writer.Header().Set("X-Amz-Version-Id", object.version)
			fmt.Fprintf(writer, "<CompleteMultipartUploadResult><Bucket>fixture</Bucket><Key>%s</Key><ETag>%s</ETag></CompleteMultipartUploadResult>", xmlText(key), object.etag)
		}
		return
	}
	if query.Get("list-type") == "2" {
		var keys []string
		for key := range server.objects {
			if strings.HasPrefix(key, query.Get("prefix")) && key > query.Get("start-after") {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		start, _ := strconv.Atoi(query.Get("continuation-token"))
		maximum, _ := strconv.Atoi(query.Get("max-keys"))
		if maximum <= 0 {
			maximum = 1000
		}
		end := min(len(keys), start+maximum)
		fmt.Fprintf(writer, "<ListBucketResult><IsTruncated>%t</IsTruncated>", end < len(keys))
		if end < len(keys) {
			fmt.Fprintf(writer, "<NextContinuationToken>%d</NextContinuationToken>", end)
		}
		for _, key := range keys[start:end] {
			value := server.objects[key]
			fmt.Fprintf(writer, "<Contents><Key>%s</Key><Size>%d</Size><ETag>%s</ETag><LastModified>2026-09-14T00:00:00Z</LastModified></Contents>", xmlText(key), len(value.body), value.etag)
		}
		fmt.Fprint(writer, "</ListBucketResult>")
		return
	}
	value, found := server.objects[key]
	if version := query.Get("versionId"); version != "" {
		value, found = server.versions[key+"?"+version]
	}
	if _, ok := query["tagging"]; ok {
		if !found {
			errorResponse(writer, 404, "NoSuchKey")
			return
		}
		switch request.Method {
		case "GET":
			fmt.Fprint(writer, "<Tagging><TagSet>")
			for key, value := range value.tags {
				fmt.Fprintf(writer, "<Tag><Key>%s</Key><Value>%s</Value></Tag>", xmlText(key), xmlText(value))
			}
			fmt.Fprint(writer, "</TagSet></Tagging>")
		case "PUT":
			var body struct {
				Tags []struct {
					Key   string
					Value string
				} `xml:"TagSet>Tag"`
			}
			_ = xml.NewDecoder(request.Body).Decode(&body)
			value.tags = map[string]string{}
			for _, tag := range body.Tags {
				value.tags[tag.Key] = tag.Value
			}
			server.objects[key] = value
		case "DELETE":
			value.tags = nil
			server.objects[key] = value
			writer.WriteHeader(204)
		}
		return
	}
	switch request.Method {
	case "PUT":
		if source := request.Header.Get("X-Amz-Copy-Source"); source != "" {
			source, _ = url.PathUnescape(source)
			source = strings.TrimPrefix(source, "/")
			source = strings.TrimPrefix(source, "fixture/")
			copied, found := server.objects[source]
			if strings.Contains(source, "?versionId=") {
				copied, found = server.versions[strings.Replace(source, "?versionId=", "?", 1)]
			}
			if !found {
				errorResponse(writer, 404, "NoSuchKey")
				return
			}
			if match := request.Header.Get("X-Amz-Copy-Source-If-Match"); match != "" && strings.Trim(match, "\"") != copied.etag {
				errorResponse(writer, 412, "PreconditionFailed")
				return
			}
			value = server.store(key, copied.body, copied.metadata)
			writer.Header().Set("X-Amz-Version-Id", value.version)
			fmt.Fprintf(writer, "<CopyObjectResult><ETag>%s</ETag><LastModified>2026-09-14T00:00:00Z</LastModified></CopyObjectResult>", value.etag)
			return
		}
		if !server.condition(writer, request, key) {
			return
		}
		body, _ := io.ReadAll(io.LimitReader(request.Body, 17<<20))
		md5sum, sha := hashes(body)
		if request.Header.Get("Content-Md5") != md5sum || request.Header.Get("X-Amz-Content-Sha256") != sha ||
			!strings.Contains(request.Header.Get("Authorization"), "content-type") {
			errorResponse(writer, 400, "BadDigest")
			return
		}
		value = server.store(key, body, requestMetadata(request))
		writer.Header().Set("ETag", strconv.Quote(value.etag))
		writer.Header().Set("X-Amz-Version-Id", value.version)
	case "GET", "HEAD":
		if !found {
			errorResponse(writer, 404, "NoSuchKey")
			return
		}
		if match := request.Header.Get("If-Match"); match != "" && strings.Trim(match, "\"") != value.etag {
			errorResponse(writer, 412, "PreconditionFailed")
			return
		}
		objectHeaders(writer, value)
		if request.Method == "HEAD" {
			return
		}
		body := value.body
		if requested := request.Header.Get("Range"); requested != "" {
			var first, last int
			if _, err := fmt.Sscanf(requested, "bytes=%d-%d", &first, &last); err != nil || first < 0 || last >= len(body) {
				writer.Header().Del("Content-Length")
				errorResponse(writer, 416, "InvalidRange")
				return
			}
			writer.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", first, last, len(body)))
			body = body[first : last+1]
			writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
			writer.WriteHeader(206)
		}
		_, _ = writer.Write(body)
	case "DELETE":
		if version := query.Get("versionId"); version != "" {
			delete(server.versions, key+"?"+version)
		} else {
			delete(server.objects, key)
		}
		writer.WriteHeader(204)
	default:
		errorResponse(writer, 400, "NotImplemented")
	}
}
func (server *peer) condition(writer http.ResponseWriter, request *http.Request, key string) bool {
	value, found := server.objects[key]
	if request.Header.Get("If-None-Match") == "*" && found {
		errorResponse(writer, 412, "PreconditionFailed")
		return false
	}
	if match := request.Header.Get("If-Match"); match != "" && (!found || strings.Trim(match, "\"") != value.etag) {
		errorResponse(writer, 412, "PreconditionFailed")
		return false
	}
	return true
}
