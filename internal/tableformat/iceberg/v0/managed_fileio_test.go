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
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type managedIOPeer struct {
	mu       sync.Mutex
	objects  map[string][]byte
	parts    map[int][]byte
	requests []string
	hook     func(*http.Request) (*http.Response, error, bool)
}

func managedResponse(request *http.Request, status int, data []byte, headers http.Header) *http.Response {
	if headers == nil {
		headers = make(http.Header)
	}
	headers.Set("Content-Length", strconv.Itoa(len(data)))
	return &http.Response{Request: request, StatusCode: status, Header: headers, ContentLength: int64(len(data)), Body: io.NopCloser(bytes.NewReader(data))}
}

func (peer *managedIOPeer) RoundTrip(request *http.Request) (*http.Response, error) {
	peer.mu.Lock()
	peer.requests = append(peer.requests, request.Method+" "+request.URL.RequestURI())
	hook := peer.hook
	peer.mu.Unlock()
	if hook != nil {
		if response, err, handled := hook(request); handled {
			return response, err
		}
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	key := strings.TrimPrefix(request.URL.Path, "/bucket/")
	query := request.URL.Query()
	errorResponse := func(status int, code string) (*http.Response, error) {
		return managedResponse(request, status, []byte("<Error><Code>"+code+"</Code></Error>"), nil), nil
	}
	if request.Method == "POST" && query.Has("uploads") {
		peer.parts = map[int][]byte{}
		return managedResponse(request, 200, []byte("<InitiateMultipartUploadResult><UploadId>owned-upload</UploadId></InitiateMultipartUploadResult>"), nil), nil
	}
	if query.Get("uploadId") != "" {
		switch request.Method {
		case "PUT":
			part, _ := strconv.Atoi(query.Get("partNumber"))
			data, err := io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}
			peer.parts[part] = data
			return managedResponse(request, 200, nil, http.Header{"Etag": []string{fmt.Sprintf(`"part-%d"`, part)}}), nil
		case "POST":
			if request.Header.Get("If-None-Match") != "*" {
				return errorResponse(400, "InvalidRequest")
			}
			if _, exists := peer.objects[key]; exists {
				return errorResponse(412, "PreconditionFailed")
			}
			var data []byte
			for part := 1; part <= len(peer.parts); part++ {
				data = append(data, peer.parts[part]...)
			}
			peer.objects[key] = data
			return managedResponse(request, 200, []byte(`<CompleteMultipartUploadResult><ETag>"complete"</ETag></CompleteMultipartUploadResult>`), nil), nil
		case "DELETE":
			peer.parts = nil
			return managedResponse(request, 204, nil, nil), nil
		}
	}
	if request.Method == "GET" && query.Get("list-type") == "2" {
		type object struct {
			Key  string
			Size int64
		}
		type common struct{ Prefix string }
		type listing struct {
			XMLName        xml.Name `xml:"ListBucketResult"`
			IsTruncated    bool
			Contents       []object
			CommonPrefixes []common
		}
		result := listing{}
		prefix := query.Get("prefix")
		directories := map[string]bool{}
		for key, data := range peer.objects {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			relative := strings.TrimPrefix(key, prefix)
			if slash := strings.IndexByte(relative, '/'); slash >= 0 {
				directories[prefix+relative[:slash+1]] = true
			} else {
				result.Contents = append(result.Contents, object{Key: key, Size: int64(len(data))})
			}
		}
		for directory := range directories {
			result.CommonPrefixes = append(result.CommonPrefixes, common{directory})
		}
		raw, _ := xml.Marshal(result)
		return managedResponse(request, 200, raw, nil), nil
	}
	switch request.Method {
	case "HEAD":
		data, exists := peer.objects[key]
		if !exists {
			return errorResponse(404, "NoSuchKey")
		}
		response := managedResponse(request, 200, nil, http.Header{"Etag": []string{`"stable"`}})
		response.Header.Set("Content-Length", strconv.Itoa(len(data)))
		response.ContentLength = int64(len(data))
		return response, nil
	case "GET":
		data, exists := peer.objects[key]
		if !exists {
			return errorResponse(404, "NoSuchKey")
		}
		if request.Header.Get("If-Match") != `"stable"` {
			return errorResponse(412, "PreconditionFailed")
		}
		var start, end int
		if _, err := fmt.Sscanf(request.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start < 0 || end >= len(data) || end < start {
			return errorResponse(416, "InvalidRange")
		}
		return managedResponse(request, 206, bytes.Clone(data[start:end+1]), http.Header{"Etag": []string{`"stable"`},
			"Content-Range": []string{fmt.Sprintf("bytes %d-%d/%d", start, end, len(data))}}), nil
	case "PUT":
		if request.Header.Get("If-None-Match") != "*" {
			return errorResponse(400, "InvalidRequest")
		}
		if _, exists := peer.objects[key]; exists {
			return errorResponse(412, "PreconditionFailed")
		}
		data, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		peer.objects[key] = data
		return managedResponse(request, 200, nil, http.Header{"Etag": []string{`"stable"`}}), nil
	case "DELETE":
		delete(peer.objects, key)
		return managedResponse(request, 204, nil, nil), nil
	}
	return errorResponse(400, "InvalidRequest")
}

func managedFixture(t *testing.T, configure func(*managedIOConfig)) (*managedIO, *managedIOPeer) {
	t.Helper()
	peer := &managedIOPeer{objects: map[string][]byte{"owned/data": []byte("abcdefghijklmnopqrstuvwxyz")}}
	client := newStorageClient(defaults(OptionsV1{StorageEndpoint: "http://fixture.invalid", StorageRegion: "test", StorageAccessKey: "test", StorageSecretKey: "test"}), peer)
	config := managedIOConfig{Context: t.Context(), Client: client, Root: "s3://bucket/owned/", MaxObjectBytes: 32 << 20, MaxIOBytes: 64 << 20,
		MaxFileOps: 128, MaxOpenFiles: 4, MaxListEntries: 64, PartSize: 5 << 20, CleanupTimeout: time.Second, AllowWrite: true, AllowDelete: true}
	if configure != nil {
		configure(&config)
	}
	files, err := newManagedIO(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = files.Close() })
	return files, peer
}

func TestManagedFileIORangesAndAuthority(t *testing.T) {
	files, peer := managedFixture(t, nil)
	file, err := files.Open("s3://bucket/owned/data")
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for _, offset := range []int64{0, 8, 20} {
		group.Go(func() {
			data := make([]byte, 3)
			count, err := file.ReadAt(data, offset)
			if err != nil || count != 3 || string(data) != "abcdefghijklmnopqrstuvwxyz"[offset:offset+3] {
				t.Errorf("range: %q %d %v", data, count, err)
			}
		})
	}
	group.Wait()
	if _, err = file.Seek(-2, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 4)
	if count, err := file.Read(data); count != 2 || err != io.EOF || string(data[:count]) != "yz" {
		t.Fatalf("end: %d %q %v", count, data, err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = file.ReadAt(data, 0); !errors.Is(err, fs.ErrClosed) {
		t.Fatal(err)
	}
	for _, uri := range []string{"s3://other/owned/data", "s3://bucket/owned-sibling/data", "s3://bucket/owned/../data", "s3://bucket/owned//data", "s3://bucket/owned/a%2Fb", "s3://bucket/owned/data?q=x"} {
		if _, err = files.Open(uri); !errors.Is(err, ErrAuthority) {
			t.Fatalf("accepted %s: %v", uri, err)
		}
	}
	peer.mu.Lock()
	requests := len(peer.requests)
	peer.mu.Unlock()
	if requests != 5 {
		t.Fatalf("unexpected requests: %d", requests)
	}
}

func TestManagedFileIOBudgetsPrecedeIO(t *testing.T) {
	files, peer := managedFixture(t, func(config *managedIOConfig) { config.MaxIOBytes = 3; config.MaxOpenFiles = 1 })
	file, err := files.Open("s3://bucket/owned/data")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = files.Open("s3://bucket/owned/data"); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err = file.ReadAt(make([]byte, 4), 0); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	peer.mu.Lock()
	requests := len(peer.requests)
	peer.mu.Unlock()
	if requests != 1 {
		t.Fatalf("budget rejected after I/O: %d", requests)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = files.ReadFile("s3://bucket/owned/data"); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
}

type managedCancelBody struct {
	ctx             context.Context
	entered, closed chan struct{}
	once            sync.Once
}

func (body *managedCancelBody) Read([]byte) (int, error) {
	body.once.Do(func() { close(body.entered) })
	<-body.ctx.Done()
	return 0, body.ctx.Err()
}
func (body *managedCancelBody) Close() error { close(body.closed); return nil }

func TestManagedFileIOCloseJoinsActualReader(t *testing.T) {
	files, peer := managedFixture(t, nil)
	file, err := files.Open("s3://bucket/owned/data")
	if err != nil {
		t.Fatal(err)
	}
	entered, closed := make(chan struct{}), make(chan struct{})
	peer.hook = func(request *http.Request) (*http.Response, error, bool) {
		if request.Method != "GET" {
			return nil, nil, false
		}
		response := managedResponse(request, 206, []byte("abcd"), http.Header{"Etag": []string{`"stable"`}, "Content-Range": []string{"bytes 0-3/26"}})
		response.Body = &managedCancelBody{ctx: request.Context(), entered: entered, closed: closed}
		return response, nil, true
	}
	returned := make(chan error, 1)
	go func() { _, err := file.ReadAt(make([]byte, 4), 0); returned <- err }()
	<-entered
	if err = files.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	default:
		t.Fatal("Close returned before actual response body release")
	}
	if err = <-returned; err == nil {
		t.Fatal("canceled read returned success")
	}
	if _, err = files.Create("s3://bucket/owned/late"); !errors.Is(err, fs.ErrClosed) {
		t.Fatal(err)
	}
}

func TestManagedFileIOCreateOnlyAndAbandon(t *testing.T) {
	files, peer := managedFixture(t, nil)
	if err := files.WriteFile("s3://bucket/owned/new", []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := files.WriteFile("s3://bucket/owned/new", []byte("overwrite")); err == nil {
		t.Fatal("existing object overwritten")
	}
	writer, err := files.Create("s3://bucket/owned/abandoned")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Write([]byte("not committed")); err != nil {
		t.Fatal(err)
	}
	if err = files.Close(); err != nil {
		t.Fatal(err)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if string(peer.objects["owned/new"]) != "new" {
		t.Fatal("changed existing data")
	}
	if _, exists := peer.objects["owned/abandoned"]; exists {
		t.Fatal("shutdown committed abandoned data")
	}
}

func TestManagedFileIOMultipartIncrementalAndUnknown(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprint(unknown), func(t *testing.T) {
			var events []managedIOEvent
			files, peer := managedFixture(t, func(config *managedIOConfig) {
				config.Observe = func(event managedIOEvent) { events = append(events, event) }
			})
			input := bytes.Repeat([]byte("x"), (5<<20)+17)
			writer, err := files.Create("s3://bucket/owned/large")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = writer.Write(input); err != nil {
				t.Fatal(err)
			}
			peer.mu.Lock()
			staged := len(peer.parts[1])
			peer.mu.Unlock()
			if staged != 5<<20 {
				t.Fatalf("not incremental: staged=%d", staged)
			}
			if unknown {
				peer.hook = func(request *http.Request) (*http.Response, error, bool) {
					if request.Method != "POST" || request.URL.Query().Get("uploadId") == "" {
						return nil, nil, false
					}
					peer.mu.Lock()
					peer.objects["owned/large"] = bytes.Clone(input)
					peer.mu.Unlock()
					return managedResponse(request, 503, []byte("<Error><Code>SlowDown</Code></Error>"), nil), nil, true
				}
			}
			err = writer.Close()
			if (err != nil) != unknown {
				t.Fatalf("close=%v unknown=%v", err, unknown)
			}
			_ = writer.Close()
			_ = files.Close()
			peer.mu.Lock()
			defer peer.mu.Unlock()
			if !bytes.Equal(peer.objects["owned/large"], input) {
				t.Fatal("multipart data changed")
			}
			commits := 0
			for _, request := range peer.requests {
				if strings.HasPrefix(request, "DELETE") {
					t.Fatal("implicit cleanup after submitted complete", request)
				}
				if strings.HasPrefix(request, "POST") && strings.Contains(request, "uploadId=") {
					commits++
				}
			}
			if commits != 1 {
				t.Fatalf("commit attempts=%d", commits)
			}
			last := events[len(events)-1]
			wanted := Acknowledged
			if unknown {
				wanted = Unknown
			}
			if last.Operation != "multipart-complete" || last.Effect != wanted {
				t.Fatalf("bad effect: %+v", last)
			}
		})
	}
}

func TestManagedFileIOMultipartAbortUsesReservedCapacity(t *testing.T) {
	files, peer := managedFixture(t, func(config *managedIOConfig) { config.MaxFileOps = 3 })
	writer, err := files.Create("s3://bucket/owned/large")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.Write(bytes.Repeat([]byte("x"), (5<<20)+1)); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.parts != nil {
		t.Fatal("multipart upload not aborted with retained request capacity")
	}
	if _, exists := peer.objects["owned/large"]; exists {
		t.Fatal("failed writer committed data")
	}
}

func TestManagedFileIOWalkAndExactDelete(t *testing.T) {
	files, peer := managedFixture(t, nil)
	peer.objects = map[string][]byte{"owned/a": []byte("a"), "owned/sub/b": []byte("b"), "owned/skip/x": []byte("x"), "elsewhere/no": []byte("no")}
	var paths []string
	err := files.WalkDir("s3://bucket/owned", func(uri string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		paths = append(paths, uri)
		if entry.IsDir() && entry.Name() == "skip" {
			return fs.SkipDir
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	wanted := []string{"s3://bucket/owned", "s3://bucket/owned/a", "s3://bucket/owned/skip", "s3://bucket/owned/sub", "s3://bucket/owned/sub/b"}
	if !slices.Equal(paths, wanted) {
		t.Fatalf("walk: %v", paths)
	}
	deleted, err := files.DeleteFiles(t.Context(), []string{"s3://bucket/owned/a", "s3://bucket/elsewhere/no", "s3://bucket/owned/missing"})
	if !errors.Is(err, ErrAuthority) || !slices.Equal(deleted, []string{"s3://bucket/owned/a", "s3://bucket/owned/missing"}) {
		t.Fatalf("deleted=%v err=%v", deleted, err)
	}
	if string(peer.objects["elsewhere/no"]) != "no" {
		t.Fatal("outside object changed")
	}
}

func TestManagedFileIOChangedIdentityAndCleanup(t *testing.T) {
	cause := errors.New("controlled body close failure")
	files, peer := managedFixture(t, nil)
	peer.hook = func(request *http.Request) (*http.Response, error, bool) {
		if request.Method != "GET" {
			return nil, nil, false
		}
		response := managedResponse(request, 206, []byte("abcd"), http.Header{"Etag": []string{`"different"`}, "Content-Range": []string{"bytes 0-3/26"}})
		response.Body = failingCloseBody{Reader: bytes.NewReader([]byte("abcd")), cause: cause}
		return response, nil, true
	}
	file, err := files.Open("s3://bucket/owned/data")
	if err != nil {
		t.Fatal(err)
	}
	if count, err := file.ReadAt(make([]byte, 4), 0); count != 0 || !errors.Is(err, ErrProtocol) {
		t.Fatalf("identity: %d %v", count, err)
	}
	if err = files.Close(); !errors.Is(err, cause) {
		t.Fatal("cleanup evidence lost", err)
	}
}
