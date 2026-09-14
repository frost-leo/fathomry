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
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	native "github.com/minio/minio-go/v7"
)

func TestReviewUploadWaitsForRequestBodyReleaseBeforeBufferReuse(t *testing.T) {
	_, options := newPeer(t)
	fixture := bindFixture(t, options, 4)
	retained := make(chan struct{})
	release := make(chan struct{})
	consumed := make(chan []byte, 1)
	fixture.client.owner.wire.base = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		responseBody := ""
		switch {
		case request.URL.Query().Has("uploads"):
			responseBody = "<InitiateMultipartUploadResult><Bucket>fixture</Bucket><Key>owned/request-lifetime</Key><UploadId>review-upload</UploadId></InitiateMultipartUploadResult>"
		case request.URL.Query().Get("partNumber") == "1":
			go func() {
				<-release
				body, err := io.ReadAll(request.Body)
				if err != nil {
					t.Error(err)
				}
				if err := request.Body.Close(); err != nil {
					t.Error(err)
				}
				consumed <- body
			}()
			close(retained)
		case request.Method == http.MethodPost:
			responseBody = "<CompleteMultipartUploadResult><Bucket>fixture</Bucket><Key>owned/request-lifetime</Key><ETag>complete</ETag></CompleteMultipartUploadResult>"
		}
		if request.Body != nil && request.URL.Query().Get("partNumber") != "1" {
			_, _ = io.Copy(io.Discard, request.Body)
			_ = request.Body.Close()
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Etag": {"\"part\""}}, Body: io.NopCloser(strings.NewReader(responseBody)), Request: request}, nil
	})
	part := bytes.Repeat([]byte("a"), 5<<20)
	input := io.MultiReader(bytes.NewReader(part), strings.NewReader("b"))
	receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("request-lifetime"), WriteRequest{Key: "owned/request-lifetime", Size: int64(len(part) + 1)}, input)
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	select {
	case <-retained:
	case <-deadline(t).Done():
		close(release)
		t.Fatal("native transport did not retain the first request body")
	}
	ctx, cancel := context.WithTimeout(deadline(t), 25*time.Millisecond)
	_, waitErr := receipt.WaitReleased(ctx)
	cancel()
	close(release)
	captured := <-consumed
	result := settle(t, receipt, nil)
	if !errors.Is(waitErr, context.DeadlineExceeded) {
		t.Error("upload released while transport still retained its request body")
	}
	if !bytes.Equal(captured, part) {
		t.Error("multipart buffer was reused before transport finished consuming its request body")
	}
	if result.Err() != nil || !result.Outcome.Value.Transfer().Complete {
		t.Fatal("upload failed after transport released its body", result.Err())
	}
}

func TestReviewCopyPreservesOpaqueETagBytes(t *testing.T) {
	for _, etag := range []string{"ordinary-tag", "opaque\u200btag"} {
		t.Run(etag, func(t *testing.T) {
			server, options := newPeer(t)
			fixture := bindFixture(t, options, 4)
			server.mu.Lock()
			server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
				if request.Method == http.MethodHead {
					objectHeaders(writer, storedObject{body: []byte("abc"), etag: "unused"})
					writer.Header().Set("ETag", "\""+etag+"\"")
					return true
				}
				if request.Method == http.MethodPut {
					if request.Header.Get("X-Amz-Copy-Source-If-Match") != "\""+etag+"\"" {
						errorResponse(writer, http.StatusPreconditionFailed, "PreconditionFailed")
						return true
					}
					_, _ = io.WriteString(writer, "<CopyObjectResult><ETag>copied</ETag></CopyObjectResult>")
					return true
				}
				return false
			}
			server.mu.Unlock()
			receipt, err := fixture.client.Copy(deadline(t), correlation("opaque-etag"), CopyRequest{Source: Address{Key: "owned/source"}, Key: "owned/copied"})
			result := settle(t, receipt, err)
			if result.Err() != nil || result.Outcome.Value.Transfer().Effect != Acknowledged {
				t.Fatal("copy changed a supported opaque source ETag", result.Err())
			}
		})
	}
}

func TestReviewCopyPreservesEmbeddedNativeError(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 4)
	server.mu.Lock()
	server.store("owned/source", []byte("abc"), nil)
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method == http.MethodPut {
			errorResponse(writer, http.StatusOK, "SlowDown")
			return true
		}
		return false
	}
	server.mu.Unlock()
	receipt, err := fixture.client.Copy(deadline(t), correlation("embedded-error"), CopyRequest{Source: Address{Key: "owned/source"}, Key: "owned/copied"})
	result := settle(t, receipt, err)
	var response native.ErrorResponse
	if !errors.As(result.Err(), &response) || response.Code != "SlowDown" || response.StatusCode != http.StatusOK || response.RequestID != "fixture" {
		t.Error("copy lost the native error embedded in an HTTP 200 response", result.Err())
	}
	if result.Outcome.Value.Transfer().Effect != Unknown || result.Outcome.Value.Complete() {
		t.Error("embedded copy error became an acknowledgement")
	}
}

func TestReviewCopyRejectsIncompleteAndWrongRootResponse(t *testing.T) {
	for _, test := range []struct {
		name     string
		body     string
		truncate bool
	}{
		{name: "wrong-root", body: "<NotCopy><ETag>copied</ETag></NotCopy>"},
		{name: "trailing-error", body: "<CopyObjectResult><ETag>copied</ETag></CopyObjectResult><Error><Code>SlowDown</Code></Error>"},
		{name: "trailing-data", body: "<CopyObjectResult><ETag>copied</ETag></CopyObjectResult>garbage"},
		{name: "truncated", body: "<CopyObjectResult><ETag>copied</ETag></CopyObjectResult>", truncate: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, options := newPeer(t)
			fixture := bindFixture(t, options, 4)
			server.mu.Lock()
			server.store("owned/source", []byte("abc"), nil)
			server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
				if request.Method != http.MethodPut {
					return false
				}
				if test.truncate {
					writer.Header().Set("Content-Length", strconv.Itoa(len(test.body)+1))
				}
				_, _ = io.WriteString(writer, test.body)
				return true
			}
			server.mu.Unlock()
			receipt, err := fixture.client.Copy(deadline(t), correlation("copy-response"), CopyRequest{Source: Address{Key: "owned/source"}, Key: "owned/copied"})
			result := settle(t, receipt, err)
			_, present := result.Outcome.Value.Object()
			if !errors.Is(result.Err(), ErrProtocol) || result.Outcome.Value.Transfer().Effect != Unknown || result.Outcome.Value.Complete() || present {
				t.Error("malformed or incomplete copy response became an acknowledgement", result.Err())
			}
			if test.truncate && !errors.Is(result.Err(), io.ErrUnexpectedEOF) {
				t.Error("copy lost the truncated-response error", result.Err())
			}
		})
	}
}

func TestReviewUploadPartAndRequestLimits(t *testing.T) {
	const partBytes = 5 << 20
	for _, test := range []struct {
		name        string
		known       bool
		extra       int
		requests    int
		wantErr     error
		wantCleanup bool
	}{
		{name: "known-exact-parts", known: true, requests: 4},
		{name: "unknown-exact-parts", requests: 4},
		{name: "trailing-byte", extra: 1, requests: 4, wantErr: ErrLimit},
		{name: "completion-budget", requests: 3, wantErr: ErrLimit, wantCleanup: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, options := newPeer(t)
			options.MaxParts = 2
			options.MaxTransferBytes = 2 * partBytes
			options.MaxReadBytes = 1024
			options.MaxRequests = test.requests
			fixture := bindFixture(t, options, 4)
			before := server.count()
			body := bytes.Repeat([]byte("a"), 2*partBytes+test.extra)
			size := int64(-1)
			if test.known {
				size = int64(len(body))
			}
			receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("part-bound"), WriteRequest{Key: "owned/part-bound", Size: size}, bytes.NewReader(body))
			result := settle(t, receipt, err)
			transfer := result.Outcome.Value.Transfer()
			if server.count()-before != test.requests || transfer.Bytes != int64(len(body)) || transfer.PartsAcknowledged != 2 {
				t.Error("part, request, or consumed-input accounting changed")
			}
			if test.wantErr == nil {
				if result.Err() != nil || transfer.Effect != Acknowledged || !transfer.Complete || !bytes.Equal(server.content("owned/part-bound"), body) {
					t.Fatal("exact part-limit upload was not completed", result.Err())
				}
				return
			}
			if !errors.Is(result.Outcome.Primary, test.wantErr) || transfer.Effect != Unknown || transfer.Complete || len(server.content("owned/part-bound")) != 0 {
				t.Error("over-limit upload was acknowledged or lost its cause", result.Err())
			}
			if test.wantCleanup {
				if !errors.Is(result.Outcome.Cleanup, ErrLimit) || !transfer.AbortAttempted || transfer.AbortAcknowledged {
					t.Error("exhausted request budget hid unresolved abort", result.Err())
				}
				receipt, err = fixture.client.Abort(deadline(t), correlation("part-bound-cleanup"), Upload{Key: "owned/part-bound", ID: transfer.UploadID})
				if result := settle(t, receipt, err); result.Err() != nil || !result.Outcome.Value.Transfer().AbortAcknowledged {
					t.Fatal("separately requested cleanup failed", result.Err())
				}
			} else if result.Outcome.Cleanup != nil || !transfer.AbortAcknowledged || transfer.CompletionAttempted {
				t.Error("overflow was published or not aborted", result.Err())
			}
			server.mu.Lock()
			remaining := len(server.uploads)
			server.mu.Unlock()
			if remaining != 0 {
				t.Error("test-owned upload remained after acknowledged cleanup")
			}
		})
	}
}

func TestReviewEarlyUploadReplyReleasesNativeRequestBody(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 4)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method != http.MethodPut || request.URL.Query().Get("partNumber") != "1" {
			return false
		}
		writer.Header().Set("Content-Type", "application/xml")
		writer.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(writer, "<Error><Code>AccessDenied</Code><Message>"+strings.Repeat("x", 256<<10)+"</Message></Error>")
		return true
	}
	server.mu.Unlock()
	receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("early-reply"), WriteRequest{Key: "owned/early-reply", Size: -1}, bytes.NewReader(bytes.Repeat([]byte("a"), 5<<20+1)))
	result := settle(t, receipt, err)
	transfer := result.Outcome.Value.Transfer()
	if !errors.Is(result.Outcome.Primary, ErrDenied) || result.Outcome.Cleanup != nil || transfer.Effect != Unknown || !transfer.AbortAcknowledged || transfer.CompletionAttempted {
		t.Fatal("early rejecting response blocked release or lost cleanup evidence", result.Err())
	}
}

func TestReviewUploadRetainsInitiationIDAlongsideNativeError(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 4)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method != http.MethodPost || !request.URL.Query().Has("uploads") {
			return false
		}
		server.mu.Lock()
		server.uploads["partial-initiation"] = &pendingUpload{key: "owned/partial-initiation", parts: map[int][]byte{}}
		server.mu.Unlock()
		_, _ = io.WriteString(writer, "<InitiateMultipartUploadResult><Bucket>fixture</Bucket><Key>owned/partial-initiation</Key><UploadId>partial-initiation</UploadId><unfinished>")
		return true
	}
	server.mu.Unlock()
	receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("partial-initiation"), WriteRequest{Key: "owned/partial-initiation", Size: -1}, bytes.NewReader(bytes.Repeat([]byte("a"), 5<<20+1)))
	result := settle(t, receipt, err)
	transfer := result.Outcome.Value.Transfer()
	var syntax *xml.SyntaxError
	if !errors.As(result.Outcome.Primary, &syntax) || transfer.Effect != Unknown || transfer.CompletionAttempted {
		t.Error("partial initiation lost its native error or changed effect evidence", result.Err())
	}
	if transfer.UploadID != "partial-initiation" || !transfer.AbortAttempted || !transfer.AbortAcknowledged || result.Outcome.Cleanup != nil {
		t.Error("observed upload ID was discarded instead of retaining cleanup responsibility", result.Err())
	}
	server.mu.Lock()
	remaining := len(server.uploads)
	server.mu.Unlock()
	if remaining != 0 {
		t.Error("known test-owned upload remained after malformed initiation response")
	}
}

func TestReviewUploadRejectsUnrelatedAllocationIdentity(t *testing.T) {
	for _, test := range []struct {
		name      string
		root      string
		bucket    string
		key       string
		id        string
		extra     string
		tail      string
		truncated bool
	}{
		{name: "wrong-root", root: "OtherOperationResult", bucket: "fixture", key: "owned/allocation"},
		{name: "wrong-bucket", root: "InitiateMultipartUploadResult", bucket: "other-bucket", key: "owned/allocation"},
		{name: "wrong-key", root: "InitiateMultipartUploadResult", bucket: "fixture", key: "owned/other-key"},
		{name: "missing-bucket", root: "InitiateMultipartUploadResult", key: "owned/allocation"},
		{name: "missing-key", root: "InitiateMultipartUploadResult", bucket: "fixture"},
		{name: "duplicate-bucket", root: "InitiateMultipartUploadResult", bucket: "fixture", key: "owned/allocation", extra: "<Bucket>fixture</Bucket>"},
		{name: "duplicate-key", root: "InitiateMultipartUploadResult", bucket: "fixture", key: "owned/allocation", extra: "<Key>owned/allocation</Key>"},
		{name: "duplicate-id", root: "InitiateMultipartUploadResult", bucket: "fixture", key: "owned/allocation", extra: "<UploadId>preexisting-upload</UploadId>"},
		{name: "conflicting-id", root: "InitiateMultipartUploadResult", bucket: "fixture", key: "owned/allocation", id: "unrelated-upload", extra: "<UploadId>preexisting-upload</UploadId>"},
		{name: "trailing-root", root: "InitiateMultipartUploadResult", bucket: "fixture", key: "owned/allocation", tail: "<OtherOperationResult/>"},
		{name: "partial-wrong-key", root: "InitiateMultipartUploadResult", bucket: "fixture", key: "owned/other-key", truncated: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, options := newPeer(t)
			fixture := bindFixture(t, options, 4)
			original := []byte("preexisting part must not change")
			server.mu.Lock()
			server.uploads["preexisting-upload"] = &pendingUpload{key: "owned/allocation", parts: map[int][]byte{1: bytes.Clone(original)}}
			server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
				if request.Method != http.MethodPost || !request.URL.Query().Has("uploads") {
					return false
				}
				id := test.id
				if id == "" {
					id = "preexisting-upload"
				}
				reply := "<" + test.root + "><Bucket>" + test.bucket + "</Bucket><Key>" + test.key + "</Key><UploadId>" + id + "</UploadId>" + test.extra
				if test.truncated {
					reply += "<unfinished>"
				} else {
					reply += "</" + test.root + ">" + test.tail
				}
				_, _ = io.WriteString(writer, reply)
				return true
			}
			server.mu.Unlock()
			before := server.count()
			input := failingReader{Reader: bytes.NewReader(bytes.Repeat([]byte("a"), 5<<20)), cause: errors.New("input failed after first part")}
			receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("allocation-identity"), WriteRequest{Key: "owned/allocation", Size: -1}, input)
			result := settle(t, receipt, err)
			transfer := result.Outcome.Value.Transfer()
			if !errors.Is(result.Outcome.Primary, ErrProtocol) || transfer.UploadID != "" || transfer.AbortAttempted || transfer.PartsAcknowledged != 0 || transfer.Effect != Unknown {
				t.Error("unrelated allocation response conferred ownership of an existing upload", result.Err())
			}
			if server.count() != before+1 {
				t.Error("invalid allocation caused a part, completion, or abort request")
			}
			server.mu.Lock()
			upload, present := server.uploads["preexisting-upload"]
			unchanged := present && len(upload.parts) == 1 && bytes.Equal(upload.parts[1], original)
			server.mu.Unlock()
			if !unchanged {
				t.Error("preexisting upload was mutated or aborted by the failed allocation")
			}
		})
	}
}
