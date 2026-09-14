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
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	native "github.com/minio/minio-go/v7"
)

func TestConditionalMultipartAndSigningRegression(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 16)
	for _, size := range []int{0, 17, 5 << 20, 5<<20 + 1, 10<<20 + 1} {
		key := "owned/size-" + strconv.Itoa(size)
		body := bytes.Repeat([]byte{byte(size)}, size)
		receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("create"), WriteRequest{Key: key, Size: int64(size), IfAbsent: true, ContentType: "application/test"}, bytes.NewReader(body))
		result := settle(t, receipt, err)
		if result.Err() != nil || result.Outcome.Value.Transfer().Effect != Acknowledged || !bytes.Equal(server.content(key), body) {
			t.Fatal("selected multipart profile failed", result.Err())
		}
		receipt, err = fixture.client.Put(deadline(t), deadline(t), correlation("condition-failure"), WriteRequest{Key: key, Size: int64(size), IfAbsent: true}, bytes.NewReader(body))
		rejected := settle(t, receipt, err)
		if !errors.Is(rejected.Err(), ErrCondition) || rejected.Outcome.Value.Transfer().Effect == Acknowledged {
			t.Fatal("final condition dropped")
		}
		if size >= 5<<20 && (!rejected.Outcome.Value.Transfer().AbortAttempted || !rejected.Outcome.Value.Transfer().AbortAcknowledged) {
			t.Fatal("failed condition cleanup missing")
		}
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	completions := 0
	for _, request := range server.requests {
		if request.method == "POST" && request.query.Get("uploadId") != "" {
			completions++
			if request.header.Get("If-None-Match") != "*" {
				t.Fatal("condition absent at final native request")
			}
		}
		if request.method == "PUT" && request.header.Get("X-Amz-Copy-Source") == "" {
			if strings.Contains(request.header.Get("X-Amz-Content-Sha256"), "STREAMING") {
				t.Fatal("unqualified streaming signer used")
			}
		}
	}
	if completions < 6 || len(server.uploads) != 0 {
		t.Fatal("multipart/cleanup controls were not exercised")
	}
}
func TestConcurrentUploadsHaveIndependentPartHashes(t *testing.T) {
	server, options := newPeer(t)
	options.MaxActive = 4
	fixture := bindFixture(t, options, 8)
	var group sync.WaitGroup
	for index := range 4 {
		group.Go(func() {
			body := bytes.Repeat([]byte{byte(index + 1)}, 10<<20+1)
			key := "owned/parallel-" + string(rune('a'+index))
			receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation(key), WriteRequest{Key: key, Size: -1, IfAbsent: true}, bytes.NewReader(body))
			result := settle(t, receipt, err)
			digest := sha256.Sum256(body)
			if result.Err() != nil || result.Outcome.Value.Transfer().SHA256 != hex.EncodeToString(digest[:]) || !bytes.Equal(server.content(key), body) {
				t.Error("part integrity/race control failed", result.Err())
			}
		})
	}
	group.Wait()
}

type failingReader struct {
	io.Reader
	cause error
}

func (reader failingReader) Read(buffer []byte) (int, error) {
	count, err := reader.Reader.Read(buffer)
	if errors.Is(err, io.EOF) {
		return count, reader.cause
	}
	return count, err
}
func TestMultipartReaderAndAbortFailuresRemainSeparate(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 8)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method == "DELETE" && request.URL.Query().Get("uploadId") != "" {
			errorResponse(writer, 503, "SlowDown")
			return true
		}
		return false
	}
	server.mu.Unlock()
	inputErr := errors.New("reader-failure")
	input := failingReader{bytes.NewReader(bytes.Repeat([]byte("a"), 5<<20)), inputErr}
	receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("reader-abort"), WriteRequest{Key: "owned/failure", Size: -1}, input)
	result := settle(t, receipt, err)
	if !errors.Is(result.Outcome.Primary, inputErr) || !errors.Is(result.Outcome.Cleanup, ErrCleanup) ||
		result.Outcome.Value.Transfer().AbortAcknowledged || result.Outcome.Value.Transfer().Effect != Unknown {
		t.Fatal("primary or cleanup loss", result.Err())
	}
	var nativeErr native.ErrorResponse
	if !errors.As(result.Outcome.Cleanup, &nativeErr) || nativeErr.Code != "SlowDown" {
		t.Fatal("native cleanup cause lost")
	}
	uploadID := result.Outcome.Value.Transfer().UploadID
	receipt, err = fixture.client.ListUploads(deadline(t), correlation("inspect"), UploadQuery{Prefix: "owned/"})
	listed := settle(t, receipt, err)
	if listed.Err() != nil || len(listed.Outcome.Value.UploadsCopy()) != 1 || listed.Outcome.Value.UploadsCopy()[0].ID != uploadID {
		t.Fatal("residual upload evidence missing", listed.Err())
	}
	receipt, err = fixture.client.ListParts(deadline(t), correlation("parts"), Upload{Key: "owned/failure", ID: uploadID}, 0)
	parts := settle(t, receipt, err)
	if parts.Err() != nil || len(parts.Outcome.Value.PartsCopy()) != 1 {
		t.Fatal("part inspection failed", parts.Err())
	}
	server.mu.Lock()
	server.hook = nil
	server.mu.Unlock()
	receipt, err = fixture.client.Abort(deadline(t), correlation("owned-abort"), Upload{Key: "owned/failure", ID: uploadID})
	if result := settle(t, receipt, err); result.Err() != nil || !result.Outcome.Value.Transfer().AbortAcknowledged {
		t.Fatal("explicit cleanup failed", result.Err())
	}
}
func TestLostCompletionRemainsUnknownAfterAbort(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 4)
	expected := bytes.Repeat([]byte("z"), 5<<20+1)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method != "POST" || request.URL.Query().Get("uploadId") == "" {
			return false
		}
		_, _ = io.Copy(io.Discard, request.Body)
		server.mu.Lock()
		server.store("owned/lost", expected, nil)
		server.mu.Unlock()
		connection, _, err := writer.(http.Hijacker).Hijack()
		if err == nil {
			_ = connection.Close()
		}
		return true
	}
	server.mu.Unlock()
	receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("lost"), WriteRequest{Key: "owned/lost", Size: int64(len(expected))}, bytes.NewReader(expected))
	result := settle(t, receipt, err)
	transfer := result.Outcome.Value.Transfer()
	if result.Err() == nil || transfer.Effect != Unknown || !transfer.CompletionAttempted || !transfer.AbortAcknowledged || !bytes.Equal(server.content("owned/lost"), expected) {
		t.Fatal("lost response interpreted as rollback")
	}
}
func TestExactInputAndCapacityBoundaries(t *testing.T) {
	for _, test := range []struct {
		name string
		size int64
		body []byte
		want error
	}{
		{"short", 4, []byte("abc"), io.ErrUnexpectedEOF}, {"long", 2, []byte("abc"), ErrInput},
		{"empty", 0, []byte{}, nil}, {"unknown-empty", -1, []byte{}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, options := newPeer(t)
			fixture := bindFixture(t, options, 4)
			before := server.count()
			receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("input"), WriteRequest{Key: "owned/input", Size: test.size}, bytes.NewReader(test.body))
			result := settle(t, receipt, err)
			if test.want != nil {
				if !errors.Is(result.Err(), test.want) || server.count() != before {
					t.Fatal("invalid small input caused I/O or lost cause")
				}
			} else if result.Err() != nil || result.Outcome.Value.Transfer().Effect != Acknowledged {
				t.Fatal(result.Err())
			}
		})
	}
	server, options := newPeer(t)
	options.MaxTransferBytes = 5 << 20
	options.MaxReadBytes = 1024
	fixture := bindFixture(t, options, 4)
	receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("overlimit"), WriteRequest{Key: "owned/too-large", Size: -1}, bytes.NewReader(bytes.Repeat([]byte("x"), 5<<20+1)))
	result := settle(t, receipt, err)
	if !errors.Is(result.Err(), ErrLimit) || result.Outcome.Value.Transfer().CompletionAttempted || len(server.content("owned/too-large")) > 0 {
		t.Fatal("unread trailing input became completed object")
	}
}
func TestCanceledPutUsesExplicitCleanupContext(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 4)
	ctx, cancel := context.WithCancel(deadline(t))
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method == "PUT" && request.URL.Query().Get("partNumber") == "1" {
			_, _ = io.Copy(io.Discard, request.Body)
			cancel()
			errorResponse(writer, 500, "InternalError")
			return true
		}
		return false
	}
	server.mu.Unlock()
	receipt, err := fixture.client.Put(ctx, deadline(t), correlation("cancel"), WriteRequest{Key: "owned/canceled", Size: -1}, bytes.NewReader(bytes.Repeat([]byte("x"), 5<<20+1)))
	result := settle(t, receipt, err)
	if !errors.Is(result.Err(), context.Canceled) || !result.Outcome.Value.Transfer().AbortAcknowledged {
		t.Fatal("canceled work blocked separately authorized cleanup", result.Err())
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	for _, request := range server.requests {
		if request.method == "POST" && request.query.Get("uploadId") != "" {
			t.Fatal("canceled upload completed")
		}
	}
}
