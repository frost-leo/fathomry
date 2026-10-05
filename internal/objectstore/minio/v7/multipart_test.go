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
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestMultipartOwnedSetupAndSaturatedCompletion(t *testing.T) {
	server, options := newPeer(t)
	options.MaxActive = 1
	fixture := bindFixture(t, options, 2)
	setup, cancel := context.WithCancel(deadline(t))
	session, root, err := fixture.client.BeginMultipart(setup, deadline(t), deadline(t), correlation("root"), WriteRequest{Key: "owned/session", Size: 3, IfAbsent: true})
	if err != nil || session == nil {
		t.Fatal("begin", err)
	}
	cancel()
	receipt, err := session.Part(deadline(t), childID("part", "root"), 1, 3, strings.NewReader("abc"))
	if result := settle(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	before := server.count()
	if _, err := session.Part(deadline(t), childID("duplicate", "root"), 1, 3, strings.NewReader("abc")); !errors.Is(err, ErrInput) {
		t.Fatal("duplicate part accepted")
	}
	if server.count() != before {
		t.Fatal("invalid part sent")
	}
	if fixture.inbox.Usage().Outstanding != 2 {
		t.Fatal("saturation fixture")
	}
	final, err := session.Complete(deadline(t))
	result := settle(t, final, err)
	if result.Err() != nil || !result.Outcome.Value.Transfer().Complete || !bytes.Equal(server.content("owned/session"), []byte("abc")) {
		t.Fatal("completion", result.Err())
	}
	if _, err := root.WaitReleased(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Complete(deadline(t)); !errors.Is(err, ErrState) {
		t.Fatal("completion replay")
	}
}

func TestEmptyUploadUsesPutWithoutMultipartAllocation(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 1)
	before := server.count()
	session, receipt, err := fixture.client.BeginMultipart(deadline(t), deadline(t), deadline(t), correlation("empty"), WriteRequest{Key: "owned/empty", Size: 0})
	if session != nil || receipt != nil || !errors.Is(err, ErrInput) || server.count() != before {
		t.Fatal("empty session acquired an unusable allocation")
	}
	receipt, err = fixture.client.Put(deadline(t), deadline(t), correlation("empty-put"), WriteRequest{Key: "owned/empty", Size: 0}, strings.NewReader(""))
	result := settle(t, receipt, err)
	if result.Err() != nil || !result.Outcome.Value.Transfer().Complete {
		t.Fatal("empty Put changed", result.Err())
	}
	server.mu.Lock()
	_, exists := server.objects["owned/empty"]
	server.mu.Unlock()
	if !exists {
		t.Fatal("empty object was not observed independently")
	}
}

func TestMultipartOwnedLostCompletionAndFailedAbort(t *testing.T) {
	for _, abortFails := range []bool{false, true} {
		t.Run(fmt.Sprint(abortFails), func(t *testing.T) {
			server, options := newPeer(t)
			fixture := bindFixture(t, options, 2)
			server.mu.Lock()
			server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
				if request.Method == "DELETE" && abortFails {
					errorResponse(writer, 503, "SlowDown")
					return true
				}
				if request.Method != "POST" || request.URL.Query().Get("uploadId") == "" {
					return false
				}
				_, _ = io.Copy(io.Discard, request.Body)
				server.mu.Lock()
				server.store("owned/lost-session", []byte("abc"), nil)
				server.mu.Unlock()
				connection, _, err := writer.(http.Hijacker).Hijack()
				if err == nil {
					_ = connection.Close()
				}
				return true
			}
			server.mu.Unlock()
			session, _, err := fixture.client.BeginMultipart(deadline(t), deadline(t), deadline(t), correlation("root"), WriteRequest{Key: "owned/lost-session", Size: 3})
			if err != nil || session == nil {
				t.Fatal("begin", err)
			}
			part, err := session.Part(deadline(t), childID("part", "root"), 1, 3, strings.NewReader("abc"))
			if value := settle(t, part, err); value.Err() != nil {
				t.Fatal(value.Err())
			}
			final, err := session.Complete(deadline(t))
			result := settle(t, final, err)
			facts := result.Outcome.Value.Transfer()
			if result.Err() == nil || facts.Effect != Unknown || !facts.CompletionAttempted || facts.AbortAcknowledged == abortFails || !bytes.Equal(server.content("owned/lost-session"), []byte("abc")) {
				t.Fatal("uncertainty lost")
			}
			if abortFails && result.Outcome.Cleanup == nil {
				t.Fatal("abort failure lost")
			}
		})
	}
}

func TestMultipartOwnedCleanupAtRequestLimit(t *testing.T) {
	server, options := newPeer(t)
	options.MaxRequests = 2
	fixture := bindFixture(t, options, 2)
	session, _, err := fixture.client.BeginMultipart(deadline(t), deadline(t), deadline(t), correlation("root"), WriteRequest{Key: "owned/limit", Size: 3})
	if err != nil || session == nil {
		t.Fatal(err)
	}
	part, err := session.Part(deadline(t), childID("part", "root"), 1, 3, strings.NewReader("abc"))
	if value := settle(t, part, err); value.Err() != nil {
		t.Fatal(value.Err())
	}
	final, err := session.Complete(deadline(t))
	result := settle(t, final, err)
	if !errors.Is(result.Err(), ErrLimit) || !result.Outcome.Value.Transfer().AbortAcknowledged || len(server.content("owned/limit")) != 0 {
		t.Fatal("cleanup lost to data quota")
	}
}

func TestOwnedMultipartPartialAcquisition(t *testing.T) {
	for _, test := range []struct {
		name, body string
		owned      bool
	}{
		{"matched-partial", "<InitiateMultipartUploadResult><Bucket>fixture</Bucket><Key>owned/partial</Key><UploadId>owned-id</UploadId><broken>", true},
		{"wrong-identity", "<InitiateMultipartUploadResult><Bucket>other</Bucket><Key>owned/partial</Key><UploadId>owned-id</UploadId></InitiateMultipartUploadResult>", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, options := newPeer(t)
			fixture := bindFixture(t, options, 1)
			server.mu.Lock()
			server.uploads["owned-id"] = &pendingUpload{key: "owned/partial", parts: map[int][]byte{}}
			server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
				if request.Method == "POST" && request.URL.Query().Has("uploads") {
					_, _ = io.WriteString(writer, test.body)
					return true
				}
				return false
			}
			server.mu.Unlock()
			session, receipt, err := fixture.client.BeginMultipart(deadline(t), deadline(t), deadline(t), correlation("partial"), WriteRequest{Key: "owned/partial", Size: 1})
			if err != nil || session != nil {
				t.Fatal("accepted setup semantics", err)
			}
			result := settle(t, receipt, nil)
			transfer := result.Outcome.Value.Transfer()
			if result.Err() == nil || transfer.Effect != Unknown || transfer.AbortAttempted != test.owned || (transfer.UploadID != "") != test.owned {
				t.Fatal("allocation authority invented/lost")
			}
			server.mu.Lock()
			_, exists := server.uploads["owned-id"]
			server.mu.Unlock()
			if exists == test.owned {
				t.Fatal("cleanup authority mismatch")
			}
		})
	}
}

func TestOwnedMultipartCompletionRejectsWrongXMLRoot(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 2)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method == "POST" && request.URL.Query().Get("uploadId") != "" {
			_, _ = io.Copy(io.Discard, request.Body)
			_, _ = io.WriteString(writer, "<Unexpected><Bucket>fixture</Bucket><Key>owned/xml</Key><ETag>etag</ETag></Unexpected>")
			return true
		}
		return false
	}
	server.mu.Unlock()
	session, _, err := fixture.client.BeginMultipart(deadline(t), deadline(t), deadline(t), correlation("root"), WriteRequest{Key: "owned/xml", Size: 1})
	if err != nil || session == nil {
		t.Fatal("begin", err)
	}
	part, err := session.Part(deadline(t), childID("part", "root"), 1, 1, strings.NewReader("x"))
	if result := settle(t, part, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	final, err := session.Complete(deadline(t))
	result := settle(t, final, err)
	if !errors.Is(result.Err(), ErrProtocol) || result.Outcome.Value.Transfer().Effect != Unknown || !result.Outcome.Value.Transfer().AbortAcknowledged {
		t.Fatal("invalid terminal XML accepted")
	}
}
