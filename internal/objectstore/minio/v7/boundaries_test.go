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
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

type gatedInput struct {
	entered chan struct{}
	gate    chan struct{}
	closed  bool
}

func (input *gatedInput) Read([]byte) (int, error) {
	close(input.entered)
	<-input.gate
	return 0, io.EOF
}
func (input *gatedInput) Close() error { input.closed = true; return nil }
func TestNonCooperativeInputRetainsOwnershipAndAdmission(t *testing.T) {
	server, options := newPeer(t)
	options.MaxActive = 1
	fixture := bindFixture(t, options, 4)
	input := &gatedInput{entered: make(chan struct{}), gate: make(chan struct{})}
	ctx, cancel := context.WithCancel(deadline(t))
	receipt, err := fixture.client.Put(ctx, deadline(t), correlation("blocked-input"), WriteRequest{Key: "owned/input", Size: 0}, input)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-input.entered:
	case <-deadline(t).Done():
		t.Fatal("input not entered")
	}
	cancel()
	if _, err := fixture.client.Stat(deadline(t), correlation("overload"), Address{Key: "owned/other"}); !errors.Is(err, resource.ErrCapacity) {
		t.Error("active native input lost admission")
	}
	short, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if err := fixture.assembly.Close(short); err == nil || fixture.assembly.Snapshot().Sources[0].Released {
		t.Error("shutdown released a live reader")
	}
	close(input.gate)
	result := settle(t, receipt, nil)
	if !errors.Is(result.Err(), context.Canceled) || result.Outcome.Value.Transfer().Effect != NotSubmitted || input.closed || server.count() != 1 {
		t.Fatal("reader ownership/cancellation lost")
	}
}
func TestControlBytesCountAndMalformedResponse(t *testing.T) {
	server, options := newPeer(t)
	options.MaxResponseBytes = 1024
	options.MaxEntries = 2
	fixture := bindFixture(t, options, 8)
	server.mu.Lock()
	for _, key := range []string{"owned/a", "owned/b", "owned/c"} {
		server.store(key, []byte("x"), nil)
	}
	server.mu.Unlock()
	receipt, err := fixture.client.List(deadline(t), correlation("count"), ListRequest{Prefix: "owned/"})
	bounded := settle(t, receipt, err)
	if !errors.Is(bounded.Err(), ErrLimit) || len(bounded.Outcome.Value.ObjectsCopy()) != 2 || bounded.Outcome.Value.Complete() {
		t.Fatal("entry count is not independently bounded")
	}
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.URL.Query().Get("list-type") != "2" {
			return false
		}
		_, _ = io.WriteString(writer, "<ListBucketResult>"+strings.Repeat(" ", 2048)+"</ListBucketResult>")
		return true
	}
	server.mu.Unlock()
	receipt, err = fixture.client.List(deadline(t), correlation("bytes"), ListRequest{Prefix: "owned/"})
	if result := settle(t, receipt, err); !errors.Is(result.Err(), ErrLimit) || result.Outcome.Value.Complete() {
		t.Fatal("control byte limit bypassed")
	}
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.URL.Query().Get("list-type") != "2" {
			return false
		}
		_, _ = io.WriteString(writer, "<ListBucketResult><Contents>")
		return true
	}
	server.mu.Unlock()
	receipt, err = fixture.client.List(deadline(t), correlation("malformed"), ListRequest{Prefix: "owned/"})
	if result := settle(t, receipt, err); result.Err() == nil || result.Outcome.Value.Complete() {
		t.Fatal("malformed native XML became successful empty")
	}
}
func TestMetadataCannotInjectConditionsAndTagsAreFrozen(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 8)
	metadata := map[string]string{"content-type": "not-a-header", "owner": "before"}
	receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("metadata"), WriteRequest{Key: "owned/meta", Size: 3, ContentType: "text/plain", Metadata: metadata}, bytes.NewReader([]byte("abc")))
	metadata["owner"] = "after"
	result := settle(t, receipt, err)
	if result.Err() != nil {
		t.Fatal(result.Err())
	}
	server.mu.Lock()
	for _, request := range server.requests {
		if request.method == "PUT" && (request.header.Get("Content-Type") != "text/plain" || request.header.Get("X-Amz-Meta-Owner") != "before" || request.header.Get("X-Amz-Meta-Content-Type") != "not-a-header") {
			t.Error("metadata overrode a native header or aliased input")
		}
	}
	server.mu.Unlock()
	tags := map[string]string{"purpose": "before"}
	receipt, err = fixture.client.SetTags(deadline(t), correlation("tags"), Address{Key: "owned/meta"}, tags)
	clear(tags)
	if result := settle(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	receipt, err = fixture.client.GetTags(deadline(t), correlation("tag-observation"), Address{Key: "owned/meta"})
	if result := settle(t, receipt, err); result.Err() != nil || result.Outcome.Value.TagsCopy()["purpose"] != "before" {
		t.Fatal("tag map was borrowed asynchronously")
	}
	receipt, err = fixture.client.SetTags(deadline(t), correlation("tags-remove"), Address{Key: "owned/meta"}, nil)
	if result := settle(t, receipt, err); result.Err() != nil {
		t.Fatal("native tag removal failed", result.Err())
	}
	receipt, err = fixture.client.GetTags(deadline(t), correlation("tags-empty"), Address{Key: "owned/meta"})
	if result := settle(t, receipt, err); result.Err() != nil || len(result.Outcome.Value.TagsCopy()) != 0 || !result.Outcome.Value.Complete() {
		t.Fatal("successful empty tag set lost", result.Err())
	}
	before := server.count()
	if _, err := fixture.client.Put(deadline(t), deadline(t), correlation("injection"), WriteRequest{Key: "owned/rejected", Metadata: map[string]string{"x-amz-acl": "public-read"}}, bytes.NewReader(nil)); !errors.Is(err, ErrInput) || server.count() != before {
		t.Fatal("reserved native metadata accepted")
	}
}
func FuzzReadInputAndResponseBound(f *testing.F) {
	f.Add([]byte("payload"), uint16(3))
	f.Add([]byte{}, uint16(0))
	f.Fuzz(func(t *testing.T, data []byte, limit uint16) {
		if len(data) > 65536 {
			t.Skip()
		}
		maximum := int(limit) % 1024
		buffer := make([]byte, maximum)
		count, err := readInput(context.Background(), bytes.NewReader(data), buffer)
		if count != min(maximum, len(data)) || err != nil && err != io.EOF {
			t.Fatal("bounded input consumption changed")
		}
		value := defaults(OptionsV1{})
		value.MaxResponseBytes = int64(maximum)
		state := newExchange(value, nil, false)
		body := &boundedBody{body: io.NopCloser(bytes.NewReader(data)), state: state}
		content, err := io.ReadAll(body)
		if len(content) > maximum || len(data) > maximum && !errors.Is(err, ErrLimit) || len(data) <= maximum && err != nil {
			t.Fatal("response bound changed")
		}
	})
}
