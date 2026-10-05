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
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestCursorVersionMarkersNeedNotSort(t *testing.T) {
	server, options := newPeer(t)
	options.MaxEntries = 1
	fixture := bindFixture(t, options, 4)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		marker := request.URL.Query().Get("key-marker")
		body := ""
		switch marker {
		case "":
			body = "<IsTruncated>true</IsTruncated><NextKeyMarker>owned/key-z</NextKeyMarker><NextVersionIdMarker>v1</NextVersionIdMarker><Version><Key>owned/key</Key><VersionId>v1</VersionId><Size>1</Size></Version>"
		case "owned/key-z":
			body = "<IsTruncated>true</IsTruncated><NextKeyMarker>owned/key-a</NextKeyMarker><NextVersionIdMarker>v2</NextVersionIdMarker><Version><Key>owned/key</Key><VersionId>v2</VersionId><Size>1</Size></Version>"
		case "owned/key-a":
			body = "<IsTruncated>false</IsTruncated><Version><Key>owned/key</Key><VersionId>v3</VersionId><Size>1</Size></Version>"
		default:
			t.Error("marker was reconstructed")
		}
		_, _ = fmt.Fprintf(writer, "<ListVersionsResult><Name>fixture</Name>%s</ListVersionsResult>", body)
		return true
	}
	server.mu.Unlock()
	cursor, root, err := fixture.client.Enumerate(deadline(t), deadline(t), correlation("root"), ListRequest{Prefix: "owned/", Versions: true})
	if err != nil {
		t.Fatal(err)
	}
	// Claim/drain incrementally: four bounded outputs must fit a two-node tree.
	claimRoot(t, fixture.inbox)
	total := 0
	for index := range 4 {
		receipt, err := cursor.Next(deadline(t), childID(fmt.Sprint("next", index), "root"))
		result := settle(t, receipt, err)
		if result.Err() != nil {
			t.Fatal(result.Err())
		}
		total += len(result.Outcome.Value.ObjectsCopy())
		record, err := fixture.inbox.Next(deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if err := record.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if total != 3 || settle(t, root, nil).Err() != nil {
		t.Fatal("opaque pair continuation rejected")
	}
}

func TestCursorSameKeyVersionContinuation(t *testing.T) {
	server, options := newPeer(t)
	options.MaxEntries = 1
	fixture := bindFixture(t, options, 4)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method != "GET" {
			return false
		}
		if request.URL.Query().Get("version-id-marker") == "" {
			_, _ = io.WriteString(writer, "<ListVersionsResult><Name>fixture</Name><IsTruncated>true</IsTruncated><NextKeyMarker>owned/key</NextKeyMarker><NextVersionIdMarker>v+%/1</NextVersionIdMarker><Version><Key>owned/key</Key><VersionId>v+%/1</VersionId><Size>1</Size></Version></ListVersionsResult>")
		} else {
			if request.URL.Query().Get("key-marker") != "owned/key" || request.URL.Query().Get("version-id-marker") != "v+%/1" {
				t.Error("opaque version continuation changed")
			}
			_, _ = io.WriteString(writer, "<ListVersionsResult><Name>fixture</Name><IsTruncated>false</IsTruncated><DeleteMarker><Key>owned/key</Key><VersionId>v2</VersionId></DeleteMarker></ListVersionsResult>")
		}
		return true
	}
	server.mu.Unlock()
	cursor, root, err := fixture.client.Enumerate(deadline(t), deadline(t), correlation("root"), ListRequest{Prefix: "owned/", Versions: true})
	if err != nil {
		t.Fatal(err)
	}
	var objects []Object
	for index := range 3 {
		receipt, err := cursor.Next(deadline(t), childID(fmt.Sprint("next", index), "root"))
		value := settle(t, receipt, err)
		if value.Err() != nil {
			t.Fatal(value.Err())
		}
		objects = append(objects, value.Outcome.Value.ObjectsCopy()...)
		if value.Outcome.Value.Complete() {
			break
		}
	}
	final := settle(t, root, nil)
	if final.Err() != nil || !final.Outcome.Value.Complete() || len(objects) != 2 || objects[0].Address.VersionID != "v+%/1" || !objects[1].DeleteMarker {
		t.Fatal("version loss", final.Err())
	}
}

func TestCursorRejectsMalformedAndNoProgress(t *testing.T) {
	for _, body := range []string{
		"<ListBucketResult><Name>fixture</Name></ListBucketResult>",
		"<ListBucketResult><Name>other</Name><IsTruncated>false</IsTruncated></ListBucketResult>",
		"<ListBucketResult><Name>fixture</Name><IsTruncated>true</IsTruncated><NextContinuationToken>same</NextContinuationToken></ListBucketResult>",
	} {
		server, options := newPeer(t)
		fixture := bindFixture(t, options, 2)
		server.mu.Lock()
		server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
			_, _ = io.WriteString(writer, body)
			return true
		}
		server.mu.Unlock()
		cursor, root, err := fixture.client.Enumerate(deadline(t), deadline(t), correlation("root"), ListRequest{Prefix: "owned/"})
		if err != nil {
			t.Fatal(err)
		}
		next, err := cursor.Next(deadline(t), childID("next", "root"))
		value := settle(t, next, err)
		if !errors.Is(value.Err(), ErrProtocol) || value.Outcome.Value.Complete() {
			t.Fatal("malformed accepted", value.Err())
		}
		if value := settle(t, root, nil); !errors.Is(value.Err(), ErrProtocol) {
			t.Fatal("root lost protocol failure")
		}
	}
}

func TestCursorCancellationJoinsFetch(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 2)
	entered := make(chan struct{})
	finished := make(chan struct{})
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		close(entered)
		<-request.Context().Done()
		close(finished)
		return true
	}
	server.mu.Unlock()
	cursor, root, err := fixture.client.Enumerate(deadline(t), deadline(t), correlation("root"), ListRequest{Prefix: "owned/"})
	if err != nil {
		t.Fatal(err)
	}
	work, cancel := context.WithCancel(deadline(t))
	next, err := cursor.Next(work, childID("next", "root"))
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	cancel()
	if result := settle(t, next, nil); !errors.Is(result.Err(), context.Canceled) {
		t.Fatal("cancellation lost")
	}
	<-finished
	if result := settle(t, root, nil); !result.Released {
		t.Fatal("root not joined")
	}
}

func TestCursorBodyCloseIsJoined(t *testing.T) {
	_, options := newPeer(t)
	fixture := bindFixture(t, options, 2)
	cause := errors.New("late-close")
	body := &gatedClose{Reader: strings.NewReader("<ListBucketResult><Name>fixture</Name><IsTruncated>false</IsTruncated></ListBucketResult>"), entered: make(chan struct{}), gate: make(chan struct{}), cause: cause}
	t.Cleanup(func() {
		select {
		case <-body.gate:
		default:
			close(body.gate)
		}
	})
	fixture.client.owner.wire.base = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body, Request: request}, nil
	})
	cursor, root, err := fixture.client.Enumerate(deadline(t), deadline(t), correlation("root"), ListRequest{Prefix: "owned/"})
	if err != nil {
		t.Fatal(err)
	}
	next, err := cursor.Next(deadline(t), childID("next", "root"))
	if err != nil {
		t.Fatal(err)
	}
	<-body.entered
	waiting, cancel := context.WithTimeout(deadline(t), 10*time.Millisecond)
	defer cancel()
	if _, err := next.WaitReleased(waiting); err == nil {
		t.Fatal("blocked close released child")
	}
	if _, err := root.WaitReleased(waiting); err == nil {
		t.Fatal("blocked close released root")
	}
	close(body.gate)
	settle(t, next, nil)
	result := settle(t, root, nil)
	if !errors.Is(result.Outcome.Cleanup, cause) {
		t.Fatal("root cleanup cause lost")
	}
}

func TestCursorCumulativeRequestLimitIsNotEOF(t *testing.T) {
	server, options := newPeer(t)
	options.MaxEntries = 1
	options.MaxRequests = 1
	fixture := bindFixture(t, options, 3)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		_, _ = io.WriteString(writer, "<ListBucketResult><Name>fixture</Name><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken><Contents><Key>owned/key</Key><Size>1</Size></Contents></ListBucketResult>")
		return true
	}
	server.mu.Unlock()
	cursor, root, err := fixture.client.Enumerate(deadline(t), deadline(t), correlation("root"), ListRequest{Prefix: "owned/"})
	if err != nil {
		t.Fatal(err)
	}
	next, err := cursor.Next(deadline(t), childID("one", "root"))
	first := settle(t, next, err)
	if first.Err() != nil || first.Outcome.Value.Complete() {
		t.Fatal("first page")
	}
	next, err = cursor.Next(deadline(t), childID("two", "root"))
	second := settle(t, next, err)
	if !errors.Is(second.Err(), ErrLimit) || second.Outcome.Value.Complete() {
		t.Fatal("request exhaustion became EOF")
	}
	if result := settle(t, root, nil); !errors.Is(result.Err(), ErrLimit) || result.Attempts.Observed != 1 {
		t.Fatal("cursor attempts lost")
	}
}

func TestCursorBoundedNativePageDrainsAcrossResults(t *testing.T) {
	server, options := newPeer(t)
	options.MaxEntries = 1
	fixture := bindFixture(t, options, 2)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		_, _ = io.WriteString(writer, "<ListBucketResult><Name>fixture</Name><IsTruncated>false</IsTruncated><Contents><Key>owned/one</Key><Size>0</Size></Contents><Contents><Key>owned/two</Key><Size>0</Size></Contents></ListBucketResult>")
		return true
	}
	server.mu.Unlock()
	cursor, root, err := fixture.client.Enumerate(deadline(t), deadline(t), correlation("root"), ListRequest{Prefix: "owned/"})
	if err != nil {
		t.Fatal(err)
	}
	claimRoot(t, fixture.inbox)
	var names []string
	for index := range 3 {
		receipt, err := cursor.Next(deadline(t), childID(fmt.Sprint("next-", index), "root"))
		result := settle(t, receipt, err)
		if result.Err() != nil || result.Outcome.Value.Complete() != (index == 2) {
			t.Fatal("native page lost across output chunks", result.Err())
		}
		for _, object := range result.Outcome.Value.ObjectsCopy() {
			names = append(names, object.Address.Key)
		}
		record, err := fixture.inbox.Next(deadline(t))
		if err != nil {
			t.Fatal(err)
		}
		if err := record.Release(); err != nil {
			t.Fatal(err)
		}
	}
	result := settle(t, root, nil)
	if len(names) != 2 || names[0] != "owned/one" || names[1] != "owned/two" || result.Attempts.Observed != 1 {
		t.Fatal("bounded page was refetched or changed")
	}
}

func FuzzCursorContinuation(f *testing.F) {
	f.Add([]byte("<ListBucketResult><Name>fixture</Name><IsTruncated>true</IsTruncated><NextContinuationToken>opaque+%</NextContinuationToken></ListBucketResult>"), false)
	f.Add([]byte("<ListVersionsResult><Name>fixture</Name><IsTruncated>true</IsTruncated><NextKeyMarker>owned/key</NextKeyMarker><NextVersionIdMarker>v+%</NextVersionIdMarker></ListVersionsResult>"), true)
	f.Add([]byte("<ListBucketResult/>"), false)
	f.Fuzz(func(t *testing.T, body []byte, versions bool) {
		if len(body) > 64<<10 {
			t.Skip()
		}
		state := &objectListing{bucket: "fixture", request: ListRequest{Prefix: "owned/", Versions: versions}, seen: map[string]bool{}}
		response := &controlResponse{body: *bytes.NewBuffer(body), statusCode: 200, readErr: io.EOF}
		err := state.checkPage(response)
		if err == nil {
			var page struct{ IsTruncated bool }
			if xml.Unmarshal(body, &page) != nil {
				t.Fatal("invalid XML accepted")
			}
			if page.IsTruncated && !errors.Is(state.checkPage(response), ErrProtocol) {
				t.Fatal("repeated continuation accepted")
			}
		}
	})
}
