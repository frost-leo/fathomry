/*
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

package chromedp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chromedp/cdproto/network"
	sdk "github.com/chromedp/chromedp"
)

func TestLiveCanceledResourceInOwnedPageOfBorrowedContextCanClose(t *testing.T) {
	release, started := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/slow" {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte("<title>owned child</title>"))
			return
		}
		w.Header().Set("Content-Length", "1000")
		_, _ = w.Write([]byte("pending"))
		w.(http.Flusher).Flush()
		close(started)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)
	endpoint, peer := cleanupFaultPeer(t, nil)
	prepared, err := Prepare(Settings{Name: "resource-borrow", RemoteURL: endpoint, MaxSessions: new(1)}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{Borrow: true, DefaultContext: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = retained.native.ClosePage(bounded(t), retained.Page().native)
		_ = owner.Close(bounded(t))
	})
	if _, err := retained.Page().Navigate(bounded(t), server.URL); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	frames, _, err := retained.Page().Frames(bounded(t))
	if err != nil || len(frames) != 1 {
		t.Fatal("resource frame missing", err)
	}
	drain(t, dependencies, 1)
	work, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, stream, receipt, err := retained.Page().LoadResource(work, frames[0], network.LoadNetworkResourceParams{URL: server.URL + "/slow", Options: &network.LoadNetworkResourceOptions{}})
	if stream == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("canceled load lost unknown stream custody", err)
	}
	select {
	case <-started:
	default:
		t.Fatal("resource did not start before cancellation")
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() || !stream.Closed() {
		t.Fatal("borrowed context prevented destruction of its owned child page")
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil || !errors.Is(snapshot.Primary(), context.DeadlineExceeded) {
		t.Fatal("resource cleanup erased the original cancellation", err)
	}
	if value, err := sdk.Run(peer, sdk.Evaluate[int]("17")); err != nil || value != 17 {
		t.Fatal("owned-child cleanup changed the borrowed peer", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveResourceWithoutSuccessfulHandleKeepsUnknownCustody(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/resource" {
			requests.Add(1)
			_, _ = w.Write([]byte("retained native bytes"))
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<title>unknown resource</title>"))
	}))
	defer server.Close()
	endpoint, _ := cleanupFaultPeerReplies(t, nil, nil, func(data []byte) []byte {
		var outer, result, resource map[string]json.RawMessage
		if json.Unmarshal(data, &outer) != nil || json.Unmarshal(outer["result"], &result) != nil || json.Unmarshal(result["resource"], &resource) != nil || len(resource["stream"]) == 0 {
			return data
		}
		delete(resource, "stream")
		result["resource"], _ = json.Marshal(resource)
		outer["result"], _ = json.Marshal(result)
		changed, err := json.Marshal(outer)
		if err != nil {
			t.Error("fixture reply transform", err)
			return data
		}
		return changed
	})
	prepared, err := Prepare(Settings{Name: "resource-unknown", RemoteURL: endpoint, MaxStreams: new(1)}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := mechanisms(t, prepared)
	owner, err := prepared.Open(context.Background(), dependencies)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retained.Page().Navigate(bounded(t), server.URL); err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	frames, _, err := retained.Page().Frames(bounded(t))
	if err != nil || len(frames) != 1 {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	params := network.LoadNetworkResourceParams{URL: server.URL + "/resource", Options: &network.LoadNetworkResourceOptions{DisableCache: true}}
	result, stream, receipt, err := retained.Page().LoadResource(bounded(t), frames[0], params)
	if err == nil || stream == nil || result.Resource == nil || !result.Resource.Success || result.Resource.Stream != "" {
		t.Fatal("successful resource without its handle falsely settled", err)
	}
	if err := stream.Close(bounded(t)); err == nil || stream.Closed() {
		t.Fatal("unknown stream claimed native closure", err)
	}
	if _, next, _, err := retained.Page().LoadResource(bounded(t), frames[0], params); !errors.Is(err, ErrLimit) || next != nil || requests.Load() != 1 {
		t.Fatal("unknown producer refunded its stream allowance", err)
	}
	drain(t, dependencies, 1)
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() || !stream.Closed() {
		t.Fatal("owned context destruction did not settle unknown resource")
	}
	if snapshot, err := receipt.WaitReleased(bounded(t)); err != nil || snapshot.Primary() == nil {
		t.Fatal("unknown producer lost independent evidence", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveResourceControlBytesFitRequestedRead(t *testing.T) {
	body := make([]byte, 4096)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/resource" {
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(body)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<title>escaped resource</title>"))
	}))
	defer server.Close()
	owner, retained, dependencies := controlFixture(t, server.URL, 8)
	frames, _, err := retained.Page().Frames(bounded(t))
	if err != nil || len(frames) != 1 {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	_, stream, _, err := retained.Page().LoadResource(bounded(t), frames[0], network.LoadNetworkResourceParams{URL: server.URL + "/resource", Options: &network.LoadNetworkResourceOptions{}})
	if err != nil || stream == nil {
		t.Fatal(err)
	}
	var result []byte
	for range 5 {
		chunk, _, err := stream.Read(bounded(t), 1024)
		if err != nil {
			t.Fatal("native JSON escaping rejected a valid chunk", err)
		}
		result = append(result, chunk.DataCopy()...)
		drain(t, dependencies, 1)
		if chunk.EOF() {
			break
		}
	}
	if !bytes.Equal(result, body) {
		t.Fatal("escaped native bytes differ")
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() || !stream.Closed() {
		t.Fatal("resource stream survived owned cleanup")
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveResourceStreamPreservesStatusBytesOffsetsAndEvidence(t *testing.T) {
	body := make([]byte, 4000)
	for index := range body {
		body[index] = byte(index % 251)
	}
	var requests atomic.Int64
	var sameOriginCookie atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/resource" {
			w.Header().Set("Content-Type", "text/html")
			http.SetCookie(w, &http.Cookie{Name: "resource-fixture", Value: "synthetic", Path: "/"})
			_, _ = w.Write([]byte("<h1>resource</h1>"))
			return
		}
		requests.Add(1)
		cookie, err := r.Cookie("resource-fixture")
		sameOriginCookie.Store(err == nil && cookie.Value == "synthetic")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Fixture", "original")
		_, _ = w.Write(body)
	}))
	defer server.Close()
	owner, retained, dependencies := controlFixture(t, server.URL, 3)
	frames, _, err := retained.Page().Frames(bounded(t))
	if err != nil || len(frames) != 1 {
		t.Fatal("frame grant missing", err)
	}
	drain(t, dependencies, 1)
	params := network.LoadNetworkResourceParams{URL: server.URL + "/resource", Options: &network.LoadNetworkResourceOptions{DisableCache: true}}
	result, stream, receipt, err := retained.Page().LoadResource(bounded(t), frames[0], params)
	if err != nil || stream == nil || result.Resource == nil || !result.Resource.Success || result.Resource.HTTPStatusCode != http.StatusOK || result.Resource.Stream == "" {
		t.Fatal("native status or resource stream missing", err)
	}
	if params.FrameID != "" || !params.Options.DisableCache || params.Options.IncludeCredentials {
		t.Fatal("resource preparation changed caller parameters")
	}
	result.Resource.Stream = "caller-change"
	result.Resource.Headers["X-Fixture"] = "caller-change"
	var data []byte
	complete := false
	for range 32 {
		chunk, _, err := stream.Read(bounded(t), 257)
		if err != nil {
			t.Fatal(err)
		}
		data = append(data, chunk.DataCopy()...)
		drain(t, dependencies, 1)
		if chunk.EOF() {
			complete = true
			break
		}
	}
	if !complete || !bytes.Equal(data, body) {
		t.Fatal("resource bytes or terminal EOF differ")
	}
	chunk, _, err := stream.ReadAt(bounded(t), 4, 0)
	if err != nil || chunk.EOF() || !bytes.Equal(chunk.DataCopy(), body[:4]) {
		t.Fatal("explicit zero offset did not reopen the native position after EOF", err)
	}
	drain(t, dependencies, 1)
	chunk, _, err = stream.Read(bounded(t), 4)
	if err != nil || !bytes.Equal(chunk.DataCopy(), body[4:8]) {
		t.Fatal("sequential read did not follow the explicit seek", err)
	}
	drain(t, dependencies, 1)
	if err := stream.Close(bounded(t)); err != nil || !stream.Closed() {
		t.Fatal("resource stream cleanup failed", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	evidence, ok := snapshot.ValueCopy()
	var original network.LoadNetworkResourceResult
	if !ok || json.Unmarshal(evidence.PayloadCopy(), &original) != nil || original.Resource == nil || original.Resource.Stream == "caller-change" || original.Resource.Headers["X-Fixture"] == "caller-change" || !evidence.Observation().StreamClosed {
		t.Fatal("independent resource metadata followed mutable caller data")
	}
	if requests.Load() != 1 || !sameOriginCookie.Load() {
		t.Fatal("reads, cleanup or evidence repeated the resource request")
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("resource custody prevented source release", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestLiveResourceStreamRejectsForeignFramesAndClosesIgnoredResults(t *testing.T) {
	server := responseBodyServer(t, []byte("resource"))
	defer server.Close()
	owner, retained, dependencies := controlFixture(t, server.URL, 32)
	frames, _, err := retained.Page().Frames(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	other, _, err := retained.NewPage(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	params := network.LoadNetworkResourceParams{URL: server.URL, Options: &network.LoadNetworkResourceOptions{}}
	if _, stream, _, err := other.LoadResource(bounded(t), frames[0], params); stream != nil || !errors.Is(err, ErrInput) {
		t.Fatal("another page accepted the frame grant", err)
	}
	drain(t, dependencies, 1)
	if _, stream, _, err := retained.Page().LoadResource(bounded(t), &Frame{}, params); stream != nil || !errors.Is(err, ErrInput) {
		t.Fatal("zero frame selected an implicit default", err)
	}
	drain(t, dependencies, 1)
	params.FrameID = "foreign"
	if _, stream, _, err := retained.Page().LoadResource(bounded(t), frames[0], params); stream != nil || !errors.Is(err, ErrInput) {
		t.Fatal("manual frame substitution was accepted", err)
	}
	drain(t, dependencies, 1)
	params.FrameID = ""
	_, stream, receipt, err := retained.Page().LoadResource(bounded(t), frames[0], params)
	if err != nil || stream == nil {
		t.Fatal(err)
	}
	_ = owner.Close(bounded(t))
	if !owner.ShutdownComplete() || !stream.Closed() {
		t.Fatal("ignored resource stream remained live")
	}
	if _, err := receipt.WaitReleased(bounded(t)); err != nil {
		t.Fatal(err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}

func TestResourceZeroAndSequentialStreamBoundaries(t *testing.T) {
	var page *Page
	if _, stream, receipt, err := page.LoadResource(context.Background(), nil, network.LoadNetworkResourceParams{}); stream != nil || receipt != nil || !errors.Is(err, ErrInput) {
		t.Fatal("nil page acquired a resource", err)
	}
	var stream *Stream
	if _, receipt, err := stream.ReadAt(context.Background(), 1, 0); receipt != nil || !errors.Is(err, ErrInput) {
		t.Fatal("nil stream acquired seek authority", err)
	}
}

func TestLiveResourceNetworkFailureHasMetadataWithoutStream(t *testing.T) {
	server := responseBodyServer(t, []byte("HTTP error body"))
	owner, retained, dependencies := controlFixture(t, server.URL, 3)
	frames, _, err := retained.Page().Frames(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	drain(t, dependencies, 1)
	result, stream, receipt, err := retained.Page().LoadResource(bounded(t), frames[0], network.LoadNetworkResourceParams{URL: server.URL + "/body", Options: &network.LoadNetworkResourceOptions{}})
	if err != nil || stream != nil || result.Resource == nil || result.Resource.Success || result.Resource.HTTPStatusCode != http.StatusTeapot || result.Resource.NetErrorName == "" {
		t.Fatal("native network failure became a Go success or imaginary stream", err)
	}
	snapshot, err := receipt.WaitReleased(bounded(t))
	if err != nil {
		t.Fatal(err)
	}
	evidence, ok := snapshot.ValueCopy()
	var saved network.LoadNetworkResourceResult
	if !ok || json.Unmarshal(evidence.PayloadCopy(), &saved) != nil || saved.Resource == nil || saved.Resource.HTTPStatusCode != http.StatusTeapot || saved.Resource.Success {
		t.Fatal("finite failure metadata was not independently retained")
	}
	if err := owner.Close(bounded(t)); err != nil || !owner.ShutdownComplete() {
		t.Fatal("known absent stream retained cleanup custody", err)
	}
	status, _ := dependencies.Evidence.Inspect()
	drain(t, dependencies, status.Outstanding)
}
