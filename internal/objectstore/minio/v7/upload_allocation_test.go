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
	"fmt"
	"io"
	"slices"
	"testing"
	"time"
)

type stagingInput struct {
	*bytes.Reader
	eofWithData bool
	capacity    int
	consumed    int
	closed      bool
}

func (input *stagingInput) Read(buffer []byte) (int, error) {
	input.capacity = max(input.capacity, cap(buffer))
	count, err := input.Reader.Read(buffer)
	input.consumed += count
	if input.eofWithData && input.Len() == 0 {
		err = io.EOF
	}
	return count, err
}

func (input *stagingInput) Close() error {
	input.closed = true
	return nil
}

func TestUploadStagingBounds(t *testing.T) {
	const part = 5 << 20
	for _, test := range []struct {
		name        string
		actual      int
		size, limit int64
		capacity    int
	}{
		{"known-empty", 0, 0, 8 << 20, 1},
		{"known-small", 64 << 10, 64 << 10, 8 << 20, 64<<10 + 1},
		{"unknown-empty", 0, -1, 1, 2},
		{"unknown-below-bound", 63 << 10, -1, 64 << 10, 64<<10 + 1},
		{"unknown-at-bound", 64 << 10, -1, 64 << 10, 64<<10 + 1},
		{"below-part", part - 1, part - 1, 8 << 20, part},
		{"at-part", part, part, 8 << 20, part + 1},
		{"above-part", part + 1, part + 1, 8 << 20, part + 1},
		{"multipart", 6 << 20, 6 << 20, 8 << 20, part + 1},
	} {
		for _, eofWithData := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/eof-with-data=%t", test.name, eofWithData), func(t *testing.T) {
				server, options := newPeer(t)
				options.MaxTransferBytes, options.MaxReadBytes = test.limit, 1
				fixture := bindFixture(t, options, 1)
				before := server.count()
				body := bytes.Repeat([]byte("s"), test.actual)
				input := &stagingInput{Reader: bytes.NewReader(body), eofWithData: eofWithData}
				key := "owned/staging"
				receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("staging"), WriteRequest{Key: key, Size: test.size, IfAbsent: true}, input)
				result := settle(t, receipt, err)
				transfer := result.Outcome.Value.Transfer()
				digest := sha256.Sum256(body)
				if result.Err() != nil || transfer.Effect != Acknowledged || !transfer.Complete || transfer.Bytes != int64(len(body)) || transfer.SHA256 != hex.EncodeToString(digest[:]) {
					t.Fatal("upload content/effect accounting", result.Err())
				}
				parts := 0
				wantTrace := []string{"PUT"}
				// Exact-part EOF on a later read already takes the multipart path.
				if test.actual > part || test.actual == part && !eofWithData {
					parts = (test.actual + part - 1) / part
					wantTrace = []string{"POST"}
					for range parts {
						wantTrace = append(wantTrace, "PUT")
					}
					wantTrace = append(wantTrace, "POST")
				}
				server.mu.Lock()
				defer server.mu.Unlock()
				object, present := server.objects[key]
				var trace []string
				for _, request := range server.requests[before:] {
					trace = append(trace, request.method)
				}
				if !present || !bytes.Equal(object.body, body) || len(server.uploads) != 0 || !slices.Equal(trace, wantTrace) {
					t.Fatalf("independent object/cleanup/request evidence: present=%t trace=%v want=%v", present, trace, wantTrace)
				}
				if transfer.PartsAcknowledged != parts || transfer.CompletionAttempted != (parts > 0) || transfer.AbortAttempted || input.closed || input.consumed != len(body) {
					t.Fatal("multipart or borrowed input lifecycle changed")
				}
				if input.capacity > test.capacity {
					t.Errorf("unnecessary staging capacity: got %d, bound %d", input.capacity, test.capacity)
				}
			})
		}
	}
}

func TestUploadBoundedInputRejection(t *testing.T) {
	readerFailure := errors.New("staging reader failure")
	for _, test := range []struct {
		name             string
		actual, consumed int
		size             int64
		terminal, want   error
	}{
		{"known-short", 0, 0, 1, io.EOF, io.ErrUnexpectedEOF},
		{"known-empty-trailing", 2, 1, 0, io.EOF, ErrInput},
		{"unknown-over-limit", 64<<10 + 2, 64<<10 + 1, -1, io.EOF, ErrLimit},
		{"reader-error", 64 << 10, 64 << 10, 64 << 10, readerFailure, readerFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, options := newPeer(t)
			options.MaxTransferBytes, options.MaxReadBytes = 64<<10, 1
			fixture := bindFixture(t, options, 1)
			before := server.count()
			input := &stagingInput{Reader: bytes.NewReader(bytes.Repeat([]byte("x"), test.actual))}
			receipt, err := fixture.client.Put(deadline(t), deadline(t), correlation("reject"), WriteRequest{Key: "owned/rejected", Size: test.size}, failingReader{input, test.terminal})
			result := settle(t, receipt, err)
			transfer := result.Outcome.Value.Transfer()
			if !errors.Is(result.Outcome.Primary, test.want) || result.Outcome.Cleanup != nil || transfer.Effect != NotSubmitted || transfer.Complete || transfer.AbortAttempted {
				t.Fatal("small input rejection or effect changed", result.Err())
			}
			if server.count() != before || transfer.Bytes != int64(test.consumed) || input.consumed != test.consumed || input.closed {
				t.Fatal("rejection submitted a request or changed input borrowing/consumption")
			}
		})
	}
}

// BenchmarkUploadStaging measures actual allocated bytes for the whole controlled
// Put, including the local HTTP peer and required evidence delivery. Capacity-only
// assertions cannot detect a large allocation hidden behind a full slice expression.
func BenchmarkUploadStaging(b *testing.B) {
	for _, test := range []struct {
		name        string
		actual      int
		size, limit int64
	}{
		{"known-empty", 0, 0, 8 << 20},
		{"known-small", 64 << 10, 64 << 10, 8 << 20},
		{"unknown-small-bound", 64 << 10, -1, 64 << 10},
		{"multipart", 6 << 20, 6 << 20, 8 << 20},
	} {
		b.Run(test.name, func(b *testing.B) {
			server, options := newPeer(b)
			server.unversioned = true
			options.MaxTransferBytes, options.MaxReadBytes = test.limit, 1
			fixture := bindFixture(b, options, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			body := bytes.Repeat([]byte("b"), test.actual)
			input := bytes.NewReader(body)
			request := WriteRequest{Key: "owned/benchmark", Size: test.size}
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			for b.Loop() {
				input.Reset(body)
				receipt, err := fixture.client.Put(ctx, ctx, correlation("benchmark"), request, input)
				if err != nil {
					b.Fatal(err)
				}
				result, err := receipt.WaitReleased(ctx)
				if err != nil || result.Err() != nil || result.Outcome.Value.Transfer().Effect != Acknowledged {
					b.Fatal("upload failed", err, result.Err())
				}
				delivery, err := fixture.inbox.Next(ctx)
				if err != nil {
					b.Fatal("independent evidence delivery", err)
				}
				evidence, waitErr := delivery.Receipt().WaitReleased(ctx)
				if err := delivery.Release(); err != nil {
					b.Fatal(err)
				}
				if waitErr != nil || evidence.Err() != nil || evidence.Outcome.Value.Transfer() != result.Outcome.Value.Transfer() {
					b.Fatal("independent evidence result", waitErr, evidence.Err())
				}
				server.mu.Lock()
				object, present := server.objects[request.Key]
				valid := present && bytes.Equal(object.body, body) && len(server.uploads) == 0
				clear(server.requests)
				server.requests = server.requests[:0]
				server.mu.Unlock()
				if !valid {
					b.Fatal("independent stored content or multipart cleanup")
				}
			}
		})
	}
}
