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

package nuki

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
)

type finishingReader struct {
	entered chan struct{}
	release chan struct{}
	cause   error
}

func (reader *finishingReader) Read([]byte) (int, error) {
	close(reader.entered)
	<-reader.release
	return 0, reader.cause
}
func (reader *finishingReader) Close() error { close(reader.release); return nil }

func TestProviderAlreadyEnteredReadKeepsLateFailure(t *testing.T) {
	cause := errors.New("late-read-failure")
	reader := &finishingReader{entered: make(chan struct{}), release: make(chan struct{}), cause: cause}
	op := &operation{ctx: context.Background(), data: &resultData{}}
	body := op.ownBody(reader, &byteBudget{limit: 16}, -1, false)
	done := make(chan error, 1)
	go func() { _, err := body.Read(make([]byte, 1)); done <- err }()
	<-reader.entered
	op.mu.Lock()
	op.finishing = true
	op.mu.Unlock()
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, cause) {
		t.Fatal("reader lost its cause", err)
	}
	body.reading.Wait()
	if !errors.Is(op.currentError(), cause) {
		t.Fatal("joining an entered read discarded its failure")
	}
}

func TestProviderValidEncodedEmptyIsNotMissingEntity(t *testing.T) {
	encoded := encodeBody(t, "gzip", nil)
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Encoding", "gzip")
		writer.Header().Set("Content-Length", strconv.Itoa(len(encoded)))
		_, _ = writer.Write(encoded)
	}))
	defer peer.Close()
	fixture := bindProvider(t, providerOptions())
	receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "encoded-empty"}, nativeRequest(t, "GET", peer.URL, nil))
	if err != nil {
		t.Fatal(err)
	}
	result := outcome(t, fixture, receipt)
	value := result.Outcome.Value
	if result.Err() != nil || !value.Complete() || len(value.DataCopy()) != 0 || value.EncodedBytesRead() != int64(len(encoded)) {
		t.Fatal("valid empty representation was rejected", result.Err())
	}
}

func TestProviderEmptyCanonicalHostIsRejected(t *testing.T) {
	for _, host := range []string{"", ".", "。", "．", "｡"} {
		if _, err := canonicalHost(host); !errors.Is(err, ErrInput) {
			t.Fatalf("empty canonical host %q accepted: %v", host, err)
		}
	}
}
