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

package http3

import (
	"context"
	"errors"
	http "github.com/sardanioss/http"
	"github.com/sardanioss/quic-go"
	"io"
	"strings"
	"testing"
	"time"
)

func TestFathomryErrorConversionKeepsAllCauses(t *testing.T) {
	sentinel := errors.New("test cleanup")
	native := &quic.StreamError{StreamID: 4, ErrorCode: 7, Remote: true}
	result := maybeReplaceError(errors.Join(native, sentinel))
	var converted *Error
	if !errors.Is(result, sentinel) || !errors.Is(result, native) || !errors.As(result, &converted) {
		t.Fatal("native error translation lost causes")
	}
}
func TestFathomrySenderCloseJoinsAndRetainsError(t *testing.T) {
	client := &ClientConn{}
	sender, err := client.startSender(io.NopCloser(strings.NewReader("input")))
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("test sender")
	closed := make(chan error, 1)
	body := &senderResponse{ReadCloser: io.NopCloser(strings.NewReader("response")), sender: sender}
	go func() { closed <- body.Close() }()
	select {
	case <-closed:
		t.Fatal("body close preceded sender completion")
	case <-time.After(10 * time.Millisecond):
	}
	sender.finish(cause)
	select {
	case err := <-closed:
		if !errors.Is(err, cause) {
			t.Fatal("sender error lost")
		}
	case <-time.After(time.Second):
		t.Fatal("sender close did not join")
	}
}

func TestFathomrySenderCloseAbortsBlockedWrite(t *testing.T) {
	client := &ClientConn{}
	sender, err := client.startSender(io.NopCloser(strings.NewReader("input")))
	if err != nil {
		t.Fatal(err)
	}
	aborted := make(chan struct{})
	sender.abortWrite = func() { close(aborted) }
	go func() { <-aborted; sender.finish(nil) }()
	body := &senderResponse{ReadCloser: io.NopCloser(strings.NewReader("response")), sender: sender}
	done := make(chan error, 1)
	go func() { done <- body.Close() }()
	select {
	case <-done:
	case <-time.After(time.Second):
		close(aborted)
		<-done
		t.Fatal("body Close never canceled the blocked write")
	}
}

type retiringClient struct{ closed bool }

func (*retiringClient) OpenRequestStream(context.Context) (*RequestStream, error) { return nil, nil }
func (*retiringClient) RoundTrip(*http.Request) (*http.Response, error)           { return nil, nil }
func (*retiringClient) handleUnidirectionalStream(*quic.ReceiveStream)            {}
func (client *retiringClient) FathomryCloseSenders() error                        { client.closed = true; return nil }
func TestFathomryRemovedClientsRetainCleanup(t *testing.T) {
	client := &retiringClient{}
	ready := make(chan struct{})
	close(ready)
	original := &roundTripperWithCount{cancel: func() {}, dialing: ready, clientConn: client}
	replacement := &roundTripperWithCount{cancel: func() {}, dialing: ready}
	transport := &Transport{clients: map[string]*roundTripperWithCount{"origin": replacement}}
	if err := transport.removeClient("origin", original); err != nil {
		t.Fatal(err)
	}
	if !client.closed || transport.clients["origin"] != replacement {
		t.Fatal("old client cleanup or replacement identity was lost")
	}
}

func TestFathomryRetryFactoryPreservesInitiatingFailure(t *testing.T) {
	rejected := &Error{ErrorCode: ErrCodeRequestRejected}
	cause := errors.New("test replay factory")
	request, _ := http.NewRequest("POST", "https://example.invalid", strings.NewReader("input"))
	request.GetBody = func() (io.ReadCloser, error) { return nil, cause }
	_, err := canRetryRequest(rejected, request)
	if !errors.Is(err, rejected) || !errors.Is(err, cause) {
		t.Fatal("replay factory discarded initiating failure or factory cause")
	}
	cleanup := errors.New("test replay cleanup")
	body := &failedReplayReader{Reader: strings.NewReader("replay"), cause: cleanup}
	request.GetBody = func() (io.ReadCloser, error) { return body, cause }
	_, err = canRetryRequest(rejected, request)
	if !errors.Is(err, rejected) || !errors.Is(err, cause) || !errors.Is(err, cleanup) || body.closed != 1 {
		t.Fatal("failed replay factory abandoned its reader or error evidence")
	}
	request.GetBody = func() (io.ReadCloser, error) { return nil, nil }
	if _, err := canRetryRequest(rejected, request); !errors.Is(err, rejected) {
		t.Fatal("nil replay reader lost the initiating failure")
	}
}

type failedReplayReader struct {
	io.Reader
	cause  error
	closed int
}

func (reader *failedReplayReader) Close() error { reader.closed++; return reader.cause }
