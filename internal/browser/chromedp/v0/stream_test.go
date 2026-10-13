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
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/chromedp/cdproto"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	sdk "github.com/chromedp/chromedp"
	"github.com/frost-leo/fathomry/internal/fault"
)

func TestBrowserResourceExactLimitAllowsOnlyPositiveEOF(t *testing.T) {
	for _, extra := range []bool{false, true} {
		t.Run(map[bool]string{false: "exact", true: "excess"}[extra], func(t *testing.T) {
			body := "abcd"
			if extra {
				body += "excess"
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/resource" {
					_, _ = w.Write([]byte(body))
					return
				}
				w.Header().Set("Content-Type", "text/html")
				_, _ = w.Write([]byte("<title>stream boundary</title>"))
			}))
			defer server.Close()
			options := browserOptions(t)
			options.MaxStreamBytes = 4
			fixture := bindFixture(t, options, 1)
			retained, err := fixture.client.Retain(deadline(t), fault.Correlation{Call: "stream-boundary"})
			if err != nil {
				t.Fatal(err)
			}
			defer retained.Close(deadline(t))
			if err := retained.Page().Navigate(deadline(t), server.URL); err != nil {
				t.Fatal(err)
			}
			frames, err := retained.Page().Frames(deadline(t))
			if err != nil || len(frames) != 1 {
				t.Fatal(err)
			}
			_, stream, err := retained.Page().LoadResource(deadline(t), frames[0], network.LoadNetworkResourceParams{URL: server.URL + "/resource", Options: &network.LoadNetworkResourceOptions{}})
			if err != nil || stream == nil {
				t.Fatal(err)
			}
			first, err := stream.Read(deadline(t), 4)
			if err != nil || string(first.DataCopy()) != "abcd" {
				t.Fatal("allowed native bytes missing", err)
			}
			last, err := stream.Read(deadline(t), 1)
			if !extra && (err != nil || !last.EOF() || len(last.DataCopy()) != 0) {
				t.Fatal("exact limit hid confirmed EOF", err)
			}
			if extra {
				if !errors.Is(err, ErrLimit) || last.EOF() || len(last.DataCopy()) != 0 {
					t.Fatal("EOF probe delivered excess bytes or false EOF", err)
				}
				if _, err := stream.Read(deadline(t), 1); !errors.Is(err, ErrState) {
					t.Fatal("exhausted stream kept draining unseen bytes", err)
				}
			}
			if stream.Bytes() != 4 {
				t.Fatal("probe changed delivered-byte accounting")
			}
			if err := stream.Close(deadline(t)); err != nil || !stream.Closed() {
				t.Fatal("exhaustion prevented cleanup", err)
			}
		})
	}
}

func TestSequentialStreamRejectsOffsetsBeforeNativeWork(t *testing.T) {
	stream := &Stream{state: &streamState{sequential: true}}
	if _, err := stream.ReadAt(context.Background(), 1, 0); !errors.Is(err, ErrUnsupported) {
		t.Fatal("sequential producer accepted explicit offset", err)
	}
}

func TestBrowserIncrementalPDFAndCleanupAfterCancellation(t *testing.T) {
	f := bindFixture(t, browserOptions(t), 1)
	retained, err := f.client.Retain(deadline(t), fault.Correlation{Call: "pdf-stream"})
	if err != nil {
		t.Fatal(err)
	}
	if err := retained.Page().Navigate(deadline(t), "data:text/html,<h1>managed stream</h1>"); err != nil {
		t.Fatal(err)
	}
	margin := 0.0
	params := page.PrintToPDFParams{MarginTop: &margin}
	stream, err := retained.Page().PDFStream(deadline(t), params)
	if err != nil {
		t.Fatal(err)
	}
	if params.TransferMode != "" || *params.MarginTop != 0 {
		t.Fatal("caller PDF parameters mutated")
	}
	var result []byte
	complete := false
	for range 256 {
		chunk, err := stream.Read(deadline(t), 1024)
		if err != nil {
			t.Fatal(err)
		}
		data := chunk.DataCopy()
		if len(data) > 1024 {
			t.Fatal("chunk bound")
		}
		result = append(result, data...)
		if chunk.EOF() {
			complete = true
			break
		}
	}
	if !complete || !bytes.HasPrefix(result, []byte("%PDF-")) {
		t.Fatal("false stream completion")
	}
	copy := *stream
	retained.Page().cancel(context.Canceled)
	if _, err := copy.Read(context.Background(), 1); !errors.Is(err, ErrState) {
		t.Fatal("canceled stream revived", err)
	}
	if err := copy.Close(deadline(t)); err != nil || !stream.Closed() {
		t.Fatal("target cleanup stopped with work", err)
	}
	if err := stream.Close(deadline(t)); err != nil {
		t.Fatal("repeated close", err)
	}
	if err := retained.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	settle(t, f, fault.Correlation{Call: "pdf-stream"}, retained.Receipt(), nil, nil, func(testing.TB, Result) {})
}

func TestBorrowedDetachFactSurvivesAutomaticStreamRetry(t *testing.T) {
	host := bindFixture(t, browserOptions(t), 1)
	original, err := host.client.Retain(deadline(t), fault.Correlation{Call: "peer-stream"})
	if err != nil {
		t.Fatal(err)
	}
	address, err := host.client.owner.debugURL(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	borrowed := bindFixture(t, OptionsV1{Name: "borrowed-stream", RemoteURL: address}, 1)
	id := fault.Correlation{Call: "stream-retry"}
	retained, err := borrowed.client.Retain(deadline(t), id, SessionOptionsV1{Borrow: true, ContextID: original.Page().browserID, TargetID: sdk.FromContext(original.Page().native).Target.TargetID})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := retained.Page().PDFStream(deadline(t), page.PrintToPDFParams{})
	if err != nil {
		t.Fatal(err)
	}
	handle := stream.state.id
	stream.state.id = "synthetic-missing-handle"
	first := stream.Close(deadline(t))
	stream.state.id = handle
	var cause *cdproto.Error
	if !errors.As(first, &cause) || stream.Closed() {
		t.Fatal("native close failure not reproduced", first)
	}
	if err := retained.Close(deadline(t)); !errors.Is(err, cause) || !stream.Closed() {
		t.Fatal("automatic retry lost cleanup history", err)
	}
	settle(t, borrowed, id, retained.Receipt(), nil, cause, func(t testing.TB, value Result) {
		if !value.ContextDetached() || value.ContextReleased() {
			t.Fatal("cleanup history changed a confirmed detach fact")
		}
	})
	if err := borrowed.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if value, err := RunAction(deadline(t), original.Page(), Evaluate[int]("42")); err != nil || value != 42 {
		t.Fatal("borrowed peer no longer usable", err)
	}
	if err := original.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	settle(t, host, fault.Correlation{Call: "peer-stream"}, original.Receipt(), nil, nil, func(testing.TB, Result) {})
}
