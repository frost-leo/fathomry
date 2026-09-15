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

package nethttp

import (
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestDirectConnectionLifetimeAndNestedEvidence(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		server, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, "direct") })
		options.MaxConnections, options.MaxActive = 1, 1
		f := bindFixture(t, options, 4)
		connection, parent, err := f.client.Connect(deadline(t), correlation("connection"), "https", server.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close(deadline(t))
		f.client.owner.transport.CloseIdleConnections()
		if err := f.assembly.Close(deadline(t)); !errors.Is(err, resource.ErrIncomplete) {
			t.Fatal("direct connection did not retain its source", err)
		}
		for _, id := range []string{"one", "two"} {
			receipt, err := connection.Do(deadline(t), deadline(t), fault.Correlation{Call: id, Parent: "connection"}, newRequest(t, "GET", server.URL, nil))
			if err != nil {
				t.Fatal(err)
			}
			result, err := receipt.WaitReleased(deadline(t))
			if err != nil || result.Err() != nil || !result.Nested || string(result.Outcome.Value.DataCopy()) != "direct" {
				t.Fatal("direct child failed", err, result.Err())
			}
		}
		if err := connection.Close(deadline(t)); err != nil {
			t.Fatal(err)
		}
		result, err := parent.WaitReleased(deadline(t))
		if err != nil || !result.Outcome.Value.Connected() || !result.Outcome.Value.Complete() {
			t.Fatal("direct lifetime incomplete", err)
		}
		for range 3 {
			delivery, err := f.inbox.Next(deadline(t))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := delivery.Receipt().WaitReleased(deadline(t)); err != nil {
				t.Fatal(err)
			}
			if err := delivery.Release(); err != nil {
				t.Fatal(err)
			}
		}
		if f.inbox.Usage() != (invocation.InboxUsage{}) {
			t.Fatal("direct evidence retained")
		}
	})
}

func TestDirectCloseStopsAnOutstandingResponse(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		release := make(chan struct{})
		defer close(release)
		server, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			writer.Header().Set("Content-Length", "2")
			_, _ = io.WriteString(writer, "a")
			writer.(http.Flusher).Flush()
			<-release
		})
		f := bindFixture(t, options, 3)
		connection, parent, err := f.client.Connect(deadline(t), correlation("parent"), "https", server.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close(deadline(t))
		stream, child, err := connection.Open(deadline(t), fault.Correlation{Call: "child", Parent: "parent"}, newRequest(t, "GET", server.URL, nil))
		if err != nil {
			t.Fatal(err)
		}
		defer stream.Close(deadline(t))
		rejected, receipt, err := connection.Open(deadline(t), fault.Correlation{Call: "other", Parent: "parent"}, newRequest(t, "GET", server.URL, nil))
		if rejected != nil || receipt != nil || !errors.Is(err, ErrState) {
			t.Fatal("overlapping child accepted")
		}
		if err := connection.Close(deadline(t)); err != nil {
			t.Fatal(err)
		}
		for _, receipt := range []*invocation.Receipt[Result]{parent, child} {
			if _, err := receipt.WaitReleased(deadline(t)); err != nil {
				t.Fatal(err)
			}
		}
		for range 2 {
			delivery, err := f.inbox.Next(deadline(t))
			if err != nil {
				t.Fatal(err)
			}
			if err := delivery.Release(); err != nil {
				t.Fatal(err)
			}
		}
	})
}
