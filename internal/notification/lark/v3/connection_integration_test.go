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

package lark

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestBorrowSharesAdmissionAndCloseWaitsForActualUse(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/auth/") {
			defaultReply(w, r)
			return
		}
		close(entered)
		<-release
		defaultReply(w, r)
	})
	options := testOptions(peer)
	options.MaxActive = 1
	bound := bindTest(t, options, 2)
	borrowed := resource.Borrow("borrowed", bound.assembly, bound.selected)
	child, err := resource.Assemble(context.Background(), context.Background(), "child", borrowed)
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close(context.Background())
	inbox, _ := invocation.NewInbox[Result](1, EvidenceBytesV1(options))
	alias, err := Bind(child, borrowed, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan invocation.Result[Result], 1)
	go func() {
		receipt, err := bound.client.Send(context.Background(), fault.Correlation{Call: "active"}, Recipient{Type: "open_id", ID: "ou_fixture"}, "active", textContent(t))
		done <- resolved(t, receipt, err)
	}()
	<-entered
	if _, err := alias.Send(context.Background(), fault.Correlation{Call: "excess"}, Recipient{Type: "open_id", ID: "ou_fixture"}, "excess", textContent(t)); err == nil {
		t.Fatal("borrow created quota")
	}
	if len(peer.snapshot()) != 2 {
		t.Fatal("saturated alias reached transport")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if err := bound.assembly.Close(ctx); err == nil {
		t.Fatal("source closed while still borrowed and active")
	}
	cancel()
	close(release)
	got := <-done
	if got.Err() != nil || got.Outcome.Value.Effect() != Accepted {
		t.Fatal("waiting cancellation rewrote actual effect")
	}
	if err := child.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = bound.assembly.Close(context.Background())
	if _, err := alias.Send(context.Background(), fault.Correlation{Call: "after-close"}, Recipient{Type: "open_id", ID: "ou_fixture"}, "after-close", textContent(t)); err == nil {
		t.Fatal("closed borrow entered native")
	}
}
func TestExecutionCancellationAndRedirectRefusal(t *testing.T) {
	for _, mode := range []string{"cancel", "redirect", "untrusted"} {
		t.Run(mode, func(t *testing.T) {
			entered := make(chan struct{})
			peer := newPeer(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.Contains(r.URL.Path, "/auth/") {
					defaultReply(w, r)
					return
				}
				if mode == "cancel" {
					close(entered)
					<-r.Context().Done()
					return
				}
				w.Header().Set("Location", "https://not-authorized.example.test")
				w.WriteHeader(302)
				_, _ = io.WriteString(w, `{"code":0}`)
			})
			options := testOptions(peer)
			if mode == "untrusted" {
				options.RootCAPEM = ""
			}
			bound := bindTest(t, options, 1)
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			cause := errors.New("caller cancellation")
			if mode == "cancel" {
				go func() { <-entered; cancel(cause) }()
			}
			receipt, err := bound.client.Send(ctx, fault.Correlation{Call: "bounded"}, Recipient{Type: "open_id", ID: "ou_fixture"}, "bounded", textContent(t))
			got := resolved(t, receipt, err)
			if got.Err() == nil {
				t.Fatal("unsafe request succeeded")
			}
			if mode == "cancel" && (!errors.Is(got.Err(), cause) || !errors.Is(got.Err(), context.Canceled) || got.Outcome.Value.Effect() != Unknown) {
				t.Fatal("cancellation lost cause or possible effect")
			}
			if mode == "redirect" && len(peer.snapshot()) != 2 {
				t.Fatal("redirect followed")
			}
			if mode == "untrusted" && len(peer.snapshot()) != 0 {
				t.Fatal("untrusted TLS sent credentials")
			}
		})
	}
}
