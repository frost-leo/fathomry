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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestProviderIntegrationAndIndependentEvidence(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		var peerMu sync.Mutex
		seen := make(map[string]string)
		server, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Error(err)
			}
			id := request.Header.Get("X-Call")
			peerMu.Lock()
			seen[id] = string(body)
			peerMu.Unlock()
			writer.Header().Set("X-Call", id)
			_, _ = writer.Write(body)
		})
		options.MaxActive, options.QueuedCalls = 4, 16
		f := bindFixture(t, options, 16)
		observer, _ := invocation.NewObserver(1)
		client, err := Bind(f.assembly, f.selected, f.inbox, observer)
		if err != nil {
			t.Fatal(err)
		}
		access, err := resource.AccessFor(f.assembly, f.selected)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var group sync.WaitGroup
		for index := range 16 {
			id := fmt.Sprintf("call-%02d", index)
			group.Go(func() {
				request, _ := http.NewRequest("POST", server.URL, strings.NewReader("payload-"+id))
				request.Header.Set("X-Call", id)
				receipt, err := client.Do(ctx, ctx, correlation(id), request)
				if err != nil || receipt == nil {
					t.Error("native integration call failed", err)
					return
				}
				got, err := receipt.WaitReleased(ctx)
				if err != nil || got.Err() != nil || string(got.Outcome.Value.DataCopy()) != "payload-"+id {
					t.Error("direct receipt data differs", err)
				}
			})
		}
		group.Wait()
		expected := make([]conformance.Expected[Result], 0, 16)
		for index := range 16 {
			id := fmt.Sprintf("call-%02d", index)
			expected = append(expected, conformance.Expected[Result]{
				Context: fault.Context{Provider: ProviderID, Source: options.Name, Scope: "test", Operation: "request", Correlation: correlation(id)},
				Source:  access.Info(), Limits: access.Limits(), Shape: invocation.Finite, Present: true, Final: true, Released: true,
				Attempts: invocation.Attempts{Observed: 1},
				Value: func(t testing.TB, result Result) {
					protocol := "HTTP/1.1"
					if h2 {
						protocol = "HTTP/2.0"
					}
					peerMu.Lock()
					observed, ok := seen[id]
					peerMu.Unlock()
					if !ok || observed != "payload-"+id || !result.Complete() || string(result.DataCopy()) != observed || result.Metadata().Protocol() != protocol || result.Metadata().HeadersCopy().Get("X-Call") != id {
						t.Error("independent peer/identity/data oracle differs")
					}
				},
			})
		}
		conformance.Receive(t, ctx, f.inbox, expected)
		status := f.assembly.Snapshot().Sources[0]
		conformance.Accounting(t, status.Usage, status.Limits, f.inbox.Usage(), 16, 16*client.EvidenceBytes())
		if status.Usage.Active != 0 || status.Usage.Queued != 0 || f.inbox.Usage().Outstanding != 0 {
			t.Fatal("accounting did not return to zero")
		}
		build, err := Build()
		if err != nil {
			t.Fatal(err)
		}
		report, err := compatibility.Assess(build, access, client.Profile(), []compatibility.Requirement{{Guarantee: "native-http", Layers: []compatibility.Layer{compatibility.SDK, compatibility.Capability}}}, nil)
		if err != nil || report.Require(compatibility.Policy{}) == nil {
			t.Fatal("missing compatibility evidence was certified", err)
		}
		conformance.Facade(t, client, "Format", "LogValue", "MarshalJSON", "UnmarshalJSON", "Open", "Do", "Connect", "EvidenceBytes", "Profile")
	})
}

func TestBorrowedAliasKeepsIdentityAndAllowance(t *testing.T) {
	server, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, "shared") })
	options.MaxActive = 1
	f := bindFixture(t, options, 2)
	borrowed := resource.Borrow("alias", f.assembly, f.selected)
	assembly, err := resource.Assemble(deadline(t), deadline(t), "borrowed", borrowed)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(deadline(t))
	client, err := Bind(assembly, borrowed, f.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	stream, first, err := client.Open(deadline(t), correlation("borrowed"), newRequest(t, "GET", server.URL, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close(deadline(t))
	if second, err := f.client.Do(deadline(t), deadline(t), correlation("blocked"), newRequest(t, "GET", server.URL, nil)); second != nil || !errors.Is(err, resource.ErrCapacity) {
		t.Fatal("alias multiplied root quota", err)
	}
	if err := f.assembly.Close(deadline(t)); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("owner released a live borrower")
	}
	if err := stream.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	result := settle(t, f, first)
	if result.Source.Configuration.Identity.Name != options.Name || result.Context.Source != options.Name || result.Source.Scope != "test" {
		t.Fatal("borrower relabeled authoritative identity")
	}
	if err := assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
}

func TestPrematureCompletionRejectingControl(t *testing.T) {
	mode := os.Getenv("FATHOMRY_NETHTTP_CONTROL")
	if mode == "" {
		binary, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		for _, control := range []string{"valid", "released", "complete"} {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			command := exec.CommandContext(ctx, binary, "-test.run=^TestPrematureCompletionRejectingControl$", "-test.v")
			command.Env = append(os.Environ(), "FATHOMRY_NETHTTP_CONTROL="+control)
			output, err := command.CombinedOutput()
			cancel()
			if control == "valid" {
				if err != nil {
					t.Fatalf("positive conformance control failed: %s", output)
				}
			} else {
				wanted := "technical completion and local-use confirmation"
				if err == nil || !strings.Contains(string(output), wanted) {
					t.Fatalf("rejecting control failed for the wrong reason: %s", output)
				}
			}
		}
		return
	}
	server, options := newPeer(t, false, func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, "body") })
	f := bindFixture(t, options, 1)
	stream, receipt, err := f.client.Open(deadline(t), correlation("live"), newRequest(t, "GET", server.URL, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close(deadline(t)); settle(t, f, receipt) }()
	got, _ := receipt.Result()
	if mode == "released" {
		got.Released = true
	}
	if mode == "complete" {
		got.Final = true
	}
	access, _ := resource.AccessFor(f.assembly, f.selected)
	expected := conformance.Expected[Result]{
		Context: fault.Context{Provider: ProviderID, Source: options.Name, Scope: "test", Operation: "request", Correlation: correlation("live")},
		Source:  access.Info(), Limits: access.Limits(), Shape: invocation.Stream, Attempts: invocation.Attempts{Observed: 1},
	}
	conformance.Result(t, got, expected)
}
