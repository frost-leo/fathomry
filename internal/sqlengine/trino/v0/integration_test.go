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

package trino

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestControlledCompositionAndIndependentEvidence(t *testing.T) {
	_, o := peer(t, func(w http.ResponseWriter, r *http.Request) {
		reply(t, w, map[string]any{"id": "composition", "columns": []any{column("x", "bigint")}, "data": [][]int{{7}}})
	})
	f := bindFixture(t, o, 2)
	info := f.client.access.Info()
	want := conformance.Expected[Result]{Context: fault.Context{Provider: ProviderID, Operation: "query", Source: o.Name, Scope: "gh41",
		Correlation: correlation("composition")}, Source: info, Limits: LimitsV1(o), Shape: invocation.Finite,
		Present: true, Final: true, Released: true, Attempts: invocation.Attempts{Observed: 1},
		Value: func(t testing.TB, value Result) {
			if string(value.DataCopy()) != "[[7]]" || value.Effect() != ReadOnly || value.QueryID() != "composition" ||
				!value.Complete() || !value.Terminal() || value.Submissions() != 1 {
				t.Error("independent row/protocol oracle mismatch")
			}
		}}
	receipt, err := f.client.Query(deadline(t), deadline(t), correlation("composition"), Statement{SQL: "SELECT 7"})
	conformance.Result(t, settle(t, receipt, err), want)
	conformance.Receive(t, deadline(t), f.inbox, []conformance.Expected[Result]{want})
	conformance.Accounting(t, f.assembly.Snapshot().Sources[0].Usage, LimitsV1(o), f.inbox.Usage(), 2, 2*f.client.EvidenceBytes())
	conformance.Facade(t, f.client, "Query", "Execute", "Insert", "Profile", "EvidenceBytes", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
	conformance.Runtime(t, Statement{SQL: "private-sql", Args: []any{"private-value"}}, &Statement{}, "private-sql", "private-value")
	conformance.Runtime(t, Result{}, &Result{}, "private-value")
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"github.com/trinodb/trino-go-client"}})
	if err != nil {
		t.Fatal(err)
	}
	report, err := compatibility.Assess(build, f.client.access, f.client.Profile(),
		[]compatibility.Requirement{{Guarantee: "trino-direct", Layers: []compatibility.Layer{compatibility.Capability, compatibility.SDK, compatibility.Service}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Decisions) != 1 || report.Decisions[0].Status == compatibility.Tested {
		t.Fatal("mock promoted to service compatibility")
	}
}
func TestBorrowedAdmissionAndShutdownHaveOneOwner(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var posts atomic.Int64
	_, o := peer(t, func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		close(entered)
		<-release
		reply(t, w, map[string]any{"id": "borrow"})
	})
	o.MaxActive = 1
	f := bindFixture(t, o, 2)
	borrowed := resource.Borrow("alias", f.assembly, f.selected)
	assembly, err := resource.Assemble(deadline(t), deadline(t), "borrower", borrowed)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		if err := assembly.Close(deadline(t)); err != nil {
			t.Error(err)
		}
	})
	client, err := Bind(assembly, borrowed, f.inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.assembly.Close(deadline(t)); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("owner released an active borrower")
	}
	ctx := deadline(t)
	cleanup := deadline(t)
	done := make(chan error, 1)
	go func() {
		receipt, err := client.Query(ctx, cleanup, correlation("borrow"), Statement{SQL: "SELECT 1"})
		if err == nil {
			result, _ := receipt.Result()
			err = result.Err()
		}
		done <- err
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatal("operation ended before blocked HTTP entry", err)
	case <-ctx.Done():
		t.Fatal("HTTP entry was not observed within the fixture budget")
	}
	if receipt, err := client.Query(ctx, cleanup, correlation("saturated"), Statement{SQL: "SELECT 1"}); receipt != nil || err == nil || posts.Load() != 1 {
		t.Error("alias bypassed authoritative capacity")
	}
	if err := assembly.Close(deadline(t)); !errors.Is(err, resource.ErrIncomplete) {
		t.Error("borrower released active SDK work")
	}
	once.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := f.assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if receipt, err := client.Query(ctx, cleanup, correlation("closed"), Statement{SQL: "SELECT 1"}); receipt != nil || err == nil {
		t.Fatal("closed binding submitted")
	}
}
func TestSourceIsolationAndFrozenOverlays(t *testing.T) {
	for _, user := range []string{"source_a", "source_b"} {
		t.Run(user, func(t *testing.T) {
			_, o := peer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Trino-User") != user || r.Header.Get("X-Trino-Catalog") != "frozen_catalog" {
					t.Error("source settings contaminated")
				}
				reply(t, w, map[string]any{"id": "isolation"})
			})
			o.User = user
			layer := []byte("catalog: frozen_catalog")
			selected, err := Select(o, resource.Layer{Kind: resource.Local, Content: layer})
			if err != nil {
				t.Fatal(err)
			}
			for i := range layer {
				layer[i] = 'x'
			}
			o.User = "mutated"
			selected = resource.WithLimits(selected, LimitsV1(o))
			assembly, err := resource.Assemble(deadline(t), deadline(t), "isolated", selected)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := assembly.Close(deadline(t)); err != nil {
					t.Error(err)
				}
			}()
			inbox, _ := invocation.NewInbox[Result](1, defaults(o).evidenceBytes())
			client, err := Bind(assembly, selected, inbox, nil)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := client.Query(deadline(t), deadline(t), correlation("isolated"), Statement{SQL: "SELECT 1"})
			success(t, settle(t, receipt, err))
			record, err := inbox.Next(deadline(t))
			if err != nil {
				t.Fatal(err)
			}
			if err := record.Release(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestSessionOverridesCannotEnableHiddenRetries(t *testing.T) {
	s := defaults(OptionsV1{Writes: true})
	for _, sql := range []string{
		"WITH SESSION retry_policy='TASK' SELECT 1",
		"CREATE TABLE data AS WITH SESSION retry_policy='QUERY' SELECT 1",
	} {
		if _, err := prepare(Statement{SQL: sql}, s, false); !errors.Is(err, ErrUnsupported) {
			t.Fatal("statement-level session overrides accepted")
		}
	}
}
func TestMissingCleanupBudgetKeepsRemoteUnknown(t *testing.T) {
	started := make(chan struct{})
	var base string
	_, o := peer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			reply(t, w, map[string]any{"id": "deadline", "nextUri": base + "/v1/statement/executing/deadline/slug/1"})
			return
		}
		if r.Method == http.MethodDelete {
			t.Error("cancel sent with cancelled cleanup authority")
			return
		}
		close(started)
		<-r.Context().Done()
	})
	base = o.Endpoint
	f := bindFixture(t, o, 1)
	work, cancel := context.WithCancel(deadline(t))
	defer cancel()
	cleanup, cancelCleanup := context.WithCancel(deadline(t))
	defer cancelCleanup()
	done := make(chan invocation.Result[Result], 1)
	go func() {
		receipt, err := f.client.Execute(work, cleanup, correlation("deadline"), Statement{SQL: "UPDATE data SET x=1"})
		if err != nil {
			done <- invocation.Result[Result]{Outcome: invocation.Outcome[Result]{Primary: err}}
			return
		}
		result, _ := receipt.Result()
		done <- result
	}()
	select {
	case <-started:
	case <-done:
		t.Fatal("operation ended before the pending GET")
	case <-work.Done():
		t.Fatal("pending GET did not start within the fixture budget")
	}
	cancelCleanup()
	cancel()
	var result invocation.Result[Result]
	select {
	case result = <-done:
	case <-deadline(t).Done():
		t.Fatal("cancelled work did not release within the fixture budget")
	}
	if !result.Released || !errors.Is(result.Outcome.Cleanup, context.Canceled) || result.Outcome.Value.CancellationAttempted() ||
		result.Outcome.Value.Terminal() || result.Outcome.Value.Effect() != Unknown {
		t.Fatal("missing cleanup evidence strengthened")
	}
	if strings.Contains(result.Err().Error(), o.Endpoint) {
		t.Fatal("private endpoint exposed")
	}
}
