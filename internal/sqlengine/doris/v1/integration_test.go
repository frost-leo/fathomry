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

package doris

import (
	"context"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

type fixture struct {
	client    *Client
	assembly  *resource.Assembly
	selection resource.Selection[Source]
	inbox     *invocation.Inbox[Result]
}

func bindFixture(t testing.TB, options OptionsV1, capacity int) fixture {
	t.Helper()
	selection, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selection = resource.WithLimits(selection, LimitsV1(options))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	assembly, err := resource.Assemble(ctx, ctx, "doris-tests", selection)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := assembly.Close(ctx); err != nil {
			t.Error("assembly cleanup", err)
		}
	})
	inbox, err := invocation.NewInbox[Result](capacity, int64(capacity)*defaults(options).evidenceReservation())
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, selection, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	return fixture{client: client, assembly: assembly, selection: selection, inbox: inbox}
}
func correlation(id string) fault.Correlation { return fault.Correlation{Call: id} }
func observe(t testing.TB, receipt *invocation.Receipt[Result], err error) invocation.Result[Result] {
	t.Helper()
	if err != nil || receipt == nil {
		t.Fatal("setup", err)
	}
	result, ready := receipt.Result()
	if !ready || !result.Final || !result.Released {
		t.Fatal("synchronous call did not finalize local ownership")
	}
	return result
}
func drain(t testing.TB, inbox *invocation.Inbox[Result], count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for range count {
		delivery, err := inbox.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Release(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestIndependentCompositionEvidenceAndCompatibility(t *testing.T) {
	peer := newSQLPeer(t, false)
	options := peer.options()
	f := bindFixture(t, options, 1)
	receipt, err := f.client.Query(context.Background(), correlation("partial"), "SELECT partial")
	result := observe(t, receipt, err)
	if result.Err() == nil || result.Outcome.Value.Complete() {
		t.Fatal("partial query certified")
	}
	info := f.client.access.Info()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conformance.Receive(t, ctx, f.inbox, []conformance.Expected[Result]{{
		Context: fault.Context{Provider: ProviderID, Scope: info.Scope, Source: options.Name, Operation: "query", Correlation: correlation("partial")},
		Source:  info, Limits: LimitsV1(options), Shape: invocation.Finite, Present: true, Final: true, Released: true,
		Primary: ErrSQL, Attempts: invocation.Attempts{Observed: 2, Exact: false},
		Value: func(t testing.TB, value Result) {
			if len(value.RowsCopy()) != 1 || string(value.RowsCopy()[0].ValuesCopy()[0]) != "row-canary" {
				t.Fatal("partial rows not retained independently")
			}
		},
	}})
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"github.com/go-sql-driver/mysql"}})
	if err != nil {
		t.Fatal(err)
	}
	report, err := compatibility.Assess(build, f.client.access, f.client.Profile(),
		[]compatibility.Requirement{{Guarantee: "batch-query", Layers: []compatibility.Layer{compatibility.Capability, compatibility.SDK, compatibility.Service}}}, nil)
	if err != nil || report.Require(compatibility.Policy{}) == nil {
		t.Fatal("unverified service inherited support")
	}
	profile := f.client.Profile()
	profile.Options[0].Value = "changed"
	if f.client.Profile().Options[0].Value == "changed" {
		t.Fatal("profile aliases caller")
	}
	conformance.Facade(t, f.client, "Query", "Exec", "StreamLoad", "InspectLabel", "Profile", "Format", "MarshalJSON", "UnmarshalJSON", "LogValue")
	conformance.Runtime(t, f.client, new(Client), "credential-canary")
	conformance.Runtime(t, options, new(OptionsV1), "credential-canary")
	conformance.Runtime(t, result.Outcome.Value, new(Result), "row-canary", "native-error-canary")
}
func TestIndependentSourcesBorrowingAndClose(t *testing.T) {
	peer := newSQLPeer(t, false)
	options := peer.options()
	first, second := bindFixture(t, options, 1), bindFixture(t, options, 1)
	if first.client.owner == second.client.owner {
		t.Fatal("independent ownership was shared")
	}
	borrowed := resource.Borrow("borrowed", first.assembly, first.selection)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assembly, err := resource.Assemble(ctx, ctx, "borrower", borrowed)
	if err != nil {
		t.Fatal(err)
	}
	client, err := Bind(assembly, borrowed, first.inbox, nil)
	if err != nil || client.owner != first.client.owner {
		t.Fatal("explicit borrow cloned resource")
	}
	if err := first.assembly.Close(ctx); err == nil {
		t.Fatal("owner released live borrower")
	}
	receipt, err := client.Query(ctx, correlation("borrowed"), "SELECT empty")
	if observe(t, receipt, err).Err() != nil {
		t.Fatal("borrow lost authority")
	}
	drain(t, first.inbox, 1)
	if err := assembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.assembly.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Query(ctx, correlation("closed"), "SELECT empty"); err == nil {
		t.Fatal("closed scope admitted work")
	}
	receipt, err = second.client.Query(ctx, correlation("other"), "SELECT empty")
	if observe(t, receipt, err).Err() != nil {
		t.Fatal("another source was closed")
	}
	drain(t, second.inbox, 1)
}
func TestEvidenceCapacityRejectsBeforeDispatch(t *testing.T) {
	peer := newSQLPeer(t, false)
	f := bindFixture(t, peer.options(), 1)
	receipt, err := f.client.Query(context.Background(), correlation("one"), "SELECT empty")
	if observe(t, receipt, err).Err() != nil {
		t.Fatal("first query failed")
	}
	before := peer.queries.Load()
	receipt, err = f.client.Query(context.Background(), correlation("two"), "SELECT empty")
	if receipt != nil || !errors.Is(err, invocation.ErrEvidence) || peer.queries.Load() != before {
		t.Fatal("evidence saturation dispatched")
	}
	drain(t, f.inbox, 1)
}

func TestActualConsumingExecutable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "consumer")
	command := exec.CommandContext(ctx, "go", "build", "-o", binary, "./testdata/consumer")
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("consumer build: %v\n%s", err, output)
	}
	output, err := exec.CommandContext(ctx, binary).Output()
	if err != nil {
		t.Fatal("consumer execution failed")
	}
	var result struct {
		Go                   string
		ConstructedAndClosed bool
		Modules              map[string]string
	}
	if json.Unmarshal(output, &result) != nil || !result.ConstructedAndClosed || result.Go != runtime.Version() {
		t.Fatal("consumer ownership/build evidence changed")
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	for path, version := range map[string]string{"github.com/go-sql-driver/mysql": "v1.10.1", "filippo.io/edwards25519": "v1.2.0"} {
		found := false
		for _, module := range info.Deps {
			if module.Path == path {
				found = module.Version == version && module.Replace == nil && module.Sum != "" && result.Modules[path] == version
			}
		}
		if !found {
			t.Fatal("unverified contributing driver combination")
		}
	}
}
