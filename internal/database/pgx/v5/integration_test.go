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

package pgx

import (
	"bytes"
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
	sdk "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestComposedSourceProfileAndIndependentReception(t *testing.T) {
	peer := newProtocolPeer(t, false)
	options := peer.options()
	fixture := bindFixture(t, options, 1)
	info := fixture.database.access.Info()
	profile := fixture.database.Profile()
	profile.Options[0].Value = "changed"
	if fixture.database.Profile().Options[0].Value != "exec" {
		t.Fatal("effective profile aliases caller")
	}
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"github.com/jackc/pgx/v5", "github.com/jackc/puddle/v2"}})
	if err != nil {
		t.Fatal(err)
	}
	report, err := compatibility.Assess(build, fixture.database.access, fixture.database.Profile(),
		[]compatibility.Requirement{{Guarantee: "bounded-query", Layers: []compatibility.Layer{compatibility.Capability, compatibility.SDK, compatibility.Service}}}, nil)
	if err != nil || report.Source.Configuration.Revision != info.Configuration.Revision || report.Require(compatibility.Policy{}) == nil {
		t.Fatal("source profile confused missing service evidence with support")
	}
	result := queryResult(t, fixture.database, "evidence", "SELECT partial")
	if result.Err() == nil {
		t.Fatal("native partial failure not produced")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conformance.Receive(t, ctx, fixture.inbox, []conformance.Expected[Result]{{
		Context: resultContext(info, "query", correlation("evidence")), Source: info, Limits: LimitsV1(options),
		Shape: invocation.Finite, Present: true, Final: true, Released: true, Primary: ErrQuery,
		Attempts: invocation.Attempts{Observed: 1, Exact: false},
		Value: func(t testing.TB, value Result) {
			if value.Complete() || value.RowsRead() != 1 || string(value.RowsCopy()[0].ValuesCopy()[0]) != "first" {
				t.Error("postgres contract: independent partial-row oracle failed")
			}
		},
	}})
	conformance.Facade(t, fixture.database, "Query", "Exec", "Prepare", "Begin", "Ping", "Stats", "Profile", "Format", "LogValue", "String", "GoString", "MarshalJSON", "UnmarshalJSON")
	conformance.Runtime(t, fixture.database, new(Database), "credential-canary")
}
func resultContext(info resource.Info, operation string, correlation fault.Correlation) fault.Context {
	return fault.Context{Provider: ProviderID, Scope: info.Scope, Source: info.Configuration.Identity.Name, Operation: operation, Correlation: correlation}
}

func TestPostgresNegativeControlChild(t *testing.T) {
	mode := os.Getenv("FATHOMRY_GH23_CONTROL")
	if mode == "" {
		return
	}
	peer := newProtocolPeer(t, false)
	fixture := bindFixture(t, peer.options(), 2)
	switch mode {
	case "complete", "privacy", "native":
		result := queryResult(t, fixture.database, "partial", "SELECT partial")
		if mode == "complete" {
			result.Outcome.Value.data.complete = true
			if result.Outcome.Value.Complete() {
				t.Error("postgres contract: partial result certified complete")
			}
		}
		if mode == "privacy" {
			var native *pgconn.PgError
			if !errors.As(result.Err(), &native) {
				t.Fatal("native error absent")
			}
			conformance.Private(t, native, "native-cause-canary", "private-detail-canary")
		}
		if mode == "native" {
			handle, err := fixture.database.owner.take(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			conformance.Facade(t, handle.Value().native, "Query", "Exec")
			if err := fixture.database.owner.give(handle); err != nil {
				t.Fatal(err)
			}
		}
		drain(t, fixture.inbox, 1)
	case "commit":
		peer.dropCommit.Store(true)
		transaction := beginTransaction(t, fixture, "lost")
		receipt, err := transaction.Commit(context.Background())
		result := operationResult(t, receipt, err)
		result.Outcome.Value.data.transaction = CommitAcknowledged
		if result.Outcome.Value.TransactionOutcome() != FinalizationUnknown || peer.commits.Load() != 1 {
			t.Error("postgres contract: lost response certified committed")
		}
		drain(t, fixture.inbox, 1)
	case "cleanup":
		native := errors.New("native-cleanup-canary")
		observed := failure(ErrCleanup, "close", native)
		observed = nil
		conformance.Cause(t, observed, func(value *fault.Error) bool { return errors.Is(value, native) })
	default:
		t.Fatal("unknown negative control")
	}
	t.Log("postgres negative oracle returned")
}
func TestPostgresBrokenControlsFailTheirIntendedOracle(t *testing.T) {
	for mode, diagnostic := range map[string]string{
		"complete": "partial result certified complete", "privacy": "diagnostic projection disclosed",
		"native": "dynamic facade exposes", "commit": "lost response certified committed", "cleanup": "native cause inspection lost",
	} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPostgresNegativeControlChild$", "-test.v", "-test.timeout=15s")
			command.Env = append(os.Environ(), "FATHOMRY_GH23_CONTROL="+mode, "GORACE=atexit_sleep_ms=0")
			output, err := command.CombinedOutput()
			var failed *exec.ExitError
			if !errors.As(err, &failed) || failed.ExitCode() != 1 || ctx.Err() != nil ||
				!bytes.Contains(output, []byte(diagnostic)) || !bytes.Contains(output, []byte("postgres negative oracle returned")) ||
				bytes.Contains(output, []byte("panic:")) || bytes.Contains(output, []byte("credential-canary")) ||
				bytes.Contains(output, []byte("private-detail-canary")) {
				t.Fatal("broken control did not fail its intended assertion safely")
			}
		})
	}
}
func TestActualPostgresConsumingExecutable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "consumer")
	command := exec.CommandContext(ctx, "go", "build", "-o", binary, "./testdata/consumer")
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("consumer build failed: %v\n%s", err, output)
	}
	output, err := exec.CommandContext(ctx, binary).Output()
	if err != nil {
		t.Fatal("consumer execution failed")
	}
	var result struct {
		Go                   string
		ConstructedAndClosed bool
		QueryExecuted        bool
		Modules              map[string]string
	}
	if json.Unmarshal(output, &result) != nil || result.Go != runtime.Version() || !result.ConstructedAndClosed || result.QueryExecuted {
		t.Fatal("consuming build evidence or execution scope changed")
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{"github.com/jackc/pgx/v5": "v5.11.0", "github.com/jackc/puddle/v2": "v2.2.2",
		"github.com/jackc/pgpassfile": "v1.0.0", "github.com/jackc/pgservicefile": "v0.0.0-20240606120523-5a60cdf6a761",
		"golang.org/x/sync": "v0.22.0", "golang.org/x/text": "v0.41.0"}
	for path, version := range expected {
		found := false
		for _, module := range info.Deps {
			if module.Path == path {
				found = module.Version == version && module.Replace == nil && module.Sum != "" && result.Modules[path] == version
			}
		}
		if !found {
			t.Fatal("actual executable has an unverified SDK/replacement combination")
		}
	}
	t.Log("Consumer configuration/ownership executed; native query tests and real-service acceptance are separate.")
}

type hostileArgument struct{ called *bool }

func (value hostileArgument) RewriteQuery(context.Context, *sdk.Conn, string, []any) (string, []any, error) {
	*value.called = true
	return "", nil, nil
}
func TestNativeArgumentEscapePathsAreRejectedBeforeAdmission(t *testing.T) {
	called := false
	for _, argument := range []any{hostileArgument{&called}, sdk.QueryExecModeSimpleProtocol, sdk.QueryResultFormats{1}, map[string]any{}, func() { called = true }} {
		if err := validStatement("SELECT $1", []any{argument}); !errors.Is(err, ErrUnsupported) {
			t.Fatal("native argument escape accepted")
		}
	}
	if called {
		t.Fatal("native argument callback ran")
	}
	for _, sql := range []string{"BEGIN", "COMMIT", "ROLLBACK", "COPY fixture TO STDOUT", "LISTEN fixture", "SET ROLE fixture", "/* comment */ SELECT 1"} {
		if err := validStatement(sql, nil); err != nil {
			t.Fatal("structurally valid SQL was filtered by its keyword")
		}
	}
}
