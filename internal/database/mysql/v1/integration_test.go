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

package mysql

import (
	"context"
	"database/sql/driver"
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
)

func TestComposedSourceProfileAndIndependentReception(t *testing.T) {
	peer := newPeer(t, true, false)
	options := peer.options()
	fixture := bindFixture(t, options, 1)
	info := fixture.db.access.Info()
	profile := fixture.db.Profile()
	profile.Options[0].Value = "changed"
	if fixture.db.Profile().Options[0].Value != "caching_sha2_password" {
		t.Fatal("effective profile aliases caller")
	}
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"github.com/go-sql-driver/mysql", "filippo.io/edwards25519"}})
	if err != nil {
		t.Fatal(err)
	}
	report, err := compatibility.Assess(build, fixture.db.access, fixture.db.Profile(),
		[]compatibility.Requirement{{Guarantee: "bounded-query", Layers: []compatibility.Layer{compatibility.Capability, compatibility.SDK, compatibility.Service}}}, nil)
	if err != nil || report.Source.Configuration.Revision != info.Configuration.Revision || report.Require(compatibility.Policy{}) == nil {
		t.Fatal("declarations or missing service evidence were mistaken for support")
	}
	receipt, err := fixture.db.Query(context.Background(), correlation("partial"), "SELECT partial")
	if result := observe(t, receipt, err); result.Err() == nil {
		t.Fatal("native partial error absent")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conformance.Receive(t, ctx, fixture.inbox, []conformance.Expected[Result]{{
		Context: fault.Context{Provider: ProviderID, Scope: info.Scope, Source: options.Name, Operation: "query", Correlation: correlation("partial")},
		Source:  info, Limits: LimitsV1(options), Shape: invocation.Finite, Present: true, Final: true, Released: true,
		Primary: ErrQuery, Attempts: invocation.Attempts{Observed: 1, Exact: false},
		Value: func(t testing.TB, value Result) {
			if value.Complete() || value.RowsRead() != 1 || string(value.RowsCopy()[0].ValuesCopy()[0]) != "value-canary" {
				t.Error("independent partial-row evidence changed")
			}
		},
	}})
	conformance.Facade(t, fixture.db, "Query", "Exec", "Prepare", "Begin", "Ping", "Stats", "Profile", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
	conformance.Runtime(t, fixture.db, new(Database), "credential-canary")
	conformance.Runtime(t, options, new(OptionsV1), "credential-canary")
}

func TestActualMySQLConsumingExecutable(t *testing.T) {
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
		t.Fatal("consumer build evidence or execution scope changed")
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
			t.Fatal("actual executable has an unverified SDK/replacement combination")
		}
	}
	t.Log("Consumer construction/ownership executed; native queries and real service acceptance are separate.")
}

type hostileValuer struct{ called *bool }

func (v hostileValuer) Value() (driver.Value, error) { *v.called = true; return "injected", nil }
func TestNativeArgumentEscapesCannotInvokeCallerCode(t *testing.T) {
	called := false
	for _, arg := range []any{hostileValuer{&called}, map[string]any{}, func() { called = true }} {
		if err := validStatement("SELECT ?", []any{arg}); !errors.Is(err, ErrUnsupported) {
			t.Fatal("dynamic argument surface was admitted")
		}
	}
	if called {
		t.Fatal("native argument callback executed")
	}
}
