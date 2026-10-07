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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/adapters/internal/errorbridge"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/sqlengine/doris/v1"
	sdk "github.com/go-sql-driver/mysql"
)

func publicWait(t testing.TB, causes ...error) *failure.Detailed[adapters.Details] {
	t.Helper()
	for _, def := range adapters.Definitions() {
		if def.Code == adapters.ErrWait {
			value, err := failure.NewDetailed(def, failure.Location{Operation: "wait"}, adapters.Details{Sequence: 61, Pending: true}, func(v adapters.Details) adapters.Details { return v }, causes...)
			if err != nil {
				t.Fatal(err)
			}
			return value
		}
	}
	t.Fatal("missing wait declaration")
	return nil
}
func requirePrivateError(t testing.TB, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("failure became success")
	}
	if strings.Contains(fmt.Sprintf("%v %+v %#v %s %q", err, err, err, err, err), "private-") {
		t.Fatal("private error text exposed")
	}
	var output bytes.Buffer
	slog.New(slog.NewJSONHandler(&output, nil)).Error("failure", "error", err)
	if strings.Contains(output.String(), "private-") || strings.Contains(output.String(), "panicked") {
		t.Fatal("structured error text exposed")
	}
	if _, err := json.Marshal(err); !errors.Is(err, failure.ErrSerialization) {
		t.Fatal("error serialization allowed")
	}
}
func TestErrorBoundaryPublicForwarding(t *testing.T) {
	remote := &sdk.MySQLError{Number: 1077, Message: "private-native"}
	original := publicWait(t, context.DeadlineExceeded, remote)
	wrapper := fmt.Errorf("private-wrapper: %w", original)
	for _, input := range []error{original, wrapper, errors.Join(wrapper), invocation.ErrFailed.New(fault.Context{}, wrapper),
		source.ErrConfiguration.New(fault.Context{}, wrapper), source.ErrAssembly.New(fault.Context{}, wrapper), source.ErrInitialization.New(fault.Context{}, wrapper)} {
		got := translate(input, "query")
		core, ok := failure.Inspect(got)
		var detailed *failure.Detailed[adapters.Details]
		var nativeError *sdk.MySQLError
		if !ok || core != original.Failure() || !errors.Is(got, input) || !errors.As(got, &detailed) || detailed != original || !errors.As(got, &nativeError) || nativeError != remote {
			t.Fatal("public core/details/original graph changed")
		}
		if translate(got, "cleanup") != got {
			t.Fatal("forwarded occurrence reclassified")
		}
		requirePrivateError(t, got)
	}
}
func TestErrorBoundaryMixedAggregateAndNativeFrame(t *testing.T) {
	shared := publicWait(t)
	provider := fail(ErrLimit, "query", errors.New("private-limit"))
	original := fmt.Errorf("private-aggregate: %w", errors.Join(shared, provider))
	got := translate(original, "query")
	if _, ok := failure.Inspect(got); ok {
		t.Fatal("aggregate invented one owner")
	}
	for range 8 {
		if translate(got, "cleanup") != got {
			t.Fatal("aggregate lost classification")
		}
	}
	for _, cause := range []error{original, shared, provider, adapters.ErrWait, ErrLimit} {
		if !errors.Is(got, cause) {
			t.Fatal("aggregate dropped cause")
		}
	}
	requirePrivateError(t, got)
	hidden := native.ErrProtocol.New(fault.Context{}, errors.New("private-hidden"))
	classified := publicWait(t, hidden)
	frame := native.ErrSQL.New(fault.Context{}, classified, native.ErrLimit)
	got = translate(source.ErrAssembly.New(fault.Context{}, frame), "query")
	core, ok := failure.Inspect(got)
	if !ok || core.Diagnostic().Definition.Code != ErrSQL || !errors.Is(got, ErrLimit) || errors.Is(got, ErrProtocol) || !errors.Is(got, hidden) {
		t.Fatal("native frame or opaque public subtree lost")
	}
	forwarded := errorbridge.Forward(frame, errors.Join(shared, provider))
	if translate(forwarded, "query") != forwarded {
		t.Fatal("retained history reclassified")
	}
	requirePrivateError(t, got)
}

type graphCycle struct{ visits int }

func (*graphCycle) Error() string   { panic("untrusted error text") }
func (v *graphCycle) Unwrap() error { v.visits++; return v }

type graphEmpty struct{}

func (graphEmpty) Error() string   { return "private-empty" }
func (graphEmpty) Unwrap() []error { return nil }
func TestErrorBoundaryBoundedInspection(t *testing.T) {
	cycle := new(graphCycle)
	got := translate(cycle, "query")
	core, ok := failure.Inspect(got)
	if !ok || cycle.visits != 128 || core.Unwrap()[0] != cycle {
		t.Fatal("unbounded graph or original lost")
	}
	requirePrivateError(t, got)
	empty := graphEmpty{}
	got = translate(empty, "query")
	if !errors.Is(got, empty) || !errors.Is(got, adapters.ErrOperation) {
		t.Fatal("non-nil empty graph became success")
	}
	requirePrivateError(t, got)
}
func TestClosedSourceRetainsSharedClassification(t *testing.T) {
	owner, _, _ := testOwner(t, newSQLPeer(t, false).options(), 0)
	if err := owner.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	release, err := owner.state.use()
	got := translate(err, "query")
	core, ok := failure.Inspect(got)
	if release != nil || !ok || core.Diagnostic().Definition.Code != adapters.ErrClosed || !errors.Is(got, adapters.ErrClosed) {
		t.Fatal("closed owner reclassified")
	}
}
func FuzzErrorBoundaryGraphs(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5})
	f.Add([]byte{1, 0, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 32 {
			return
		}
		public := publicWait(t, context.Canceled)
		var original error = public
		entirelyPublic := true
		for _, shape := range data {
			switch shape % 6 {
			case 0:
				original = fmt.Errorf("private-wrapper: %w", original)
			case 1:
				original = errors.Join(original, public)
			case 2:
				original = invocation.ErrFailed.New(fault.Context{}, original)
			case 3:
				original = native.ErrSQL.New(fault.Context{}, original)
				entirelyPublic = false
			case 4:
				original = source.ErrConfiguration.New(fault.Context{}, original)
			case 5:
				original = source.ErrAssembly.New(fault.Context{}, original)
			}
		}
		got := translate(original, "query")
		if !errors.Is(got, original) || !errors.Is(got, public) || !errors.Is(got, context.Canceled) || translate(got, "cleanup") != got {
			t.Fatal("graph identity/idempotence changed")
		}
		if entirelyPublic && (errors.Is(got, ErrSQL) || errors.Is(got, adapters.ErrOperation)) {
			t.Fatal("public graph acquired provider fallback")
		}
		requirePrivateError(t, got)
	})
}
