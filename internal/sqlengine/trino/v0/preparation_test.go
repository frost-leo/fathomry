/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package trino

import (
	"context"
	"math"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

func TestPreparationUsesOneResolvedBudgetWithoutReadiness(t *testing.T) {
	var requests atomic.Int64
	_, options := peer(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		reply(t, w, map[string]any{"id": "prepared"})
	})
	layers := []resource.Layer{{Kind: resource.Local, Content: []byte("max_active: 3\nmax_sql_bytes: 2048\nmax_result_bytes: 4096\nmax_page_bytes: 1024\nmax_read_rows: 70000\nmax_read_pages: 5000\nmax_read_wire_bytes: 536870912\nread_timeout_ns: 300000000000\n")}}
	preparation, err := PrepareV1(options, layers...)
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 0 {
		t.Fatal("pure preparation submitted readiness")
	}
	resolved := preparation.Options()
	budget := preparation.Budget()
	if resolved.MaxActive != 3 || resolved.MaxReadRows != 70000 || resolved.MaxReadPages != 5000 || resolved.MaxReadWireBytes != 512<<20 || resolved.ReadTimeout != 5*time.Minute {
		t.Fatal("resolved options lost")
	}
	if budget.Active != 3 || budget.SourceBytes < budget.WorkBytes+budget.EvidenceBytes || budget.ReaderWorkBytes < budget.PageBytes || budget.ReaderEvidenceBytes < budget.PageBytes || budget.ReaderTerminalBytes <= 0 {
		t.Fatal("budget misses overlapping storage")
	}
	if preparation.Limits() != LimitsV1(resolved) {
		t.Fatal("admission does not use resolved values")
	}
	same, err := PrepareResolvedV1(resolved)
	if err != nil || same.Budget() != budget || same.Options() != resolved {
		t.Fatal("resolved preparation redefaulted or changed costs")
	}
	resolved.MaxReadRows = 1
	if preparation.Options().MaxReadRows != 70000 {
		t.Fatal("settings alias")
	}
	selected := resource.WithLimits(preparation.Select(), preparation.Limits())
	assembly, err := resource.Assemble(deadline(t), deadline(t), "prepared", selected)
	if assembly != nil {
		defer assembly.Close(deadline(t))
	}
	if err != nil {
		t.Fatal(err)
	}
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := source.owner.settings.budget()
	if err != nil || actual != budget {
		t.Fatal("adopted source changed budget")
	}
}

func TestPreparationStrictZeroAndArithmetic(t *testing.T) {
	options := OptionsV1{Name: "prepared", Endpoint: "http://127.0.0.1:1", User: "test", Plaintext: true}
	prepared, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareResolvedV1(options); err == nil {
		t.Fatal("unresolved values silently defaulted")
	}
	for _, layer := range []string{"max_read_rows: 0", "max_read_pages: -1", "max_read_wire_bytes: 0", "read_timeout_ns: 0", "max_sql_bytes: 0", "unknown: true", "max_read_rows: 1.5", "max_read_rows: null"} {
		if _, err := PrepareV1(options, resource.Layer{Kind: resource.Local, Content: []byte(layer)}); err == nil {
			t.Fatalf("invalid strict override accepted: %s", layer)
		}
	}
	resolved := prepared.Options()
	for _, mutate := range []func(*OptionsV1){
		func(o *OptionsV1) { o.MaxReadRows = 0 }, func(o *OptionsV1) { o.MaxReadPages = 0 }, func(o *OptionsV1) { o.MaxReadWireBytes = 0 }, func(o *OptionsV1) { o.ReadTimeout = 0 },
		func(o *OptionsV1) { o.MaxReadRows = 1<<24 + 1 }, func(o *OptionsV1) { o.MaxReadPages = 65537 }, func(o *OptionsV1) { o.MaxReadWireBytes = 4<<30 + 1 },
	} {
		copy := resolved
		mutate(&copy)
		if _, err := PrepareResolvedV1(copy); err == nil {
			t.Fatal("unsupported resolved bound accepted")
		}
	}
	resolved.MaxReadRows = 1 << 24
	resolved.MaxReadPages = 65536
	resolved.MaxReadWireBytes = 4 << 30
	resolved.MaxActive = 8
	resolved.MaxSQLBytes = 4 << 20
	resolved.MaxPageBytes = 8 << 20
	resolved.MaxResultBytes = 32 << 20
	maximum, err := PrepareResolvedV1(resolved)
	if err != nil {
		t.Fatal(err)
	}
	if maximum.Limits().Bytes <= 0 || maximum.Budget().SourceBytes <= 0 {
		t.Fatal("budget overflow")
	}
	if _, ok := addBudget(math.MaxInt64, 1); ok {
		t.Fatal("overflow allowed")
	}
	if _, ok := addBudget(-1); ok {
		t.Fatal("negative bytes allowed")
	}
}

func TestPreparationReaderAdmissionChargesResolvedWorkAndEvidence(t *testing.T) {
	var base string
	_, options := peer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" {
			w.WriteHeader(204)
			return
		}
		reply(t, w, map[string]any{"id": "budget", "nextUri": base + "/v1/statement/executing/budget/slug/1"})
	})
	base = options.Endpoint
	preparation, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	fixture := bindFixture(t, preparation.Options(), 1)
	reader, receipt, err := fixture.client.QueryPages(deadline(t), deadline(t), correlation("budget"), Statement{SQL: "SELECT 1"})
	if err != nil {
		t.Fatal(err)
	}
	usage := fixture.assembly.Snapshot().Sources[0].Usage
	if usage.ActiveBytes != preparation.Budget().ReaderWorkBytes || fixture.inbox.Usage().ReservedBytes != preparation.Budget().ReaderTerminalBytes {
		t.Fatal("native accounting differs from offline budget")
	}
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	_ = reader.Close(cancelled)
	_ = reader.Close(deadline(t))
	result := readerFinal(t, reader, receipt)
	if !result.Released || fixture.assembly.Snapshot().Sources[0].Usage.ActiveBytes != 0 {
		t.Fatal("actual release did not reclaim working bytes")
	}
	if fixture.inbox.Usage().ReservedBytes == 0 {
		t.Fatal("release erased required evidence")
	}
}
