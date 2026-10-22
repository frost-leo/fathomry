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

package declaration_test

import (
	"context"
	"errors"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
	"github.com/frost-leo/fathomry/i18n/v1"
)

func TestPublicConsumerAndResources(t *testing.T) {
	bundle, err := wf.Compile(context.Background(), health.Declarations())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := bundle.BytesCopy()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := wf.Parse(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	report, err := wf.Check(context.Background(), bundle, parsed)
	if err != nil || !report.Passed || report.Workflows != 1 || report.Modes != 2 {
		t.Fatal(report, err)
	}
	if _, err := failure.Prepare(wf.Definitions()...); err != nil {
		t.Fatal(err)
	}
	catalog, err := i18n.Prepare(wf.Component())
	if err != nil {
		t.Fatal(err)
	}
	entries, err := catalog.Inspect()
	if err != nil || len(entries) != 2*len(wf.Definitions()) {
		t.Fatal(err, len(entries))
	}
	for _, entry := range entries {
		if entry.Component != "workflow" || entry.SourceDigest == "" {
			t.Fatal("incomplete resource", entry.ID)
		}
	}
}

func TestDiagnosticBoundary(t *testing.T) {
	_, err := wf.Parse(context.Background(), []byte("{"))
	if detail, ok := wf.Diagnose(err); !ok || detail.Code != wf.ErrInvalid {
		t.Fatal(detail, ok, err)
	}
	if _, ok := wf.Diagnose(errors.Join(err, context.Canceled)); ok {
		t.Fatal("joined cancellation became completed validation")
	}
	definition := wf.Definitions()[0]
	foreign, problem := failure.NewDetailed(definition, failure.Location{}, wf.Diagnostic{Code: wf.ErrUnsupported}, func(value wf.Diagnostic) wf.Diagnostic { return value })
	if problem != nil {
		t.Fatal(problem)
	}
	if _, ok := wf.Diagnose(foreign); ok {
		t.Fatal("inconsistent detailed identity admitted")
	}
}
