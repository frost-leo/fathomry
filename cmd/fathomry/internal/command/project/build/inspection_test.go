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

package build

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"io"
	"strings"
	"testing"

	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
)

func TestInspection(t *testing.T) {
	ctx := context.Background()
	declaration := health.Declarations()
	var output bytes.Buffer
	if err := Export(ctx, Definition{Name: "health", Declarations: []wf.Declaration{declaration}}, &output); err != nil {
		t.Fatal(err)
	}
	document, report, err := inspect(ctx, output.Bytes())
	if err != nil || document.Name != "health" || !report.Passed || report.Workflows != 1 || report.Modes != 2 {
		t.Fatal(document.Name, report, err)
	}
	original := bytes.Clone(output.Bytes())
	declaration.Workflows[0].Modes[0].Values = map[string]wf.Decimal{"attempt_timeout": "9999"}
	output.Reset()
	if err := Export(ctx, Definition{Name: "health", Declarations: []wf.Declaration{declaration}}, &output); !errors.Is(err, ErrRejected) || output.Len() != 0 {
		t.Fatal("invalid source exported", err)
	}
	if _, _, err := inspect(ctx, original); err != nil {
		t.Fatal("snapshot aliased source", err)
	}
	output.Reset()
	if err := Export(ctx, Definition{Name: "empty"}, &output); err != nil {
		t.Fatal(err)
	}
	_, empty, err := inspect(ctx, output.Bytes())
	if err != nil || !empty.Passed || empty.Inputs != 0 || empty.Workflows != 0 {
		t.Fatal("empty project claimed a workflow", empty, err)
	}
}

func TestInspectionRejects(t *testing.T) {
	ctx := context.Background()
	for _, raw := range []string{
		"", "null", "{}", `{"schema":"fathomry.project-inspection/v2","name":"health","bundles":[]}`,
		`{"schema":"fathomry.project-inspection/v1","name":"health"}`,
		`{"schema":"fathomry.project-inspection/v1","name":"health","bundles":null}`,
		`{"schema":"fathomry.project-inspection/v1","name":"../health","bundles":[]}`,
		`{"schema":"fathomry.project-inspection/v1","name":"health","bundles":[],"extra":0}`,
		`{"schema":"fathomry.project-inspection/v1","name":"health","na\u006de":"health","bundles":[]}`,
		`{"schema":"fathomry.project-inspection/v1","name":"health","bundles":[]}{}`,
		`{"schema":"fathomry.project-inspection/v1","name":"health","bundles":[{}]}`,
		`{"schema":"fathomry.project-inspection/v1","name":"health","bundles":[null]}`,
	} {
		if _, _, err := inspect(ctx, []byte(raw)); err == nil {
			t.Fatalf("admitted %q", raw)
		}
	}
	tooMany := inspection{Schema: inspectionSchema, Name: "health", Bundles: make([]jsontext.Value, wf.MaxInputs+1)}
	for index := range tooMany.Bundles {
		tooMany.Bundles[index] = jsontext.Value("{}")
	}
	raw, err := json.Marshal(tooMany)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := inspect(ctx, raw); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	// The seventeenth element must reject before its malformed content is read.
	bounded := []byte(`{"schema":"fathomry.project-inspection/v1","name":"health","bundles":[` + strings.Repeat("{},", wf.MaxInputs) + "invalid-tail")
	if _, _, err := inspect(ctx, bounded); !errors.Is(err, ErrLimit) {
		t.Fatal("late collection bound", err)
	}
	canceledContext, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := inspect(canceledContext, raw); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestProjectConflict(t *testing.T) {
	first, second := health.Declarations(), health.Declarations()
	second.Workflows[0].Modes[0].Values = map[string]wf.Decimal{"attempt_timeout": "1200"}
	var output bytes.Buffer
	if err := Export(context.Background(), Definition{Name: "health", Declarations: []wf.Declaration{first, second}}, &output); err != nil {
		t.Fatal(err)
	}
	_, report, err := inspect(context.Background(), output.Bytes())
	if !errors.Is(err, ErrRejected) || report.Passed || len(report.Conflicts) != 1 {
		t.Fatal(report, err)
	}
}

type brokenWriter struct{ problem error }

func (writer brokenWriter) Write([]byte) (int, error) { return 0, writer.problem }

func TestExportWriterFailures(t *testing.T) {
	if err := Export(context.Background(), Definition{Name: "health"}, brokenWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
	problem := errors.New("private-canary")
	if err := Export(context.Background(), Definition{Name: "health"}, brokenWriter{problem}); !errors.Is(err, problem) {
		t.Fatal(err)
	}
}

func FuzzInspection(f *testing.F) {
	f.Add([]byte(`{"schema":"fathomry.project-inspection/v1","name":"health","bundles":[]}`))
	f.Add([]byte(`{"schema":"fathomry.project-inspection/v1","name":"health","bundles":[{}]}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20 {
			t.Skip()
		}
		document, report, err := inspect(context.Background(), raw)
		if err == nil && (!validName(document.Name) || !report.Passed || document.Bundles == nil) {
			t.Fatal("invalid success")
		}
	})
}
