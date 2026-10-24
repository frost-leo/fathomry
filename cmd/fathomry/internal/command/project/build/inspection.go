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

	projectworkflows "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow"
	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

// InspectionArgument selects the versioned offline branch before application Boot.
const InspectionArgument = "--fathomry-project=v1"
const inspectionSchema = "fathomry.project-inspection/v1"
const maxInspectionBytes = wf.MaxInputBytes + 4096

// Definition is project-owned assembly data. Workflows combines declarations and
// optional metadata; legacy Declarations is mutually exclusive. Empty inputs
// describe a configuration-only project, never a completed workflow check.
// Name is a portable lowercase project name, also used for the default binary path.
type Definition struct {
	Name         string
	Workflows    []projectworkflows.Workflow
	Declarations []wf.Declaration
}

type inspection struct {
	Schema  string     `json:"schema"`
	Name    string     `json:"name"`
	Bundles bundleList `json:"bundles"`
}

type bundleList []jsontext.Value

func (bundles *bundleList) UnmarshalJSONFrom(decoder *jsontext.Decoder) error {
	start, err := decoder.ReadToken()
	if err != nil {
		return err
	}
	if start.Kind() != '[' {
		return fail(ErrInspection)
	}
	values := bundleList{}
	total := 0
	for decoder.PeekKind() != ']' {
		if len(values) >= wf.MaxInputs {
			return fail(ErrLimit)
		}
		raw, err := decoder.ReadValue()
		if err != nil {
			return err
		}
		if len(raw) > wf.MaxBundleBytes || len(raw) > wf.MaxInputBytes-total {
			return fail(ErrLimit)
		}
		total += len(raw)
		values = append(values, jsontext.Value(bytes.Clone(raw)))
	}
	if _, err := decoder.ReadToken(); err != nil {
		return err
	}
	*bundles = values
	return nil
}

// Export compiles borrowed declarations and writes one complete inspection document.
// It does not load configuration, call business functions, or discover declarations.
// Failure before writing emits no document; writer failure may leave partial output.
func Export(ctx context.Context, definition Definition, output io.Writer) error {
	if err := canceled(ctx); err != nil {
		return err
	}
	if output == nil || !validName(definition.Name) {
		return fail(ErrOptions)
	}
	if len(definition.Declarations) > wf.MaxInputs {
		return fail(ErrLimit)
	}
	if len(definition.Workflows) > 0 && len(definition.Declarations) > 0 {
		return fail(ErrOptions)
	}
	document := inspection{Schema: inspectionSchema, Name: definition.Name, Bundles: []jsontext.Value{}}
	total := 0
	if len(definition.Workflows) > 0 {
		report, bundles, err := projectworkflows.CheckProject(ctx, projectworkflows.ProjectInput{Workflows: definition.Workflows}, projectworkflows.Selection{})
		if err != nil {
			return phaseError(ctx, ErrRejected, err)
		}
		if !report.Passed || !report.FullProject || report.Empty {
			return fail(ErrRejected)
		}
		for _, bundle := range bundles {
			raw, err := bundle.BytesCopy()
			if err != nil {
				return fail(ErrRejected, err)
			}
			total += len(raw)
			if total > wf.MaxInputBytes {
				return fail(ErrLimit)
			}
			document.Bundles = append(document.Bundles, jsontext.Value(raw))
		}
	}
	for _, declared := range definition.Declarations {
		bundle, err := wf.Compile(ctx, declared)
		if err != nil {
			return phaseError(ctx, ErrRejected, err)
		}
		raw, err := bundle.BytesCopy()
		if err != nil {
			return fail(ErrRejected, err)
		}
		total += len(raw)
		if total > wf.MaxInputBytes {
			return fail(ErrLimit)
		}
		document.Bundles = append(document.Bundles, jsontext.Value(raw))
	}
	raw, err := json.Marshal(document)
	if err != nil {
		return fail(ErrInspection, err)
	}
	if len(raw) >= maxInspectionBytes {
		return fail(ErrLimit)
	}
	if err := canceled(ctx); err != nil {
		return err
	}
	return writeAll(output, append(raw, '\n'))
}

func inspect(ctx context.Context, raw []byte) (inspection, wf.Report, error) {
	document := inspection{}
	if err := canceled(ctx); err != nil {
		return document, wf.Report{}, err
	}
	if len(raw) > maxInspectionBytes {
		return document, wf.Report{}, fail(ErrLimit)
	}
	if err := json.Unmarshal(raw, &document, json.RejectUnknownMembers(true)); err != nil {
		if errors.Is(err, ErrLimit) {
			return inspection{}, wf.Report{}, fail(ErrLimit, err)
		}
		return inspection{}, wf.Report{}, fail(ErrInspection, err)
	}
	if document.Schema != inspectionSchema || !validName(document.Name) || document.Bundles == nil {
		return inspection{}, wf.Report{}, fail(ErrInspection)
	}
	if len(document.Bundles) > wf.MaxInputs {
		return inspection{}, wf.Report{}, fail(ErrLimit)
	}
	bundles := make([]*wf.Bundle, 0, len(document.Bundles))
	total := 0
	for _, raw := range document.Bundles {
		total += len(raw)
		if total > wf.MaxInputBytes {
			return inspection{}, wf.Report{}, fail(ErrLimit)
		}
		bundle, err := wf.Parse(ctx, raw)
		if err != nil {
			return inspection{}, wf.Report{}, phaseError(ctx, ErrRejected, err)
		}
		bundles = append(bundles, bundle)
	}
	report := wf.Report{Passed: true, Entries: []wf.Entry{}, Conflicts: []wf.Conflict{}, Diagnostics: []wf.InputDiagnostic{}}
	if len(bundles) != 0 {
		var err error
		report, err = wf.Check(ctx, bundles...)
		if err != nil {
			return inspection{}, wf.Report{}, phaseError(ctx, ErrRejected, err)
		}
		if !report.Passed {
			return document, report, fail(ErrRejected)
		}
	}
	if err := canceled(ctx); err != nil {
		return inspection{}, wf.Report{}, err
	}
	return document, report, nil
}

func validName(name string) bool {
	if len(name) == 0 || len(name) > 64 || name[0] < 'a' || name[0] > 'z' {
		return false
	}
	for _, char := range name {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' {
			return false
		}
	}
	switch name {
	case "con", "prn", "aux", "nul", "vendor", "testdata":
		return false
	}
	return !(len(name) == 4 && (strings.HasPrefix(name, "com") || strings.HasPrefix(name, "lpt")) && name[3] >= '1' && name[3] <= '9')
}

func writeAll(writer io.Writer, raw []byte) error {
	count, err := writer.Write(raw)
	if err != nil {
		return fail(ErrOutput, err)
	}
	if count != len(raw) {
		return fail(ErrOutput, io.ErrShortWrite)
	}
	return nil
}
