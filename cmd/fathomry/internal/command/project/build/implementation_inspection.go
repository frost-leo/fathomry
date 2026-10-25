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

	workflows "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow"
	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	implementation "github.com/frost-leo/fathomry/framework/workflow/implementation/v1"
)

// ImplementationInspectionArgument selects explicit binding-aware inspection.
const ImplementationInspectionArgument = "--fathomry-project=v2"
const implementationInspectionSchema = "fathomry.project-inspection/v2"
const maxImplementationInspectionBytes = declaration.MaxInputBytes + implementation.MaxManifestBytes + 4096

type implementationList []workflows.ImplementationReport
type inspectionV2 struct {
	Schema          string             `json:"schema"`
	Name            string             `json:"name"`
	Bundles         bundleList         `json:"bundles"`
	Implementations implementationList `json:"implementations"`
}

func (values *implementationList) UnmarshalJSONFrom(decoder *jsontext.Decoder) error {
	raw, err := decoder.ReadValue()
	if err != nil {
		return fail(ErrInspection, err)
	}
	if len(raw) > implementation.MaxManifestBytes {
		return fail(ErrLimit)
	}
	decoder = jsontext.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.ReadToken()
	if err != nil || token.Kind() != '[' {
		return fail(ErrInspection, err)
	}
	result := implementationList{}
	for decoder.PeekKind() != ']' {
		if len(result) >= declaration.MaxInputs*declaration.MaxWorkflows {
			return fail(ErrLimit)
		}
		raw, err := decoder.ReadValue()
		if err != nil {
			return fail(ErrInspection, err)
		}
		var record workflows.ImplementationReport
		if err := json.Unmarshal(raw, &record, json.RejectUnknownMembers(true)); err != nil {
			return fail(ErrInspection, err)
		}
		result = append(result, record)
	}
	if _, err = decoder.ReadToken(); err != nil {
		return fail(ErrInspection, err)
	}
	*values = result
	return nil
}

// ExportImplementations emits v2 only after full project checking. It does not
// run handlers, Boot or services; data-only versions remain explicitly unbound.
func ExportImplementations(ctx context.Context, definition Definition, output io.Writer) error {
	if err := canceled(ctx); err != nil {
		return err
	}
	if output == nil || !validName(definition.Name) {
		return fail(ErrInspection)
	}
	document := inspectionV2{Schema: implementationInspectionSchema, Name: definition.Name, Bundles: bundleList{}, Implementations: implementationList{}}
	if len(definition.Workflows) > 0 || len(definition.Declarations) > 0 {
		report, bundles, err := workflows.CheckProject(ctx, workflows.ProjectInput{Workflows: definition.Workflows, Declarations: definition.Declarations}, workflows.Selection{})
		if err != nil {
			return phaseError(ctx, ErrRejected, err)
		}
		if !report.Passed || !report.FullProject || report.Empty {
			return fail(ErrRejected)
		}
		for _, bundle := range bundles {
			raw, err := bundle.BytesCopy()
			if err != nil {
				return phaseError(ctx, ErrInspection, err)
			}
			document.Bundles = append(document.Bundles, jsontext.Value(raw))
		}
		for _, group := range report.Workflows {
			document.Implementations = append(document.Implementations, group.Implementations...)
		}
	}
	manifestBytes, err := json.Marshal(document.Implementations)
	if err != nil {
		return fail(ErrInspection, err)
	}
	if len(manifestBytes) > implementation.MaxManifestBytes {
		return fail(ErrLimit)
	}
	raw, err := json.Marshal(document)
	if err != nil {
		return fail(ErrInspection, err)
	}
	if len(raw) > maxImplementationInspectionBytes {
		return fail(ErrLimit)
	}
	if err := canceled(ctx); err != nil {
		return err
	}
	return writeAll(output, raw)
}

func inspectImplementations(ctx context.Context, raw []byte) (inspection, declaration.Report, []workflows.ImplementationReport, error) {
	if err := canceled(ctx); err != nil {
		return inspection{}, declaration.Report{}, nil, err
	}
	if len(raw) > maxImplementationInspectionBytes {
		return inspection{}, declaration.Report{}, nil, fail(ErrLimit)
	}
	var value inspectionV2
	if err := json.Unmarshal(raw, &value, json.RejectUnknownMembers(true)); err != nil {
		if errors.Is(err, ErrLimit) || errors.Is(err, implementation.ErrLimit) {
			return inspection{}, declaration.Report{}, nil, fail(ErrLimit, err)
		}
		return inspection{}, declaration.Report{}, nil, fail(ErrInspection, err)
	}
	if value.Schema != implementationInspectionSchema || value.Implementations == nil {
		return inspection{}, declaration.Report{}, nil, fail(ErrInspection)
	}
	document := inspection{Schema: value.Schema, Name: value.Name, Bundles: value.Bundles}
	report, bundles, err := checkInspectionDocument(ctx, document)
	if err != nil {
		return inspection{}, declaration.Report{}, nil, err
	}
	manifestBytes, err := json.Marshal(value.Implementations)
	if err != nil || len(manifestBytes) > implementation.MaxManifestBytes {
		return inspection{}, declaration.Report{}, nil, fail(ErrLimit, err)
	}
	expected := map[declaration.Identity]*declaration.Bundle{}
	for _, bundle := range bundles {
		contents, err := bundle.ContentsCopy()
		if err != nil {
			return inspection{}, declaration.Report{}, nil, fail(ErrInspection, err)
		}
		for _, content := range contents {
			if content.Entry.Identity.Mode == "" {
				expected[content.Entry.Identity] = bundle
			}
		}
	}
	if len(value.Implementations) != len(expected) {
		return inspection{}, declaration.Report{}, nil, fail(ErrInspection)
	}
	seen := map[declaration.Identity]bool{}
	for _, record := range value.Implementations {
		if err := canceled(ctx); err != nil {
			return inspection{}, declaration.Report{}, nil, err
		}
		bundle, exists := expected[record.Identity]
		if !exists || seen[record.Identity] || !record.Selected || !record.RequirementsKnown || record.Diagnostic != nil {
			return inspection{}, declaration.Report{}, nil, fail(ErrInspection)
		}
		seen[record.Identity] = true
		requirement, err := implementation.Describe(ctx, bundle, record.Identity)
		if err != nil {
			return inspection{}, declaration.Report{}, nil, fail(ErrInspection, err)
		}
		if record.RequiredBindings != requirement.Bindings || record.CallSites != requirement.Calls {
			return inspection{}, declaration.Report{}, nil, fail(ErrInspection)
		}
		switch record.Status {
		case "not_supplied":
			if record.Supplied || record.SuppliedBindings != 0 || record.Manifest != nil {
				return inspection{}, declaration.Report{}, nil, fail(ErrInspection)
			}
		case "complete":
			if !record.Supplied || record.SuppliedBindings != requirement.Bindings || record.Manifest == nil {
				return inspection{}, declaration.Report{}, nil, fail(ErrInspection)
			}
			if record.Manifest.Specification.Identity != record.Identity {
				return inspection{}, declaration.Report{}, nil, fail(ErrInspection)
			}
			if err := implementation.ValidateManifest(ctx, bundle, *record.Manifest); err != nil {
				return inspection{}, declaration.Report{}, nil, phaseError(ctx, ErrInspection, err)
			}
		default:
			return inspection{}, declaration.Report{}, nil, fail(ErrInspection)
		}
	}
	return document, report, value.Implementations, nil
}
