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

package catalog

import "github.com/frost-leo/fathomry/failure/v1"

// Stable identities classify catalog failures, not retry or mutation outcome.
const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowCatalog)<<16 | 0x0001
	ErrMetadata      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowCatalog)<<16 | 0x0002
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowCatalog)<<16 | 0x0003
	ErrCanceled      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowCatalog)<<16 | 0x0004
	ErrHandle        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowCatalog)<<16 | 0x0005
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowCatalog)<<16 | 0x0006
	ErrConflict      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowCatalog)<<16 | 0x0007
	ErrDatabase      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowCatalog)<<16 | 0x0008
	ErrCategory      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowCatalog)<<16 | 0x0009
	ErrStored        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowCatalog)<<16 | 0x000a
	ErrSchema        failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowCatalog)<<16 | 0x000b
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowCatalog)<<16 | 0x000c
	ErrCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowCatalog)<<16 | 0x000d
	ErrMissing       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityWorkflowCatalog)<<16 | 0x000e
)

// Diagnostic contains fixed reason tokens and bounded numeric paths only.
// Payloads, target names, resource keys and tag values never enter diagnostics.
type Diagnostic struct {
	Code   failure.Code `json:"code"`
	Reason string       `json:"reason"`
	Path   string       `json:"path,omitempty"`
}

// Diagnose inspects this direct catalog occurrence, not arbitrary nested causes.
func Diagnose(err error) (Diagnostic, bool) {
	value, ok := err.(*failure.Detailed[Diagnostic])
	if !ok || value == nil {
		return Diagnostic{}, false
	}
	return value.Details()
}

func problem(code failure.Code, reason, path string, causes ...error) error {
	return problemAt(code, "catalog.prepare", reason, path, causes...)
}

func problemAt(code failure.Code, operation, reason, path string, causes ...error) error {
	for _, definition := range Definitions() {
		if definition.Code == code {
			value, err := failure.NewDetailed(definition, failure.Location{Operation: operation},
				Diagnostic{Code: code, Reason: reason, Path: path}, func(value Diagnostic) Diagnostic { return value }, causes...)
			if err != nil {
				return err
			}
			return value
		}
	}
	return failure.ErrDefinition
}

// Definitions returns detached first-party catalog publication metadata.
func Definitions() []failure.Definition {
	return []failure.Definition{
		{Code: ErrInput, Identifier: "fathomry.workflow_catalog.invalid_registration", Module: "fathomry", Component: "workflow_catalog", Revision: 1, Details: failure.Contract{ID: "fathomry.workflow_catalog.diagnostic", Version: 1}, Message: "The catalog request is invalid."},
		{Code: ErrMetadata, Identifier: "fathomry.workflow_catalog.invalid_metadata", Module: "fathomry", Component: "workflow_catalog", Revision: 1, Details: failure.Contract{ID: "fathomry.workflow_catalog.diagnostic", Version: 1}, Message: "The catalog display or initial metadata is invalid."},
		{Code: ErrLimit, Identifier: "fathomry.workflow_catalog.limit_exceeded", Module: "fathomry", Component: "workflow_catalog", Revision: 1, Details: failure.Contract{ID: "fathomry.workflow_catalog.diagnostic", Version: 1}, Message: "A catalog operation limit was exceeded."},
		{Code: ErrCanceled, Identifier: "fathomry.workflow_catalog.canceled", Module: "fathomry", Component: "workflow_catalog", Revision: 1, Details: failure.Contract{ID: "fathomry.workflow_catalog.diagnostic", Version: 1}, Message: "The catalog operation was canceled."},
		{Code: ErrHandle, Identifier: "fathomry.workflow_catalog.invalid_prepared", Module: "fathomry", Component: "workflow_catalog", Revision: 1, Details: failure.Contract{ID: "fathomry.workflow_catalog.diagnostic", Version: 1}, Message: "The prepared catalog registration is invalid."},
		{Code: ErrSerialization, Identifier: "fathomry.workflow_catalog.serialization_unsupported", Module: "fathomry", Component: "workflow_catalog", Revision: 1, Details: failure.Contract{ID: "fathomry.workflow_catalog.diagnostic", Version: 1}, Message: "In-process catalog handles cannot be serialized."},
		{Code: ErrConflict, Identifier: "fathomry.workflow_catalog.registration_conflict", Module: "fathomry", Component: "workflow_catalog", Revision: 1, Details: failure.Contract{ID: "fathomry.workflow_catalog.diagnostic", Version: 1}, Message: "Registered immutable content or selected publication metadata conflicts."},
		{Code: ErrDatabase, Identifier: "fathomry.workflow_catalog.database_failed", Module: "fathomry", Component: "workflow_catalog", Revision: 1, Details: failure.Contract{ID: "fathomry.workflow_catalog.diagnostic", Version: 1}, Message: "The catalog database operation failed; transaction effects remain separate."},
		{Code: ErrCategory, Identifier: "fathomry.workflow_catalog.category_unavailable", Module: "fathomry", Component: "workflow_catalog", Revision: 1, Details: failure.Contract{ID: "fathomry.workflow_catalog.diagnostic", Version: 1}, Message: "The initial category is missing or does not allow new associations."},
		{Code: ErrStored, Identifier: "fathomry.workflow_catalog.stored_content_invalid", Module: "fathomry", Component: "workflow_catalog", Revision: 1, Details: failure.Contract{ID: "fathomry.workflow_catalog.diagnostic", Version: 1}, Message: "Stored catalog content is unsupported, corrupt or incomplete."},
		{Code: ErrSchema, Identifier: "fathomry.workflow_catalog.schema_refused", Module: "fathomry", Component: "workflow_catalog", Revision: 1, Details: failure.Contract{ID: "fathomry.workflow_catalog.diagnostic", Version: 1}, Message: "The retained database transaction does not prove a compatible current catalog schema."},
		{Code: ErrState, Identifier: "fathomry.workflow_catalog.operation_unavailable", Module: "fathomry", Component: "workflow_catalog", Revision: 1, Details: failure.Contract{ID: "fathomry.workflow_catalog.diagnostic", Version: 1}, Message: "The catalog operation is unavailable in its current state."},
		{Code: ErrCleanup, Identifier: "fathomry.workflow_catalog.cleanup_incomplete", Module: "fathomry", Component: "workflow_catalog", Revision: 1, Details: failure.Contract{ID: "fathomry.workflow_catalog.diagnostic", Version: 1}, Message: "Catalog cleanup has not completed successfully; retain the original owner."},
		{Code: ErrMissing, Identifier: "fathomry.workflow_catalog.identity_missing", Module: "fathomry", Component: "workflow_catalog", Revision: 1, Details: failure.Contract{ID: "fathomry.workflow_catalog.diagnostic", Version: 1}, Message: "The requested catalog identity does not exist."},
	}
}
