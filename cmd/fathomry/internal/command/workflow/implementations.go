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

package workflow

import (
	"context"
	"slices"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	implementation "github.com/frost-leo/fathomry/framework/workflow/implementation/v1"
)

// ImplementationReport separates declaration scope, supplied entries and checked
// coverage. Not-checked counters are not evidence of an empty required set.
// Manifest is retained only by full artifact inspection, not ordinary vet output.
type ImplementationReport struct {
	Identity          declaration.Identity       `json:"identity"`
	Selected          bool                       `json:"selected"`
	Supplied          bool                       `json:"supplied"`
	Status            string                     `json:"status"`
	RequirementsKnown bool                       `json:"requirements_known"`
	RequiredBindings  int                        `json:"required_bindings"`
	SuppliedBindings  int                        `json:"supplied_bindings"`
	CallSites         int                        `json:"call_sites"`
	Diagnostic        *implementation.Diagnostic `json:"diagnostic,omitempty"`
	Manifest          *implementation.Manifest   `json:"manifest,omitempty"`
}

func implementationStructure(source declaration.Declaration, sets []implementation.VersionBindings) error {
	if len(sets) > declaration.MaxWorkflows {
		return command.Fail(command.ErrLimit)
	}
	if len(sets) == 0 {
		return nil
	}
	versions := map[string]declaration.WorkflowDeclaration{}
	for _, version := range source.Workflows {
		if _, exists := versions[version.Version]; exists {
			return command.Fail(command.ErrUsage)
		}
		versions[version.Version] = version
	}
	seen := map[string]bool{}
	for _, set := range sets {
		if set.Version == "" || len(set.Version) > 128 {
			return command.Fail(command.ErrUsage)
		}
		version, exists := versions[set.Version]
		if !exists || seen[set.Version] {
			return command.Fail(command.ErrUsage)
		}
		seen[set.Version] = true
		if len(set.Bindings) > 4096 || len(version.Spec.Bindings) > 4096 {
			return command.Fail(command.ErrLimit)
		}
		bindings := map[string]bool{}
		for _, binding := range set.Bindings {
			if binding.Key == "" || len(binding.Key) > 64 {
				return command.Fail(command.ErrUsage)
			}
			if _, exists := version.Spec.Bindings[binding.Key]; !exists || bindings[binding.Key] {
				return command.Fail(command.ErrUsage)
			}
			bindings[binding.Key] = true
		}
	}
	return nil
}

func allVersions(group projectGroup) []string {
	result := []string{}
	for _, version := range group.originals {
		if !slices.Contains(result, version.Version) {
			result = append(result, version.Version)
		}
	}
	slices.Sort(result)
	return result
}

func checkImplementations(ctx context.Context, group projectGroup, checked *groupAnalysis, keep bool) error {
	result := &checked.report
	result.Implementations = []ImplementationReport{}
	sets := map[string][]implementation.Binding{}
	for _, set := range group.implementations {
		sets[set.Version] = set.Bindings
	}
	selectedVersions := map[string]bool{}
	for _, part := range group.parts {
		for _, version := range part.Workflows {
			selectedVersions[version.Version] = true
		}
	}
	for _, version := range allVersions(group) {
		if err := projectContext(ctx); err != nil {
			return err
		}
		bindings, supplied := sets[version]
		selected := selectedVersions[version]
		identity := declaration.Identity{Namespace: result.Namespace, Code: result.Code, Version: displayIdentity(version, 128)}
		item := ImplementationReport{Identity: identity, Selected: selected, Supplied: supplied, SuppliedBindings: len(bindings), Status: "not_checked"}
		if !selected || !result.Declaration.Passed {
			result.Implementations = append(result.Implementations, item)
			continue
		}
		var bundle *declaration.Bundle
		for _, part := range checked.parts {
			if part.bundle == nil {
				continue
			}
			_, found, err := part.bundle.ContentCopy(item.Identity)
			if err != nil {
				return err
			}
			if found {
				bundle = part.bundle
			}
			if bundle != nil {
				break
			}
		}
		if bundle == nil {
			return command.Fail(command.ErrCheck)
		}
		requirements, err := implementation.Describe(ctx, bundle, item.Identity)
		if err != nil {
			return err
		}
		item.RequirementsKnown = true
		item.RequiredBindings = requirements.Bindings
		item.CallSites = requirements.Calls
		if !supplied {
			item.Status = "not_supplied"
			result.Implementations = append(result.Implementations, item)
			continue
		}
		registry, err := implementation.Prepare(ctx, bundle, item.Identity, bindings)
		if err != nil {
			if problem := projectContext(ctx); problem != nil {
				return command.Fail(command.ErrCanceled, problem, err)
			}
			diagnostic, ok := implementation.Diagnose(err)
			if !ok {
				return err
			}
			item.Status = "failed"
			item.Diagnostic = &diagnostic
			result.Passed = false
		} else {
			item.Status = "complete"
			if keep {
				manifest, err := registry.ManifestCopy()
				if err != nil {
					return err
				}
				item.Manifest = &manifest
			}
		}
		result.Implementations = append(result.Implementations, item)
	}
	return nil
}
