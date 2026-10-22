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

package declaration

import (
	"bytes"
	"context"
	"slices"
)

// MaxConflictPaths bounds retained paths per conflicting identity.
const MaxConflictPaths = 16

// Conflict explains a same-identity content difference without exposing values.
type Conflict struct {
	Identity       Identity `json:"identity"`
	Profile        string   `json:"profile"`
	Fingerprints   []string `json:"fingerprints"`
	Paths          []string `json:"paths"`
	PathsTruncated bool     `json:"paths_truncated"`
}

// InputDiagnostic binds a completed invalid document to its zero-based explicit
// input position. It contains no filename, raw bytes or native error.
type InputDiagnostic struct {
	Input      int        `json:"input"`
	Diagnostic Diagnostic `json:"diagnostic"`
}

// Report describes only supplied snapshots. Entries lists consistent identities;
// modes under conflicting workflow specifications are counted but not compared.
// CLI diagnostics identify rejected documents; counts cover validated inputs only.
// Passed does not certify freshness, complete project export or runtime readiness.
type Report struct {
	Passed          bool              `json:"passed"`
	Inputs          int               `json:"inputs"`
	Workflows       int               `json:"workflows"`
	Modes           int               `json:"modes"`
	UncomparedModes int               `json:"uncompared_modes"`
	Entries         []Entry           `json:"entries"`
	Conflicts       []Conflict        `json:"conflicts"`
	Diagnostics     []InputDiagnostic `json:"diagnostics"`
	Truncated       bool              `json:"truncated"`
}

// Check compares complete validated bundles by canonical content, not only hashes.
// Different mode subsets under an equal parent may coexist. Input ordering does
// not change comparison. Cancellation/errors return no completed report.
func Check(ctx context.Context, bundles ...*Bundle) (Report, error) {
	if err := canceled(ctx); err != nil {
		return Report{}, err
	}
	if len(bundles) == 0 {
		return Report{}, problem(ErrHandle, "input_set", "/")
	}
	if len(bundles) > MaxInputs {
		return Report{}, problem(ErrLimit, "inputs", "/")
	}
	total := 0
	groups := map[Identity][]compiledEntry{}
	for _, bundle := range bundles {
		if err := canceled(ctx); err != nil {
			return Report{}, err
		}
		if bundle == nil || !bundle.valid {
			return Report{}, problem(ErrHandle, "bundle", "/")
		}
		if len(bundle.raw) > MaxInputBytes-total {
			return Report{}, problem(ErrLimit, "bytes", "/")
		}
		total += len(bundle.raw)
		for _, entry := range bundle.entries {
			groups[entry.Identity] = append(groups[entry.Identity], entry)
		}
	}
	identities := make([]Identity, 0, len(groups))
	for identity := range groups {
		identities = append(identities, identity)
	}
	slices.SortFunc(identities, compareIdentity)
	report := Report{Passed: true, Inputs: len(bundles), Entries: []Entry{}, Conflicts: []Conflict{}, Diagnostics: []InputDiagnostic{}}
	conflictingParents := map[Identity]bool{}
	for _, identity := range identities {
		if err := canceled(ctx); err != nil {
			return Report{}, err
		}
		if identity.Mode == "" {
			report.Workflows++
		} else {
			report.Modes++
			parent := identity
			parent.Mode = ""
			parent.Revision = ""
			if conflictingParents[parent] {
				report.UncomparedModes++
				continue
			}
		}
		members := groups[identity]
		slices.SortFunc(members, func(a, b compiledEntry) int { return bytes.Compare(a.content, b.content) })
		distinct := members[:0]
		for _, member := range members {
			if len(distinct) == 0 || !bytes.Equal(distinct[len(distinct)-1].content, member.content) {
				distinct = append(distinct, member)
			}
		}
		if len(distinct) == 1 {
			report.Entries = append(report.Entries, distinct[0].Entry)
			continue
		}
		report.Passed = false
		if identity.Mode == "" {
			conflictingParents[identity] = true
		}
		if len(report.Conflicts) == MaxDiagnostics {
			report.Truncated = true
			continue
		}
		conflict := Conflict{Identity: identity, Profile: distinct[0].Profile, Fingerprints: []string{}, Paths: []string{}}
		prefix := "/spec"
		maximum := MaxSpecBytes
		if identity.Mode != "" {
			prefix = "/config"
			maximum = MaxModeBytes
		}
		first, err := readJSON(ctx, distinct[0].content, maximum, MaxInnerDepth)
		if err != nil {
			return Report{}, err
		}
		pathSet := map[string]bool{}
		for index, member := range distinct {
			conflict.Fingerprints = append(conflict.Fingerprints, member.SHA256)
			if index == 0 {
				continue
			}
			next, err := readJSON(ctx, member.content, maximum, MaxInnerDepth)
			if err != nil {
				return Report{}, err
			}
			if err := differentPaths(ctx, first, next, prefix, pathSet, &conflict.PathsTruncated); err != nil {
				return Report{}, err
			}
		}
		conflict.Paths = keys(pathSet)
		slices.Sort(conflict.Fingerprints)
		conflict.Fingerprints = slices.Compact(conflict.Fingerprints)
		report.Conflicts = append(report.Conflicts, conflict)
	}
	if err := canceled(ctx); err != nil {
		return Report{}, err
	}
	return report, nil
}

func differentPaths(ctx context.Context, left, right any, path string, paths map[string]bool, truncated *bool) error {
	if err := canceled(ctx); err != nil {
		return err
	}
	add := func() {
		if paths[path] {
			return
		}
		if len(paths) == MaxConflictPaths {
			*truncated = true
		} else {
			paths[path] = true
		}
	}
	if *truncated {
		return nil
	}
	switch left := left.(type) {
	case map[string]any:
		right, ok := right.(map[string]any)
		if !ok {
			add()
			return nil
		}
		names := map[string]bool{}
		for name := range left {
			names[name] = true
		}
		for name := range right {
			names[name] = true
		}
		for _, name := range keys(names) {
			leftValue, leftOK := left[name]
			rightValue, rightOK := right[name]
			child := at(path, name)
			if !leftOK || !rightOK {
				if paths[child] {
					continue
				}
				if len(paths) == MaxConflictPaths {
					*truncated = true
					return nil
				}
				paths[child] = true
				continue
			}
			if err := differentPaths(ctx, leftValue, rightValue, child, paths, truncated); err != nil {
				return err
			}
			if *truncated {
				return nil
			}
		}
	case []any:
		right, ok := right.([]any)
		if !ok || len(left) != len(right) {
			add()
			return nil
		}
		for index := range left {
			if err := differentPaths(ctx, left[index], right[index], indexPath(path, index), paths, truncated); err != nil {
				return err
			}
			if *truncated {
				return nil
			}
		}
	default:
		switch right.(type) {
		case map[string]any, []any:
			add()
		default:
			if left != right {
				add()
			}
		}
	}
	return nil
}
