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
	"crypto/sha256"
	"encoding/hex"
	json "encoding/json/v2"
	"fmt"
	"log/slog"
	"slices"
)

// Identity is a portable workflow tuple, optionally extended by a mode/revision.
// Labels are opaque; sorting this value never infers version compatibility.
type Identity struct {
	Namespace string `json:"namespace"`
	Code      string `json:"code"`
	Version   string `json:"version"`
	Mode      string `json:"mode,omitempty"`
	Revision  string `json:"revision,omitempty"`
}

// Entry exposes identity and recomputed fingerprint, not mutable content.
type Entry struct {
	Identity Identity `json:"identity"`
	Profile  string   `json:"profile"`
	SHA256   string   `json:"sha256"`
}

type compiledEntry struct {
	Entry
	content []byte
}

type wireSpec struct {
	Format    string `json:"format"`
	Semantics string `json:"semantics"`
	Specification
}
type wireConfig struct {
	Format string             `json:"format"`
	Values map[string]Decimal `json:"values"`
}
type wireMode struct {
	Code     string     `json:"code"`
	Revision string     `json:"revision"`
	Profile  string     `json:"fingerprint_profile"`
	Digest   string     `json:"config_sha256"`
	Config   wireConfig `json:"config"`
}
type wireWorkflow struct {
	Namespace string     `json:"namespace"`
	Code      string     `json:"code"`
	Version   string     `json:"version"`
	Profile   string     `json:"fingerprint_profile"`
	Digest    string     `json:"spec_sha256"`
	Spec      wireSpec   `json:"spec"`
	Modes     []wireMode `json:"modes"`
}
type wireBundle struct {
	Format    string         `json:"format"`
	Workflows []wireWorkflow `json:"workflows"`
}

// Bundle is an immutable validated snapshot, constructed only by Compile or Parse.
// Its zero value is invalid. Copying the handle shares immutable state, not a
// mutable view. No caller-owned data is retained after construction returns.
// Do not overwrite a handle while another goroutine is using it.
type Bundle struct {
	valid   bool
	raw     []byte
	entries []compiledEntry
}

// BytesCopy returns a detached canonical bundle, including recomputed digests.
func (bundle *Bundle) BytesCopy() ([]byte, error) {
	if bundle == nil || !bundle.valid {
		return nil, problem(ErrHandle, "bundle", "/")
	}
	return bytes.Clone(bundle.raw), nil
}

// MarshalJSON exports a valid snapshot, including a copied handle. Standard JSON
// encoders encode nil pointers as null; use BytesCopy to reject nil handles.
// Decoding must use Parse with context.
func (bundle Bundle) MarshalJSON() ([]byte, error) { return bundle.BytesCopy() }

// UnmarshalJSON refuses mutation of an existing shared immutable snapshot.
func (*Bundle) UnmarshalJSON([]byte) error { return problem(ErrHandle, "parse_required", "/") }

// Format deliberately does not render declaration contents.
func (Bundle) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("workflow.Bundle")) }

// LogValue deliberately does not log declaration contents.
func (Bundle) LogValue() slog.Value { return slog.StringValue("workflow.Bundle") }

// Compile validates caller-owned concrete declarations and materializes only
// permitted empty collections and fixed control defaults. Do not mutate input
// until this call returns. Failures never return a partially valid bundle.
func Compile(ctx context.Context, declaration Declaration) (*Bundle, error) {
	if err := canceled(ctx); err != nil {
		return nil, err
	}
	if len(declaration.Workflows) > MaxWorkflows {
		return nil, problem(ErrLimit, "workflows", "/")
	}
	if err := boundAuthoring(ctx, declaration); err != nil {
		return nil, err
	}
	document := wireBundle{Format: BundleFormat}
	totalModes := 0
	for _, input := range declaration.Workflows {
		totalModes += len(input.Modes)
		if len(input.Modes) > MaxModesPerWorkflow || totalModes > MaxModes {
			return nil, problem(ErrLimit, "modes", "/")
		}
		workflow := wireWorkflow{Namespace: input.Namespace, Code: input.Code, Version: input.Version,
			Profile: SpecProfile, Spec: wireSpec{Format: SpecFormat, Semantics: Semantics, Specification: input.Spec}}
		for _, mode := range input.Modes {
			workflow.Modes = append(workflow.Modes, wireMode{Code: mode.Code, Revision: mode.Revision, Profile: ModeProfile,
				Config: wireConfig{Format: ModeFormat, Values: mode.Values}})
		}
		document.Workflows = append(document.Workflows, workflow)
	}
	raw, err := marshal(ctx, document, MaxBundleBytes)
	if err != nil {
		return nil, err
	}
	return prepareBundle(ctx, raw, true)
}

// Parse borrows input only until return and verifies a complete self-contained
// bundle. It never supplies missing wire fields/mode values or trusts a producer
// digest. Whitespace and declared set ordering may normalize; provenance is unknown.
func Parse(ctx context.Context, input []byte) (*Bundle, error) {
	return prepareBundle(ctx, input, false)
}

func prepareBundle(ctx context.Context, raw []byte, authoring bool) (*Bundle, error) {
	tree, err := readJSON(ctx, raw, MaxBundleBytes, MaxBundleDepth)
	if err != nil {
		return nil, err
	}
	if err := (validator{ctx: ctx, authoring: authoring}).shape(tree); err != nil {
		return nil, err
	}
	normalized, err := marshal(ctx, tree, MaxBundleBytes)
	if err != nil {
		return nil, err
	}
	var document wireBundle
	if err := json.Unmarshal(normalized, &document, json.RejectUnknownMembers(true)); err != nil {
		return nil, problem(ErrInvalid, "shape", "/", err)
	}
	bundle := &Bundle{}
	identities := map[Identity]bool{}
	contentBytes := 0
	for index := range document.Workflows {
		if err := canceled(ctx); err != nil {
			return nil, err
		}
		workflow := &document.Workflows[index]
		path := indexPath("/workflows", index)
		identity := workflowIdentity(*workflow)
		if identities[identity] {
			return nil, invalid("duplicate_identity", path)
		}
		identities[identity] = true
		if err := normalizeGraph(ctx, &workflow.Spec.Specification, at(path, "spec")); err != nil {
			return nil, err
		}
		if err := normalizeControls(ctx, &workflow.Spec.Specification, at(path, "spec")); err != nil {
			return nil, err
		}
		specBytes, err := canonical(ctx, workflow.Spec, MaxSpecBytes)
		if err != nil {
			return nil, err
		}
		if _, err := readJSON(ctx, specBytes, MaxSpecBytes, MaxInnerDepth); err != nil {
			return nil, err
		}
		if len(specBytes) > MaxBundleBytes-contentBytes {
			return nil, problem(ErrLimit, "bytes", path)
		}
		contentBytes += len(specBytes)
		digest := fingerprint(SpecProfile, specBytes)
		if !authoring && workflow.Digest != digest {
			return nil, problem(ErrIntegrity, "digest_mismatch", at(path, "spec_sha256"))
		}
		workflow.Digest = digest
		bundle.entries = append(bundle.entries, compiledEntry{Entry: Entry{Identity: identity, Profile: SpecProfile, SHA256: digest}, content: specBytes})
		for modeIndex := range workflow.Modes {
			mode := &workflow.Modes[modeIndex]
			modePath := indexPath(at(path, "modes"), modeIndex)
			modeIdentity := identity
			modeIdentity.Mode = mode.Code
			modeIdentity.Revision = mode.Revision
			if identities[modeIdentity] {
				return nil, invalid("duplicate_identity", modePath)
			}
			identities[modeIdentity] = true
			values, err := normalizeMode(ctx, &workflow.Spec.Specification, mode.Config.Values, authoring, at(modePath, "config"))
			if err != nil {
				return nil, err
			}
			mode.Config.Values = values
			modeBytes, err := canonical(ctx, mode.Config, MaxModeBytes)
			if err != nil {
				return nil, err
			}
			if len(modeBytes) > MaxBundleBytes-contentBytes {
				return nil, problem(ErrLimit, "bytes", modePath)
			}
			contentBytes += len(modeBytes)
			digest := fingerprint(ModeProfile, modeBytes)
			if !authoring && mode.Digest != digest {
				return nil, problem(ErrIntegrity, "digest_mismatch", at(modePath, "config_sha256"))
			}
			mode.Digest = digest
			bundle.entries = append(bundle.entries, compiledEntry{Entry: Entry{Identity: modeIdentity, Profile: ModeProfile, SHA256: digest}, content: modeBytes})
		}
		slices.SortFunc(workflow.Modes, func(a, b wireMode) int {
			if result := compareText(a.Code, b.Code); result != 0 {
				return result
			}
			return compareText(a.Revision, b.Revision)
		})
	}
	slices.SortFunc(document.Workflows, func(a, b wireWorkflow) int { return compareIdentity(workflowIdentity(a), workflowIdentity(b)) })
	slices.SortFunc(bundle.entries, func(a, b compiledEntry) int { return compareIdentity(a.Identity, b.Identity) })
	bundle.raw, err = canonical(ctx, document, MaxBundleBytes)
	if err != nil {
		return nil, err
	}
	if err := boundJSONValues(ctx, bundle.raw); err != nil {
		return nil, err
	}
	bundle.valid = true
	return bundle, nil
}

func fingerprint(profile string, content []byte) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(profile))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(content)
	return hex.EncodeToString(hash.Sum(nil))
}

func workflowIdentity(workflow wireWorkflow) Identity {
	return Identity{Namespace: workflow.Namespace, Code: workflow.Code, Version: workflow.Version}
}

func compareIdentity(left, right Identity) int {
	leftParts := [5]string{left.Namespace, left.Code, left.Version, left.Mode, left.Revision}
	rightParts := [5]string{right.Namespace, right.Code, right.Version, right.Mode, right.Revision}
	for index := range leftParts {
		if result := compareText(leftParts[index], rightParts[index]); result != 0 {
			return result
		}
	}
	return 0
}
