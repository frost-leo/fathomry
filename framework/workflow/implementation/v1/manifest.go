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

package implementation

import (
	"context"
	json "encoding/json/v2"
	"reflect"
	"slices"
	"strconv"
	"strings"

	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

// Format and ABIProfile identify data/shape contracts, not business or SDK versions.
const (
	Format           = "fathomry.workflow-implementations/v1"
	ABIProfile       = "fathomry.required-json-values/v1"
	MaxManifestBytes = 16 << 20
	MaxTypeDepth     = 32
	MaxTypeVisits    = 1 << 20
	MaxTypeMethods   = 256
)

// BindingInfo identifies one unique implementation obligation, not its Go address.
type BindingInfo struct {
	Key      string `json:"key"`
	Type     string `json:"type"`
	Revision string `json:"revision"`
}

// CallInfo identifies a declaration call site. Several calls can share one binding.
type CallInfo struct {
	Node     string `json:"node"`
	Activity string `json:"activity"`
	Binding  string `json:"binding"`
}

// Manifest is data-only complete coverage for one exact specification. Its identity
// and descriptors may be sensitive; it contains no callables or runtime values.
type Manifest struct {
	Format        string            `json:"format"`
	ABIProfile    string            `json:"abi_profile"`
	Specification declaration.Entry `json:"specification"`
	Bindings      []BindingInfo     `json:"bindings"`
	Calls         []CallInfo        `json:"calls"`
}

// Requirements describes obligations without admitting Go shapes or certifying coverage.
type Requirements struct {
	Specification declaration.Entry
	Bindings      int
	Calls         int
}

type projection struct {
	manifest Manifest
	spec     declaration.Specification
}

// Describe reads a verified version's requirements. Unsupported Go ABI features
// can still be described by declaration-only tools.
func Describe(ctx context.Context, bundle *declaration.Bundle, identity declaration.Identity) (Requirements, error) {
	value, err := project(ctx, bundle, identity)
	if err != nil {
		return Requirements{}, err
	}
	return Requirements{Specification: value.manifest.Specification, Bindings: len(value.manifest.Bindings), Calls: len(value.manifest.Calls)}, nil
}

func project(ctx context.Context, bundle *declaration.Bundle, identity declaration.Identity) (projection, error) {
	if err := canceled(ctx); err != nil {
		return projection{}, err
	}
	if identity.Mode != "" || identity.Revision != "" {
		return projection{}, problem(ErrInvalid, "version_identity", "")
	}
	content, found, err := bundle.ContentCopy(identity)
	if err != nil {
		return projection{}, problem(ErrHandle, "bundle", "", err)
	}
	if found {
		if err := canceled(ctx); err != nil {
			return projection{}, err
		}
		var wire struct {
			Format    string `json:"format"`
			Semantics string `json:"semantics"`
			declaration.Specification
		}
		if err := json.Unmarshal(content.Bytes, &wire, json.RejectUnknownMembers(true)); err != nil {
			return projection{}, problem(ErrInvalid, "projection", "", err)
		}
		result := projection{spec: wire.Specification, manifest: Manifest{Format: Format, ABIProfile: ABIProfile, Specification: content.Entry, Bindings: []BindingInfo{}, Calls: []CallInfo{}}}
		for _, key := range sortedKeys(wire.Bindings) {
			value := wire.Bindings[key]
			result.manifest.Bindings = append(result.manifest.Bindings, BindingInfo{key, value.Type, value.Revision})
		}
		for _, name := range sortedKeys(wire.Nodes) {
			for _, call := range sortedKeys(wire.Nodes[name].Activities) {
				result.manifest.Calls = append(result.manifest.Calls, CallInfo{name, call, wire.Nodes[name].Activities[call].Binding})
			}
		}
		if err := canceled(ctx); err != nil {
			return projection{}, err
		}
		return result, nil
	}
	return projection{}, problem(ErrInvalid, "version_missing", "")
}

// ValidateManifest verifies a data-only complete claim against checked declaration
// content. It cannot restore Go callables, attest a program or authorize a Worker.
func ValidateManifest(ctx context.Context, bundle *declaration.Bundle, value Manifest) error {
	if err := canceled(ctx); err != nil {
		return err
	}
	if value.Format != Format || value.ABIProfile != ABIProfile {
		return problem(ErrUnsupported, "manifest_profile", "")
	}
	if err := manifestBound(value); err != nil {
		return err
	}
	expected, err := project(ctx, bundle, value.Specification.Identity)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected.manifest, value) {
		return problem(ErrInvalid, "manifest_mismatch", "")
	}
	walker := typeMatcher{ctx: ctx}
	for _, key := range sortedKeys(expected.spec.Bindings) {
		binding := expected.spec.Bindings[key]
		for _, ports := range []map[string]declaration.Port{binding.Inputs, binding.Outputs} {
			for _, name := range sortedKeys(ports) {
				port := ports[name]
				if !port.Required || port.Delivery != "value" {
					return problem(ErrUnsupported, "port_profile", "")
				}
				if err := walker.schema(expected.spec.Contracts[port.Contract], 0); err != nil {
					return err
				}
			}
		}
	}
	return canceled(ctx)
}

func manifestBound(value Manifest) error {
	if len(value.Bindings) > 4096 || len(value.Calls) > 4096 {
		return problem(ErrLimit, "manifest_count", "")
	}
	if len(value.Specification.Identity.Namespace) > 255 || len(value.Specification.Identity.Code) > 128 || len(value.Specification.Identity.Version) > 128 ||
		len(value.Specification.Identity.Mode) > 128 || len(value.Specification.Identity.Revision) > 128 ||
		len(value.Specification.Profile) > 128 || len(value.Specification.SHA256) > 64 || len(value.Format) > 64 || len(value.ABIProfile) > 64 {
		return problem(ErrLimit, "manifest_text", "")
	}
	for _, entry := range value.Bindings {
		if len(entry.Key) > 64 || len(entry.Type) > 128 || len(entry.Revision) > 128 {
			return problem(ErrLimit, "manifest_text", "")
		}
	}
	for _, entry := range value.Calls {
		if len(entry.Node) > 64 || len(entry.Activity) > 64 || len(entry.Binding) > 64 {
			return problem(ErrLimit, "manifest_text", "")
		}
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return problem(ErrInvalid, "manifest_encoding", "", err)
	}
	if len(raw) > MaxManifestBytes {
		return problem(ErrLimit, "manifest_bytes", "")
	}
	return nil
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
func indexed(path string, index int) string      { return path + "/" + strconv.Itoa(index) }
func propertyPath(path string, index int) string { return indexed(path+"/properties", index) }
func fieldName(field reflect.StructField) (string, bool) {
	tag, ok := field.Tag.Lookup("json")
	return tag, ok && tag != "" && tag != "-" && !strings.Contains(tag, ",")
}
