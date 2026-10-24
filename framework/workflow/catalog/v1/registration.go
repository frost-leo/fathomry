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

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

// Metadata bounds are independent of the unchanged declaration document limits.
// MetadataBytes includes selector bytes, generated addresses, text, context and tags.
const (
	MaxResources        = 4096
	MaxMetadataBytes    = 4 << 20
	MaxResourceKeyBytes = 1024
	MaxTextBytes        = 16 << 10
	MaxNameCharacters   = 256
	MaxContextBytes     = 1024
	MaxTags             = 64
	MaxTagKeyBytes      = 128
	MaxTagValueBytes    = 256
)

// Kind identifies a closed display owner inside the selected workflow aggregate.
type Kind string

const (
	Definition   Kind = "definition"
	Version      Kind = "version"
	Mode         Kind = "mode"
	ModeRevision Kind = "mode_revision"
	Node         Kind = "node"
	Activity     Kind = "activity"
	Control      Kind = "control"
)

// Field selects supported display metadata, never an executable property.
type Field string

const (
	Name        Field = "name"
	Description Field = "description"
)

// Target is a closed owner union. Definition has no selectors; Version needs
// Version; Mode additionally needs Mode; ModeRevision adds Revision. Node and
// Activity use Version/Node[/Activity]; Control uses Version/Control. All unused
// fields must be empty. Namespace/workflow come only from the verified bundle.
type Target struct {
	Kind                                             Kind
	Version, Mode, Revision, Node, Activity, Control string
}

// Text is publisher-owned English and optional translator context. Empty Context
// means absent context; an empty description is present, not an omitted resource.
type Text struct{ English, Context string }

// Resource supplies display data for an existing declaration owner. The catalog
// derives scope/key; callers cannot redirect a resource with an opaque address.
type Resource struct {
	Target Target
	Field  Field
	Text   Text
}

// Category applies only on first definition creation. Name nil means reference
// an existing category; non-nil Name declares one. Description requires Name.
// Later equal registration must not recreate/compare an ignored initial category.
type Category struct {
	Code              string
	Name, Description *Text
}

// Metadata is the project-owned publication data accompanying a declaration.
// It contains no second workflow list or executable content. A nil Metadata
// pointer in project assembly means publication metadata was not supplied.
type Metadata struct {
	Resources []Resource
	Category  *Category
	Tags      map[string]string
}

func (Metadata) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("catalog.Metadata")) }
func (Metadata) LogValue() slog.Value           { return slog.StringValue("catalog.Metadata") }

// Registration is caller-owned authoring data for exactly one Namespace/code.
// Bundle must already be verified by declaration.Compile or declaration.Parse.
// Tags/category are first-creation inputs, not management updates. Do not mutate
// inputs during Prepare or put credentials in tags; secret detection is not promised.
type Registration struct {
	Bundle    *declaration.Bundle
	Resources []Resource
	Category  *Category
	Tags      map[string]string
}

// PreparedRegistration is an immutable in-process publication snapshot, not a
// durable document, registration receipt, database owner or schema-admission token.
// Copies share immutable state. There is no live source, pool or runtime here.
type PreparedRegistration struct{ state *registration }

type registration struct {
	info                        Preparation
	contents                    []declaration.Content
	resources, initialResources []publicationResource
	category                    *initialCategory
	tags                        []tag
}
type publicationResource struct {
	scope, key string
	text       Text
	field      Field
}
type initialCategory struct {
	code    string
	declare bool
}
type tag struct{ key, value string }

// Preparation reports bounded input facts only. It does not claim a database
// exists, that anything was persisted, or that a workflow is enabled/deployed.
// DocumentBytes counts retained canonical inner documents, excluding Bundle wrappers.
type Preparation struct {
	Namespace, Code                                                     string
	Versions, Modes, Revisions, Resources, MetadataBytes, DocumentBytes int
}

// Inspect returns detached counts/identity without exposing declarations or text.
func (value *PreparedRegistration) Inspect() (Preparation, error) {
	if value == nil || value.state == nil {
		return Preparation{}, problem(ErrHandle, "prepared", "")
	}
	return value.state.info, nil
}
func (Registration) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("catalog.Registration"))
}
func (Registration) LogValue() slog.Value { return slog.StringValue("catalog.Registration") }
func (PreparedRegistration) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("catalog.PreparedRegistration"))
}
func (PreparedRegistration) LogValue() slog.Value {
	return slog.StringValue("catalog.PreparedRegistration")
}
func (PreparedRegistration) MarshalJSON() ([]byte, error) {
	return nil, problem(ErrSerialization, "prepared", "")
}
func (*PreparedRegistration) UnmarshalJSON([]byte) error {
	return problem(ErrSerialization, "prepared", "")
}

type graphOwners struct {
	Nodes map[string]struct {
		Activities map[string]struct{} `json:"activities"`
	} `json:"nodes"`
	Controls map[string]struct{} `json:"controls"`
}

// Prepare validates and copies one selected publication before any database work.
// It reuses the verified declaration authority, checks metadata owner references,
// generates bounded injective keys and separates first-creation-only inputs.
// No partial snapshot is returned on failure; no registration or I/O is performed.
func Prepare(ctx context.Context, input Registration) (*PreparedRegistration, error) {
	if err := canceled(ctx); err != nil {
		return nil, err
	}
	if len(input.Resources) > MaxResources || len(input.Tags) > MaxTags {
		return nil, problem(ErrLimit, "metadata_count", "")
	}
	total := 0
	for index, item := range input.Resources {
		if err := canceled(ctx); err != nil {
			return nil, err
		}
		path := "/resources/" + strconv.Itoa(index)
		if item.Field != Name && item.Field != Description {
			return nil, problem(ErrMetadata, "field", path)
		}
		if err := admitText(item.Field, item.Text, path); err != nil {
			return nil, err
		}
		selectors := []string{string(item.Target.Kind), item.Target.Version, item.Target.Mode, item.Target.Revision, item.Target.Node, item.Target.Activity, item.Target.Control}
		size := len(item.Field) + len(item.Text.English) + len(item.Text.Context)
		for _, selector := range selectors {
			if len(selector) > 128 || !utf8.ValidString(selector) {
				return nil, problem(ErrMetadata, "selector", path)
			}
			size += len(selector)
		}
		if err := charge(&total, size); err != nil {
			return nil, err
		}
	}
	if input.Category != nil {
		category := input.Category
		if !validText(category.Code, 128) || category.Code == "" || strings.ContainsFunc(category.Code, unicode.IsControl) || category.Name == nil && category.Description != nil {
			return nil, problem(ErrMetadata, "category", "/category")
		}
		if err := charge(&total, len(category.Code)); err != nil {
			return nil, err
		}
		for _, item := range []struct {
			field Field
			text  *Text
		}{{Name, category.Name}, {Description, category.Description}} {
			if item.text == nil {
				continue
			}
			if err := admitText(item.field, *item.text, "/category"); err != nil {
				return nil, err
			}
			if err := charge(&total, len(item.text.English)+len(item.text.Context)); err != nil {
				return nil, err
			}
		}
	}
	keys := make([]string, 0, len(input.Tags))
	for key, value := range input.Tags {
		if !validText(key, MaxTagKeyBytes) || key == "" || !validText(value, MaxTagValueBytes) || reservedTag(key) {
			return nil, problem(ErrMetadata, "tag", "/tags")
		}
		if err := charge(&total, len(key)+len(value)); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	contents, err := input.Bundle.ContentsCopy()
	if err != nil {
		return nil, problem(ErrInput, "declarations", "", err)
	}
	if len(contents) == 0 {
		return nil, problem(ErrInput, "empty_aggregate", "")
	}
	identity := contents[0].Entry.Identity
	result := &registration{contents: contents, info: Preparation{Namespace: identity.Namespace, Code: identity.Code}}
	versions := map[string][]byte{}
	modes := map[Target]struct{}{}
	required := map[Target]bool{{Kind: Definition}: false}
	for _, content := range contents {
		if err := canceled(ctx); err != nil {
			return nil, err
		}
		selected := content.Entry.Identity
		result.info.DocumentBytes += len(content.Bytes)
		if selected.Namespace != identity.Namespace || selected.Code != identity.Code {
			return nil, problem(ErrInput, "mixed_workflows", "")
		}
		if selected.Mode == "" {
			versions[selected.Version] = content.Bytes
		} else {
			target := Target{Kind: Mode, Version: selected.Version, Mode: selected.Mode}
			required[target] = false
			modes[Target{Kind: ModeRevision, Version: selected.Version, Mode: selected.Mode, Revision: selected.Revision}] = struct{}{}
		}
	}
	result.info.Versions, result.info.Modes, result.info.Revisions = len(versions), len(required)-1, len(modes)
	projections := map[string]graphOwners{}
	seen := map[string]struct{}{}
	for index, item := range input.Resources {
		if err := canceled(ctx); err != nil {
			return nil, err
		}
		path := "/resources/" + strconv.Itoa(index)
		scope, parts, err := owner(item.Target, versions, modes, projections)
		if err != nil {
			return nil, problem(ErrMetadata, "owner", path, err)
		}
		parts = append([]string{identity.Namespace, identity.Code}, parts...)
		parts = append(parts, string(item.Field))
		key, err := resourceKey(parts...)
		if err != nil {
			return nil, err
		}
		address := scope + "\x00" + key
		if _, exists := seen[address]; exists {
			return nil, problem(ErrMetadata, "duplicate_resource", path)
		}
		seen[address] = struct{}{}
		if err := charge(&total, len(scope)+len(key)); err != nil {
			return nil, err
		}
		result.resources = append(result.resources, publicationResource{scope: scope, key: key, text: copyText(item.Text), field: item.Field})
		if _, needed := required[item.Target]; needed && item.Field == Name {
			required[item.Target] = true
		}
	}
	for _, present := range required {
		if !present {
			return nil, problem(ErrMetadata, "required_name", "/resources")
		}
	}
	if input.Category != nil {
		selected := input.Category
		result.category = &initialCategory{code: strings.Clone(selected.Code), declare: selected.Name != nil}
		for _, item := range []struct {
			field Field
			text  *Text
		}{{Name, selected.Name}, {Description, selected.Description}} {
			if item.text == nil {
				continue
			}
			key, err := resourceKey(identity.Namespace, selected.Code, string(item.field))
			if err != nil {
				return nil, err
			}
			scope := "workflow.category"
			if err := charge(&total, len(scope)+len(key)); err != nil {
				return nil, err
			}
			result.initialResources = append(result.initialResources, publicationResource{scope: scope, key: key, text: copyText(*item.text), field: item.field})
		}
	}
	if len(result.resources)+len(result.initialResources) > MaxResources {
		return nil, problem(ErrLimit, "resource_count", "")
	}
	slices.Sort(keys)
	for _, key := range keys {
		result.tags = append(result.tags, tag{strings.Clone(key), strings.Clone(input.Tags[key])})
	}
	order := func(left, right publicationResource) int {
		if value := strings.Compare(left.scope, right.scope); value != 0 {
			return value
		}
		return strings.Compare(left.key, right.key)
	}
	slices.SortFunc(result.resources, order)
	slices.SortFunc(result.initialResources, order)
	result.info.Resources, result.info.MetadataBytes = len(result.resources)+len(result.initialResources), total
	if err := canceled(ctx); err != nil {
		return nil, err
	}
	return &PreparedRegistration{state: result}, nil
}

func owner(target Target, versions map[string][]byte, modes map[Target]struct{}, projections map[string]graphOwners) (string, []string, error) {
	admitted := Target{Kind: target.Kind}
	var parts []string
	if target.Kind != Definition {
		if _, exists := versions[target.Version]; !exists {
			return "", nil, ErrMetadata
		}
		admitted.Version = target.Version
		parts = append(parts, target.Version)
	}
	switch target.Kind {
	case Definition, Version:
	case Mode:
		admitted.Mode = target.Mode
		found := false
		for mode := range modes {
			if mode.Version == target.Version && mode.Mode == target.Mode {
				found = true
				break
			}
		}
		if !found {
			return "", nil, ErrMetadata
		}
		parts = append(parts, target.Mode)
	case ModeRevision:
		admitted.Mode, admitted.Revision = target.Mode, target.Revision
		if _, exists := modes[admitted]; !exists {
			return "", nil, ErrMetadata
		}
		parts = append(parts, target.Mode, target.Revision)
	case Node, Activity, Control:
		projection, exists := projections[target.Version]
		if !exists {
			// The Bundle already verified executable semantics. Decode only owner names,
			// once per referenced version; this is projection, not a second validator.
			if err := json.Unmarshal(versions[target.Version], &projection); err != nil {
				return "", nil, err
			}
			projections[target.Version] = projection
		}
		if target.Kind == Control {
			admitted.Control = target.Control
			if _, exists := projection.Controls[target.Control]; !exists {
				return "", nil, ErrMetadata
			}
			parts = append(parts, target.Control)
		} else {
			admitted.Node = target.Node
			node, exists := projection.Nodes[target.Node]
			if !exists {
				return "", nil, ErrMetadata
			}
			parts = append(parts, target.Node)
			if target.Kind == Activity {
				admitted.Activity = target.Activity
				if _, exists := node.Activities[target.Activity]; !exists {
					return "", nil, ErrMetadata
				}
				parts = append(parts, target.Activity)
			}
		}
	default:
		return "", nil, ErrMetadata
	}
	if target != admitted {
		return "", nil, ErrMetadata
	}
	return "workflow." + string(target.Kind), parts, nil
}

func resourceKey(parts ...string) (string, error) {
	var value strings.Builder
	value.WriteString("v1;")
	for _, part := range parts {
		if len(part) > MaxResourceKeyBytes || value.Len()+len(part)+len(strconv.Itoa(len(part)))+1 > MaxResourceKeyBytes {
			return "", problem(ErrLimit, "resource_key", "")
		}
		value.WriteString(strconv.Itoa(len(part)))
		value.WriteByte(':')
		value.WriteString(part)
	}
	return value.String(), nil
}
func copyText(value Text) Text {
	return Text{strings.Clone(value.English), strings.Clone(value.Context)}
}
func validText(value string, maximum int) bool {
	return len(value) <= maximum && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}
func admitText(field Field, value Text, path string) error {
	if !validText(value.English, MaxTextBytes) || !validText(value.Context, MaxContextBytes) ||
		field == Name && (strings.TrimSpace(value.English) == "" || utf8.RuneCountInString(value.English) > MaxNameCharacters) {
		return problem(ErrMetadata, "text", path)
	}
	return nil
}
func charge(total *int, amount int) error {
	if amount > MaxMetadataBytes-*total {
		return problem(ErrLimit, "metadata_bytes", "")
	}
	*total += amount
	return nil
}
func reservedTag(key string) bool {
	lower := strings.ToLower(key)
	return strings.HasPrefix(lower, "fathomry.") || strings.HasPrefix(lower, "temporal.") ||
		slices.Contains([]string{"id", "created_at", "updated_at", "row_revision", "namespace", "code", "category_id", "enabled", "version", "mode", "revision", "default_version_id", "default_mode_id", "default_mode_version_id"}, lower)
}
func canceled(ctx context.Context) error {
	if ctx == nil {
		return problem(ErrInput, "context", "")
	}
	if ctx.Err() != nil {
		return problem(ErrCanceled, "context", "", ctx.Err(), context.Cause(ctx))
	}
	return nil
}
