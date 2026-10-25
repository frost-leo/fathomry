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
	"encoding/json/jsontext"
	"fmt"
	"log/slog"
	"slices"
)

// Content is detached per-entry metadata and inner JSON, not a second bundle
// protocol. ContentsCopy returns verified canonical bytes; caller-constructed
// values are untrusted until ParseContents succeeds. Do not mutate them during
// parsing. Bytes may contain private declarations; explicit access is sensitive.
type Content struct {
	Entry Entry
	Bytes []byte
}

// Format avoids accidental declaration disclosure through ordinary formatting.
func (Content) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("workflow.Content")) }

// LogValue avoids accidental declaration disclosure through ordinary logging.
func (Content) LogValue() slog.Value { return slog.StringValue("workflow.Content") }

// ContentsCopy returns independently owned inner documents in machine-identity
// order. Each entry includes the verified identity/profile/digest. Mutating a
// returned slice or bytes cannot alter this immutable bundle or another copy.
func (bundle *Bundle) ContentsCopy() ([]Content, error) {
	if bundle == nil || !bundle.valid {
		return nil, problem(ErrHandle, "bundle", "/")
	}
	result := make([]Content, len(bundle.entries))
	for index, entry := range bundle.entries {
		result[index] = Content{Entry: entry.Entry, Bytes: bytes.Clone(entry.content)}
	}
	return result, nil
}

// ContentCopy returns detached content for one exact identity without copying
// other documents in the bundle. Missing content is not a default selection.
func (bundle *Bundle) ContentCopy(identity Identity) (Content, bool, error) {
	if bundle == nil || !bundle.valid {
		return Content{}, false, problem(ErrHandle, "bundle", "/")
	}
	for _, entry := range bundle.entries {
		if entry.Identity == identity {
			return Content{Entry: entry.Entry, Bytes: bytes.Clone(entry.content)}, true, nil
		}
	}
	return Content{}, false, nil
}

// ParseContents verifies a selected bounded set of stored inner documents through
// the same authority as Parse. Every mode needs its exact parent specification;
// duplicate identities reject, and input order is irrelevant. It additionally
// requires byte-for-byte canonical content, unlike Parse's normalization of an
// ordinary bundle. No missing value/digest is synthesized or repaired. Unsupported
// profiles/formats and corrupt/noncanonical content retain distinct error codes.
// Inputs are borrowed until return, then no mutable caller-owned data is retained.
func ParseContents(ctx context.Context, contents []Content) (*Bundle, error) {
	if err := canceled(ctx); err != nil {
		return nil, err
	}
	if len(contents) == 0 {
		return nil, problem(ErrHandle, "input_set", "/")
	}
	if len(contents) > MaxWorkflows+MaxModes {
		return nil, problem(ErrLimit, "contents", "/")
	}
	original := make(map[Identity]int, len(contents))
	parents := make(map[Identity]map[string]any)
	modeCount, totalBytes := 0, 0
	for index, content := range contents {
		if err := canceled(ctx); err != nil {
			return nil, err
		}
		path := indexPath("/contents", index)
		identity := content.Entry.Identity
		if len(identity.Namespace) > 255 || len(identity.Code) > 128 || len(identity.Version) > 128 ||
			len(identity.Mode) > 128 || len(identity.Revision) > 128 {
			return nil, problem(ErrLimit, "identity", path)
		}
		if (identity.Mode == "") != (identity.Revision == "") {
			return nil, invalid("mode_identity", path)
		}
		profile, maximum := SpecProfile, MaxSpecBytes
		if identity.Mode != "" {
			profile, maximum = ModeProfile, MaxModeBytes
			modeCount++
			if modeCount > MaxModes {
				return nil, problem(ErrLimit, "modes", path)
			}
		}
		if err := supported(content.Entry.Profile, profile, at(path, "profile")); err != nil {
			return nil, err
		}
		if err := (validator{}).digest(content.Entry.SHA256, at(path, "sha256")); err != nil {
			return nil, err
		}
		if len(content.Bytes) > maximum || len(content.Bytes) > MaxBundleBytes-totalBytes {
			return nil, problem(ErrLimit, "bytes", path)
		}
		totalBytes += len(content.Bytes)
		if _, exists := original[identity]; exists {
			return nil, invalid("duplicate_identity", path)
		}
		original[identity] = index
		if identity.Mode == "" {
			if len(parents) == MaxWorkflows {
				return nil, problem(ErrLimit, "workflows", path)
			}
			parents[identity] = map[string]any{
				"namespace": identity.Namespace, "code": identity.Code, "version": identity.Version,
				"fingerprint_profile": content.Entry.Profile, "spec_sha256": content.Entry.SHA256,
				"spec": jsontext.Value(content.Bytes), "modes": []any{},
			}
		}
	}
	for index, content := range contents {
		if err := canceled(ctx); err != nil {
			return nil, err
		}
		identity := content.Entry.Identity
		if identity.Mode == "" {
			continue
		}
		parent := identity
		parent.Mode, parent.Revision = "", ""
		workflow, exists := parents[parent]
		if !exists {
			return nil, invalid("missing_parent", indexPath("/contents", index))
		}
		modes := workflow["modes"].([]any)
		if len(modes) == MaxModesPerWorkflow {
			return nil, problem(ErrLimit, "modes", indexPath("/contents", index))
		}
		workflow["modes"] = append(modes, map[string]any{
			"code": identity.Mode, "revision": identity.Revision,
			"fingerprint_profile": content.Entry.Profile, "config_sha256": content.Entry.SHA256,
			"config": jsontext.Value(content.Bytes),
		})
	}
	identities := make([]Identity, 0, len(parents))
	for identity := range parents {
		identities = append(identities, identity)
	}
	slices.SortFunc(identities, compareIdentity)
	workflows := make([]any, 0, len(parents))
	for _, identity := range identities {
		workflows = append(workflows, parents[identity])
	}
	raw, err := marshal(ctx, map[string]any{"format": BundleFormat, "workflows": workflows}, MaxBundleBytes)
	if err != nil {
		return nil, err
	}
	bundle, err := Parse(ctx, raw)
	if err != nil {
		return nil, err
	}
	for _, entry := range bundle.entries {
		if err := canceled(ctx); err != nil {
			return nil, err
		}
		index := original[entry.Identity]
		if !bytes.Equal(contents[index].Bytes, entry.content) {
			return nil, problem(ErrIntegrity, "noncanonical", indexPath("/contents", index))
		}
	}
	return bundle, nil
}
