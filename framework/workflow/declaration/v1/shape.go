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
	"context"
	"maps"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type validator struct {
	ctx       context.Context
	authoring bool
}

func keys[T any](value map[string]T) []string { return slices.Sorted(maps.Keys(value)) }

// Paths contain fixed field segments and admitted ASCII identifiers, never raw
// unvalidated object names or declaration values.
func at(path, key string) string              { return path + "/" + key }
func indexPath(path string, index int) string { return at(path, strconv.Itoa(index)) }

func object(value any, required, optional, path string) (map[string]any, error) {
	result, ok := value.(map[string]any)
	if !ok {
		return nil, invalid("object", path)
	}
	requiredKeys := strings.Fields(required)
	for _, key := range requiredKeys {
		if _, ok := result[key]; !ok {
			return nil, invalid("missing_field", at(path, key))
		}
	}
	optionalKeys := strings.Fields(optional)
	for key := range result {
		if !slices.Contains(requiredKeys, key) && !slices.Contains(optionalKeys, key) {
			return nil, invalid("unknown_field", path)
		}
	}
	return result, nil
}

func array(value any, minimum, maximum int, path string) ([]any, error) {
	values, ok := value.([]any)
	if !ok || len(values) < minimum {
		return nil, invalid("array", path)
	}
	if len(values) > maximum {
		return nil, problem(ErrLimit, "collection", path)
	}
	return values, nil
}

func identifier(value any, maximum int, kind string, path string) error {
	text, ok := value.(string)
	if !ok || len(text) == 0 || len(text) > maximum {
		return invalid("identifier", path)
	}
	for index, character := range []byte(text) {
		lower := character >= 'a' && character <= 'z'
		upper := character >= 'A' && character <= 'Z'
		digit := character >= '0' && character <= '9'
		valid := lower || (index > 0 && (digit || character == '_' || character == '-'))
		switch kind {
		case "label":
			valid = lower || upper || digit || (index > 0 && strings.ContainsRune("._+-", rune(character)))
		case "binding":
			valid = lower || upper || (index > 0 && (digit || strings.ContainsRune("._/-", rune(character))))
		case "field":
			valid = lower || upper || character == '_' || (index > 0 && digit)
		}
		if !valid {
			return invalid("identifier", path)
		}
	}
	return nil
}

func choice(value any, choices, path string) error {
	text, ok := value.(string)
	if !ok {
		return invalid("string", path)
	}
	for _, expected := range strings.Fields(choices) {
		if text == expected {
			return nil
		}
	}
	return invalid("choice", path)
}

func supported(value any, expected, path string) error {
	text, ok := value.(string)
	if !ok {
		return invalid("string", path)
	}
	if text != expected {
		return problem(ErrUnsupported, "profile", path)
	}
	return nil
}

func boolean(value any, path string) error {
	if _, ok := value.(bool); !ok {
		return invalid("boolean", path)
	}
	return nil
}

func integerRange(value any, minimum, maximum int64, path string) error {
	number, ok := value.(int64)
	if !ok || number < minimum || number > maximum {
		return invalid("integer_range", path)
	}
	return nil
}

func decimal(value any, path string) (int64, error) {
	text, ok := value.(string)
	if !ok || len(text) == 0 || len(text) > 19 || (len(text) > 1 && text[0] == '0') {
		return 0, invalid("decimal", path)
	}
	for _, character := range []byte(text) {
		if character < '0' || character > '9' {
			return 0, invalid("decimal", path)
		}
	}
	number, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, invalid("decimal", path)
	}
	return number, nil
}

func set(value any, minimum, maximum int, choices, path string) error {
	values, err := array(value, minimum, maximum, path)
	if err != nil {
		return err
	}
	seen := make(map[string]bool, len(values))
	for _, item := range values {
		text, ok := item.(string)
		if !ok {
			return invalid("string", path)
		}
		if seen[text] {
			return invalid("duplicate_member", path)
		}
		seen[text] = true
		if choices != "" {
			if err := choice(text, choices, path); err != nil {
				return err
			}
		} else if err := identifier(text, 64, "id", path); err != nil {
			return err
		}
	}
	return nil
}

func (v validator) records(value any, minimum, maximum int, path string, check func(any, string) error) error {
	entries, ok := value.(map[string]any)
	if !ok || len(entries) < minimum {
		return invalid("mapping", path)
	}
	if len(entries) > maximum {
		return problem(ErrLimit, "collection", path)
	}
	for _, key := range keys(entries) {
		if err := canceled(v.ctx); err != nil {
			return err
		}
		if err := identifier(key, 64, "id", path); err != nil {
			return err
		}
		if err := check(entries[key], at(path, key)); err != nil {
			return err
		}
	}
	return nil
}

func (v validator) shape(value any) error {
	root, err := object(value, "format workflows", "", "")
	if err != nil {
		return err
	}
	if err := supported(root["format"], BundleFormat, "/format"); err != nil {
		return err
	}
	workflows, err := array(root["workflows"], 1, MaxWorkflows, "/workflows")
	if err != nil {
		return err
	}
	totalModes := 0
	for index, value := range workflows {
		if err := canceled(v.ctx); err != nil {
			return err
		}
		path := indexPath("/workflows", index)
		entry, err := object(value, "namespace code version fingerprint_profile spec_sha256 spec modes", "", path)
		if err != nil {
			return err
		}
		namespace, ok := entry["namespace"].(string)
		if !ok || len(namespace) == 0 || len(namespace) > 255 || !utf8.ValidString(namespace) || strings.ContainsFunc(namespace, unicode.IsControl) {
			return invalid("namespace", at(path, "namespace"))
		}
		if err := identifier(entry["code"], 128, "id", at(path, "code")); err != nil {
			return err
		}
		if err := identifier(entry["version"], 128, "label", at(path, "version")); err != nil {
			return err
		}
		if err := supported(entry["fingerprint_profile"], SpecProfile, at(path, "fingerprint_profile")); err != nil {
			return err
		}
		if err := v.digest(entry["spec_sha256"], at(path, "spec_sha256")); err != nil {
			return err
		}
		if err := boundDepth(v.ctx, entry["spec"], 1, MaxInnerDepth); err != nil {
			return err
		}
		if err := v.spec(entry["spec"], at(path, "spec")); err != nil {
			return err
		}
		modes, err := array(entry["modes"], 0, MaxModesPerWorkflow, at(path, "modes"))
		if err != nil {
			return err
		}
		totalModes += len(modes)
		if totalModes > MaxModes {
			return problem(ErrLimit, "modes", "/workflows")
		}
		for modeIndex, value := range modes {
			modePath := indexPath(at(path, "modes"), modeIndex)
			mode, err := object(value, "code revision fingerprint_profile config_sha256 config", "", modePath)
			if err != nil {
				return err
			}
			if err := identifier(mode["code"], 128, "id", at(modePath, "code")); err != nil {
				return err
			}
			if err := identifier(mode["revision"], 128, "label", at(modePath, "revision")); err != nil {
				return err
			}
			if err := supported(mode["fingerprint_profile"], ModeProfile, at(modePath, "fingerprint_profile")); err != nil {
				return err
			}
			if err := v.digest(mode["config_sha256"], at(modePath, "config_sha256")); err != nil {
				return err
			}
			config, err := object(mode["config"], "format values", "", at(modePath, "config"))
			if err != nil {
				return err
			}
			if err := supported(config["format"], ModeFormat, at(modePath, "config/format")); err != nil {
				return err
			}
			if err := v.records(config["values"], 0, 4096, at(modePath, "config/values"), func(value any, path string) error { _, err := decimal(value, path); return err }); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v validator) digest(value any, path string) error {
	text, ok := value.(string)
	if !ok {
		return invalid("digest", path)
	}
	if v.authoring && text == "" {
		return nil
	}
	if len(text) != 64 {
		return problem(ErrIntegrity, "digest", path)
	}
	for _, character := range []byte(text) {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return problem(ErrIntegrity, "digest", path)
		}
	}
	return nil
}

func (v validator) spec(value any, path string) error {
	spec, err := object(value, "format semantics contracts bindings inputs outputs nodes edges controls relations limits completion", "", path)
	if err != nil {
		return err
	}
	if err = supported(spec["format"], SpecFormat, at(path, "format")); err != nil {
		return err
	}
	if err = supported(spec["semantics"], Semantics, at(path, "semantics")); err != nil {
		return err
	}
	for _, group := range []struct {
		name     string
		min, max int
		check    func(any, string) error
	}{
		{"contracts", 1, 1024, func(value any, path string) error { return v.schema(value, path, 0) }},
		{"bindings", 1, 4096, v.binding}, {"inputs", 0, 64, portShape}, {"outputs", 0, 64, exportShape},
		{"nodes", 1, 1024, v.node}, {"edges", 0, 8192, edgeShape}, {"controls", 0, 4096, controlShape},
		{"relations", 0, 256, relationShape},
	} {
		if err := v.records(spec[group.name], group.min, group.max, at(path, group.name), group.check); err != nil {
			return err
		}
	}
	limits, err := object(spec["limits"], "active_chunks items_per_chunk chunk_item_concurrency run_timeout_ms cleanup_reserve_ms", "", at(path, "limits"))
	if err != nil {
		return err
	}
	for _, key := range keys(limits) {
		if err := settingShape(limits[key], at(at(path, "limits"), key)); err != nil {
			return err
		}
	}
	completion, err := object(spec["completion"], "item run maximum_failed_items", "", at(path, "completion"))
	if err != nil {
		return err
	}
	if err := choice(completion["item"], "selected_paths_and_required_effects", at(path, "completion/item")); err != nil {
		return err
	}
	if err := choice(completion["run"], "all_succeeded allow_failed", at(path, "completion/run")); err != nil {
		return err
	}
	_, err = decimal(completion["maximum_failed_items"], at(path, "completion/maximum_failed_items"))
	return err
}

func portShape(value any, path string) error {
	port, err := object(value, "contract delivery required", "", path)
	if err != nil {
		return err
	}
	if err := identifier(port["contract"], 64, "id", at(path, "contract")); err != nil {
		return err
	}
	if err := choice(port["delivery"], "value complete_batch", at(path, "delivery")); err != nil {
		return err
	}
	return boolean(port["required"], at(path, "required"))
}

func exportShape(value any, path string) error {
	port, err := object(value, "node port required", "", path)
	if err != nil {
		return err
	}
	if err := identifier(port["node"], 64, "id", at(path, "node")); err != nil {
		return err
	}
	if err := identifier(port["port"], 64, "id", at(path, "port")); err != nil {
		return err
	}
	return boolean(port["required"], at(path, "required"))
}

func sourceShape(value any, path string) error {
	record, ok := value.(map[string]any)
	if !ok {
		return invalid("source", path)
	}
	required := ""
	switch record["kind"] {
	case "root", "input":
		required = "kind port"
	case "edge":
		required = "kind edge port"
	case "activity":
		required = "kind activity port"
	default:
		return invalid("source_kind", path)
	}
	record, err := object(value, required, "", path)
	if err != nil {
		return err
	}
	for _, key := range strings.Fields(required) {
		if key != "kind" {
			if err := identifier(record[key], 64, "id", at(path, key)); err != nil {
				return err
			}
		}
	}
	return nil
}

func settingShape(value any, path string) error {
	record, ok := value.(map[string]any)
	if !ok || len(record) != 1 {
		return invalid("setting", path)
	}
	if value, ok := record["value"]; ok {
		_, err := decimal(value, at(path, "value"))
		return err
	}
	if control, ok := record["control"]; ok {
		return identifier(control, 64, "id", at(path, "control"))
	}
	return invalid("setting", path)
}

func policyShape(value any, path string) error {
	record, err := object(value, "start_to_close_ms schedule_to_close_ms maximum_attempts initial_interval_ms maximum_interval_ms backoff_milli", "heartbeat_ms", path)
	if err != nil {
		return err
	}
	for _, key := range keys(record) {
		if err := settingShape(record[key], at(path, key)); err != nil {
			return err
		}
	}
	return nil
}

func (v validator) binding(value any, path string) error {
	record, err := object(value, "type revision retry_safety inputs outputs effects", "", path)
	if err != nil {
		return err
	}
	if err := identifier(record["type"], 128, "binding", at(path, "type")); err != nil {
		return err
	}
	if err := identifier(record["revision"], 128, "label", at(path, "revision")); err != nil {
		return err
	}
	if err := choice(record["retry_safety"], "read_only idempotent not_retryable", at(path, "retry_safety")); err != nil {
		return err
	}
	if err := v.records(record["inputs"], 0, 64, at(path, "inputs"), portShape); err != nil {
		return err
	}
	if err := v.records(record["outputs"], 0, 64, at(path, "outputs"), func(value any, path string) error {
		if err := portShape(value, path); err != nil {
			return err
		}
		if value.(map[string]any)["required"] != true {
			return invalid("output_presence", path)
		}
		return nil
	}); err != nil {
		return err
	}
	return v.records(record["effects"], 0, 64, at(path, "effects"), func(value any, path string) error { return set(value, 1, 3, "accepted committed visible", path) })
}

func (v validator) node(value any, path string) error {
	record, err := object(value, "inputs outputs activities phases join routes route required_effects", "gate", path)
	if err != nil {
		return err
	}
	if err := v.records(record["inputs"], 0, 64, at(path, "inputs"), func(value any, path string) error {
		input, err := object(value, "contract delivery required source", "", path)
		if err != nil {
			return err
		}
		if err := identifier(input["contract"], 64, "id", at(path, "contract")); err != nil {
			return err
		}
		if err := choice(input["delivery"], "value complete_batch", at(path, "delivery")); err != nil {
			return err
		}
		if err := boolean(input["required"], at(path, "required")); err != nil {
			return err
		}
		return sourceShape(input["source"], at(path, "source"))
	}); err != nil {
		return err
	}
	if err := v.records(record["outputs"], 0, 64, at(path, "outputs"), sourceShape); err != nil {
		return err
	}
	if err := v.records(record["activities"], 1, 256, at(path, "activities"), func(value any, path string) error {
		call, err := object(value, "binding inputs policy", "", path)
		if err != nil {
			return err
		}
		if err := identifier(call["binding"], 64, "id", at(path, "binding")); err != nil {
			return err
		}
		if err := v.records(call["inputs"], 0, 64, at(path, "inputs"), sourceShape); err != nil {
			return err
		}
		return policyShape(call["policy"], at(path, "policy"))
	}); err != nil {
		return err
	}
	phases, err := array(record["phases"], 1, 256, at(path, "phases"))
	if err != nil {
		return err
	}
	for index, phase := range phases {
		if err := set(phase, 1, 256, "", indexPath(at(path, "phases"), index)); err != nil {
			return err
		}
	}
	if err := choice(record["join"], "all any", at(path, "join")); err != nil {
		return err
	}
	if err := set(record["routes"], 1, 64, "", at(path, "routes")); err != nil {
		return err
	}
	route, ok := record["route"].(map[string]any)
	if !ok {
		return invalid("route", at(path, "route"))
	}
	switch route["kind"] {
	case "fixed":
		if _, err := object(route, "kind value", "", at(path, "route")); err != nil {
			return err
		}
		if err := identifier(route["value"], 64, "id", at(path, "route/value")); err != nil {
			return err
		}
	case "activity":
		if _, err := object(route, "kind activity port", "", at(path, "route")); err != nil {
			return err
		}
		for _, key := range []string{"activity", "port"} {
			if err := identifier(route[key], 64, "id", at(at(path, "route"), key)); err != nil {
				return err
			}
		}
	default:
		return invalid("route_kind", at(path, "route"))
	}
	if raw, ok := record["gate"]; ok {
		gate, err := object(raw, "scope nodes condition", "", at(path, "gate"))
		if err != nil {
			return err
		}
		if err := choice(gate["scope"], "chunk run", at(path, "gate/scope")); err != nil {
			return err
		}
		if err := set(gate["nodes"], 1, 256, "", at(path, "gate/nodes")); err != nil {
			return err
		}
		if err := choice(gate["condition"], "all_succeeded all_settled", at(path, "gate/condition")); err != nil {
			return err
		}
	}
	return v.records(record["required_effects"], 0, 64, at(path, "required_effects"), func(value any, path string) error {
		effect, err := object(value, "activity effect guarantee", "", path)
		if err != nil {
			return err
		}
		for _, key := range []string{"activity", "effect"} {
			if err := identifier(effect[key], 64, "id", at(path, key)); err != nil {
				return err
			}
		}
		return choice(effect["guarantee"], "accepted committed visible", at(path, "guarantee"))
	})
}

func edgeShape(value any, path string) error {
	edge, err := object(value, "from to on", "", path)
	if err != nil {
		return err
	}
	for _, key := range []string{"from", "to"} {
		if err := identifier(edge[key], 64, "id", at(path, key)); err != nil {
			return err
		}
	}
	return set(edge["on"], 1, 64, "", at(path, "on"))
}

func controlShape(value any, path string) error {
	control, err := object(value, "kind minimum maximum override", "default", path)
	if err != nil {
		return err
	}
	if err := choice(control["kind"], "count duration_ms ratio_milli", at(path, "kind")); err != nil {
		return err
	}
	for _, key := range []string{"minimum", "maximum", "default"} {
		if raw, ok := control[key]; ok {
			if _, err := decimal(raw, at(path, key)); err != nil {
				return err
			}
		}
	}
	return set(control["override"], 0, 2, "run item", at(path, "override"))
}

func relationShape(value any, path string) error {
	relation, err := object(value, "kind terms limit", "", path)
	if err != nil {
		return err
	}
	if err := choice(relation["kind"], "sum_le", at(path, "kind")); err != nil {
		return err
	}
	if err := set(relation["terms"], 1, 64, "", at(path, "terms")); err != nil {
		return err
	}
	return identifier(relation["limit"], 64, "id", at(path, "limit"))
}
