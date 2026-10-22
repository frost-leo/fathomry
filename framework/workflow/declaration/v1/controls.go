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
	"math"
	"slices"
)

const maxDurationMS int64 = math.MaxInt64 / 1000000

type controlSlot struct {
	name, kind       string
	value            Setting
	minimum, maximum int64
	global           bool
}

func limitSlots(limits Limits) []controlSlot {
	return []controlSlot{
		{"active_chunks", "count", limits.ActiveChunks, 1, math.MaxInt64, true},
		{"items_per_chunk", "count", limits.ItemsPerChunk, 1, math.MaxInt64, true},
		{"chunk_item_concurrency", "count", limits.ChunkItemConcurrency, 1, math.MaxInt64, true},
		{"run_timeout_ms", "duration_ms", limits.RunTimeoutMS, 1, maxDurationMS, true},
		{"cleanup_reserve_ms", "duration_ms", limits.CleanupReserveMS, 1, maxDurationMS, true},
	}
}

func policySlots(policy Policy) []controlSlot {
	result := []controlSlot{
		{"start_to_close_ms", "duration_ms", policy.StartToCloseMS, 1, maxDurationMS, false},
		{"schedule_to_close_ms", "duration_ms", policy.ScheduleToCloseMS, 1, maxDurationMS, false},
		{"maximum_attempts", "count", policy.MaximumAttempts, 1, math.MaxInt32, false},
		{"initial_interval_ms", "duration_ms", policy.InitialIntervalMS, 1, maxDurationMS, false},
		{"maximum_interval_ms", "duration_ms", policy.MaximumIntervalMS, 1, maxDurationMS, false},
		{"backoff_milli", "ratio_milli", policy.BackoffMilli, 1000, 1000000, false},
	}
	if policy.HeartbeatMS != nil {
		result = append(result, controlSlot{"heartbeat_ms", "duration_ms", *policy.HeartbeatMS, 1, maxDurationMS, false})
	}
	return result
}

func normalizeControls(ctx context.Context, spec *Specification, path string) error {
	used := map[string]bool{}
	defaults := map[string]Decimal{}
	for _, name := range keys(spec.Controls) {
		if err := canceled(ctx); err != nil {
			return err
		}
		control := spec.Controls[name]
		controlPath := at(path, "controls/"+name)
		minimum, _ := decimal(string(control.Minimum), controlPath)
		maximum, _ := decimal(string(control.Maximum), controlPath)
		if minimum > maximum {
			return invalid("control_range", controlPath)
		}
		if control.Default != nil {
			value, _ := decimal(string(*control.Default), controlPath)
			if value < minimum || value > maximum {
				return invalid("control_default", controlPath)
			}
			defaults[name] = *control.Default
		}
		slices.Sort(control.Override)
	}
	validateSlot := func(slot controlSlot, path string) error {
		if slot.value.Value != nil {
			value, err := decimal(string(*slot.value.Value), path)
			if err != nil {
				return err
			}
			if value < slot.minimum || value > slot.maximum {
				return invalid("slot_range", path)
			}
			return nil
		}
		control, ok := spec.Controls[slot.value.Control]
		if !ok {
			return invalid("control_ref", path)
		}
		used[slot.value.Control] = true
		if control.Kind != slot.kind {
			return invalid("control_kind", path)
		}
		minimum, _ := decimal(string(control.Minimum), path)
		maximum, _ := decimal(string(control.Maximum), path)
		if minimum < slot.minimum || maximum > slot.maximum {
			return invalid("slot_range", path)
		}
		if slot.global && slices.Contains(control.Override, "item") {
			return invalid("override_scope", path)
		}
		return nil
	}
	for _, slot := range limitSlots(spec.Limits) {
		if err := validateSlot(slot, at(path, "limits/"+slot.name)); err != nil {
			return err
		}
	}
	for _, nodeName := range keys(spec.Nodes) {
		if err := canceled(ctx); err != nil {
			return err
		}
		node := spec.Nodes[nodeName]
		for _, callName := range keys(node.Activities) {
			call := node.Activities[callName]
			policyPath := at(path, "nodes/"+nodeName+"/activities/"+callName+"/policy")
			for _, slot := range policySlots(call.Policy) {
				if err := validateSlot(slot, at(policyPath, slot.name)); err != nil {
					return err
				}
			}
			if spec.Bindings[call.Binding].RetrySafety == "not_retryable" {
				attempts := call.Policy.MaximumAttempts
				maximum := int64(0)
				if attempts.Value != nil {
					maximum, _ = decimal(string(*attempts.Value), policyPath)
				} else {
					maximum, _ = decimal(string(spec.Controls[attempts.Control].Maximum), policyPath)
				}
				if maximum != 1 {
					return invalid("retry_safety", at(policyPath, "maximum_attempts"))
				}
			}
		}
	}
	for _, name := range keys(spec.Relations) {
		relation := spec.Relations[name]
		relationPath := at(path, "relations/"+name)
		limit, ok := spec.Controls[relation.Limit]
		if !ok {
			return invalid("relation_ref", relationPath)
		}
		for _, term := range relation.Terms {
			control, ok := spec.Controls[term]
			if !ok {
				return invalid("relation_ref", relationPath)
			}
			if control.Kind != limit.Kind {
				return invalid("relation_unit", relationPath)
			}
		}
		slices.Sort(relation.Terms)
	}
	for _, name := range keys(spec.Controls) {
		if !used[name] {
			return invalid("unused_control", at(path, "controls/"+name))
		}
	}
	maximumFailed, _ := decimal(string(spec.Completion.MaximumFailedItems), at(path, "completion"))
	if spec.Completion.Run == "all_succeeded" && maximumFailed != 0 {
		return invalid("completion", at(path, "completion"))
	}
	return checkBudgets(ctx, spec, defaults, func(relative string) string { return at(path, relative) })
}

func resolve(setting Setting, values map[string]Decimal) (int64, bool) {
	if setting.Value != nil {
		value, _ := decimal(string(*setting.Value), "/")
		return value, true
	}
	text, ok := values[setting.Control]
	if !ok {
		return 0, false
	}
	value, _ := decimal(string(text), "/")
	return value, true
}

// The caller locates a violation in its specification or concrete mode values.
func checkBudgets(ctx context.Context, spec *Specification, values map[string]Decimal, locate func(string) string) error {
	run, knownRun := resolve(spec.Limits.RunTimeoutMS, values)
	cleanup, knownCleanup := resolve(spec.Limits.CleanupReserveMS, values)
	if knownRun && knownCleanup && cleanup >= run {
		return invalid("budget", locate("limits"))
	}
	chunk, knownChunk := resolve(spec.Limits.ItemsPerChunk, values)
	concurrency, knownConcurrency := resolve(spec.Limits.ChunkItemConcurrency, values)
	if knownChunk && knownConcurrency && concurrency > chunk {
		return invalid("budget", locate("limits"))
	}
	for _, nodeName := range keys(spec.Nodes) {
		if err := canceled(ctx); err != nil {
			return err
		}
		for _, callName := range keys(spec.Nodes[nodeName].Activities) {
			policy := spec.Nodes[nodeName].Activities[callName].Policy
			policyPath := locate("nodes/" + nodeName + "/activities/" + callName + "/policy")
			start, knownStart := resolve(policy.StartToCloseMS, values)
			total, knownTotal := resolve(policy.ScheduleToCloseMS, values)
			initial, knownInitial := resolve(policy.InitialIntervalMS, values)
			maximum, knownMaximum := resolve(policy.MaximumIntervalMS, values)
			if knownStart && knownTotal && start > total || knownInitial && knownMaximum && initial > maximum {
				return invalid("budget", policyPath)
			}
			if policy.HeartbeatMS != nil {
				heartbeat, knownHeartbeat := resolve(*policy.HeartbeatMS, values)
				if knownHeartbeat && knownStart && heartbeat > start {
					return invalid("budget", policyPath)
				}
			}
			if knownTotal && knownRun && knownCleanup && total > run-cleanup {
				return invalid("budget", policyPath)
			}
		}
	}
	for _, name := range keys(spec.Relations) {
		if err := canceled(ctx); err != nil {
			return err
		}
		relation := spec.Relations[name]
		limitText, known := values[relation.Limit]
		if !known {
			continue
		}
		for _, term := range relation.Terms {
			if _, ok := values[term]; !ok {
				known = false
				break
			}
		}
		if !known {
			continue
		}
		limit, _ := decimal(string(limitText), "/")
		sum := int64(0)
		for _, term := range relation.Terms {
			value, _ := decimal(string(values[term]), "/")
			if value > limit-sum {
				return invalid("relation", locate("relations/"+name))
			}
			sum += value
		}
	}
	return nil
}

func normalizeMode(ctx context.Context, spec *Specification, values map[string]Decimal, materialize bool, path string) (map[string]Decimal, error) {
	if values == nil {
		values = map[string]Decimal{}
	}
	for _, name := range keys(values) {
		if _, ok := spec.Controls[name]; !ok {
			return nil, invalid("unknown_control", at(path, "values"))
		}
	}
	for _, name := range keys(spec.Controls) {
		if err := canceled(ctx); err != nil {
			return nil, err
		}
		control := spec.Controls[name]
		text, ok := values[name]
		if !ok {
			if !materialize || control.Default == nil {
				return nil, invalid("missing_control", at(path, "values/"+name))
			}
			text = *control.Default
			values[name] = text
		}
		value, err := decimal(string(text), at(path, "values/"+name))
		if err != nil {
			return nil, err
		}
		minimum, _ := decimal(string(control.Minimum), path)
		maximum, _ := decimal(string(control.Maximum), path)
		if value < minimum || value > maximum {
			return nil, invalid("control_value", at(path, "values/"+name))
		}
	}
	if err := checkBudgets(ctx, spec, values, func(string) string { return at(path, "values") }); err != nil {
		return nil, err
	}
	return values, nil
}
