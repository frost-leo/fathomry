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
	"slices"
)

func normalizeGraph(ctx context.Context, spec *Specification, path string) error {
	usedContracts, usedBindings := map[string]bool{}, map[string]bool{}
	checkPort := func(port Port, path string) error {
		if _, ok := spec.Contracts[port.Contract]; !ok {
			return invalid("contract_ref", path)
		}
		usedContracts[port.Contract] = true
		return nil
	}
	for _, name := range keys(spec.Inputs) {
		if err := checkPort(spec.Inputs[name], at(path, "inputs/"+name)); err != nil {
			return err
		}
	}
	bindingIdentities := map[[2]string]bool{}
	for _, name := range keys(spec.Bindings) {
		if err := canceled(ctx); err != nil {
			return err
		}
		binding := spec.Bindings[name]
		identity := [2]string{binding.Type, binding.Revision}
		if bindingIdentities[identity] {
			return invalid("binding_identity", at(path, "bindings/"+name))
		}
		bindingIdentities[identity] = true
		if binding.RetrySafety == "read_only" && len(binding.Effects) != 0 {
			return invalid("retry_safety", at(path, "bindings/"+name))
		}
		for _, direction := range []struct {
			name  string
			ports map[string]Port
		}{{"inputs", binding.Inputs}, {"outputs", binding.Outputs}} {
			for _, portName := range keys(direction.ports) {
				if err := checkPort(direction.ports[portName], at(path, "bindings/"+name+"/"+direction.name+"/"+portName)); err != nil {
					return err
				}
			}
		}
		for _, guarantees := range binding.Effects {
			slices.Sort(guarantees)
		}
	}
	names := keys(spec.Nodes)
	nodeIndex := make(map[string]int, len(names))
	incoming, successors := map[string][]string{}, map[string][]string{}
	ancestors := make([][]uint64, len(names))
	for index, name := range names {
		nodeIndex[name] = index
		ancestors[index] = make([]uint64, (len(names)+63)/64)
	}
	for _, name := range keys(spec.Edges) {
		if err := canceled(ctx); err != nil {
			return err
		}
		edge := spec.Edges[name]
		if _, ok := spec.Nodes[edge.From]; !ok {
			return invalid("edge_ref", at(path, "edges/"+name))
		}
		if _, ok := spec.Nodes[edge.To]; !ok {
			return invalid("edge_ref", at(path, "edges/"+name))
		}
		if edge.From == edge.To {
			return invalid("cycle", at(path, "edges/"+name))
		}
		for _, route := range edge.On {
			if !slices.Contains(spec.Nodes[edge.From].Routes, route) {
				return invalid("edge_route", at(path, "edges/"+name))
			}
		}
		slices.Sort(edge.On)
		incoming[edge.To] = append(incoming[edge.To], name)
		successors[edge.From] = append(successors[edge.From], edge.To)
	}
	indegree := map[string]int{}
	ready := []string{}
	for _, name := range names {
		indegree[name] = len(incoming[name])
		if indegree[name] == 0 {
			ready = append(ready, name)
		}
	}
	order := make([]string, 0, len(names))
	for len(ready) > 0 {
		if err := canceled(ctx); err != nil {
			return err
		}
		name := ready[0]
		ready = ready[1:]
		order = append(order, name)
		owner := nodeIndex[name]
		for _, target := range successors[name] {
			row := ancestors[nodeIndex[target]]
			row[owner/64] |= uint64(1) << uint(owner%64)
			for block, bits := range ancestors[owner] {
				row[block] |= bits
			}
			indegree[target]--
			if indegree[target] == 0 {
				ready = append(ready, target)
			}
		}
		slices.Sort(ready)
	}
	if len(order) != len(names) {
		return invalid("cycle", at(path, "edges"))
	}
	calls := 0
	for _, node := range spec.Nodes {
		calls += len(node.Activities)
	}
	if calls > 4096 {
		return problem(ErrLimit, "calls", at(path, "nodes"))
	}
	exported := map[string]map[string]Port{}
	for _, name := range order {
		if err := canceled(ctx); err != nil {
			return err
		}
		node := spec.Nodes[name]
		nodePath := at(path, "nodes/"+name)
		phases := map[string]int{}
		for index, phase := range node.Phases {
			slices.Sort(phase)
			for _, call := range phase {
				if _, ok := node.Activities[call]; !ok {
					return invalid("phase_membership", at(nodePath, "phases"))
				}
				if _, ok := phases[call]; ok {
					return invalid("phase_membership", at(nodePath, "phases"))
				}
				phases[call] = index
			}
		}
		if len(phases) != len(node.Activities) {
			return invalid("phase_membership", at(nodePath, "phases"))
		}
		slices.Sort(node.Routes)
		if node.Join == "all" {
			intersections := map[string][]string{}
			for _, edgeName := range incoming[name] {
				edge := spec.Edges[edgeName]
				intersection, seen := intersections[edge.From]
				if !seen {
					intersection = slices.Clone(edge.On)
				} else {
					intersection = slices.DeleteFunc(intersection, func(route string) bool { return !slices.Contains(edge.On, route) })
				}
				if len(intersection) == 0 {
					return invalid("impossible_join", at(nodePath, "join"))
				}
				intersections[edge.From] = intersection
			}
		}
		for _, portName := range keys(node.Inputs) {
			input := node.Inputs[portName]
			portPath := at(nodePath, "inputs/"+portName)
			target := Port{Contract: input.Contract, Delivery: input.Delivery, Required: input.Required}
			if err := checkPort(target, portPath); err != nil {
				return err
			}
			var source Port
			switch input.Source.Kind {
			case "root":
				var ok bool
				source, ok = spec.Inputs[input.Source.Port]
				if !ok {
					return invalid("input_ref", portPath)
				}
			case "edge":
				if !slices.Contains(incoming[name], input.Source.Edge) {
					return invalid("input_ref", portPath)
				}
				producer := spec.Edges[input.Source.Edge].From
				var ok bool
				source, ok = exported[producer][input.Source.Port]
				if !ok {
					return invalid("input_ref", portPath)
				}
				if node.Join == "any" && len(incoming[name]) > 1 {
					source.Required = false
				}
			default:
				return invalid("source_kind", portPath)
			}
			if err := compatiblePort(source, target, portPath); err != nil {
				return err
			}
		}
		callOutput := func(call, port string) (Port, error) {
			activity, ok := node.Activities[call]
			if !ok {
				return Port{}, invalid("activity_ref", nodePath)
			}
			binding, ok := spec.Bindings[activity.Binding]
			if !ok {
				return Port{}, invalid("binding_ref", at(nodePath, "activities/"+call))
			}
			result, ok := binding.Outputs[port]
			if !ok {
				return Port{}, invalid("output_ref", at(nodePath, "activities/"+call))
			}
			return result, nil
		}
		for _, callName := range keys(node.Activities) {
			call := node.Activities[callName]
			callPath := at(nodePath, "activities/"+callName)
			binding, ok := spec.Bindings[call.Binding]
			if !ok {
				return invalid("binding_ref", callPath)
			}
			usedBindings[call.Binding] = true
			for _, arg := range keys(binding.Inputs) {
				if _, ok := call.Inputs[arg]; !ok && binding.Inputs[arg].Required {
					return invalid("call_input", at(callPath, "inputs"))
				}
			}
			for _, arg := range keys(call.Inputs) {
				target, ok := binding.Inputs[arg]
				if !ok {
					return invalid("call_input", at(callPath, "inputs"))
				}
				reference := call.Inputs[arg]
				var source Port
				switch reference.Kind {
				case "input":
					input, ok := node.Inputs[reference.Port]
					if !ok {
						return invalid("input_ref", at(callPath, "inputs/"+arg))
					}
					source = Port{Contract: input.Contract, Delivery: input.Delivery, Required: input.Required}
				case "activity":
					var err error
					source, err = callOutput(reference.Activity, reference.Port)
					if err != nil {
						return err
					}
					if phases[reference.Activity] >= phases[callName] {
						return invalid("phase_dependency", at(callPath, "inputs/"+arg))
					}
				default:
					return invalid("source_kind", at(callPath, "inputs/"+arg))
				}
				if err := compatiblePort(source, target, at(callPath, "inputs/"+arg)); err != nil {
					return err
				}
			}
		}
		exported[name] = map[string]Port{}
		for _, portName := range keys(node.Outputs) {
			source := node.Outputs[portName]
			if source.Kind != "activity" {
				return invalid("source_kind", at(nodePath, "outputs/"+portName))
			}
			port, err := callOutput(source.Activity, source.Port)
			if err != nil {
				return err
			}
			exported[name][portName] = port
		}
		if node.Route.Kind == "fixed" {
			if len(node.Routes) != 1 || node.Routes[0] != node.Route.Value {
				return invalid("route_contract", at(nodePath, "route"))
			}
		} else {
			port, err := callOutput(node.Route.Activity, node.Route.Port)
			if err != nil {
				return err
			}
			schema := spec.Contracts[port.Contract]
			if port.Delivery != "value" || schema.Type != "string" || !sameStrings(schema.Enum, node.Routes) {
				return invalid("route_contract", at(nodePath, "route"))
			}
		}
		for _, effectName := range keys(node.RequiredEffects) {
			effect := node.RequiredEffects[effectName]
			call, ok := node.Activities[effect.Activity]
			if !ok {
				return invalid("effect_ref", at(nodePath, "required_effects/"+effectName))
			}
			if !slices.Contains(spec.Bindings[call.Binding].Effects[effect.Effect], effect.Guarantee) {
				return invalid("effect_ref", at(nodePath, "required_effects/"+effectName))
			}
		}
		if node.Gate != nil {
			slices.Sort(node.Gate.Nodes)
			for _, observed := range node.Gate.Nodes {
				index, ok := nodeIndex[observed]
				if !ok || ancestors[nodeIndex[name]][index/64]&(uint64(1)<<uint(index%64)) == 0 {
					return invalid("gate_dependency", at(nodePath, "gate"))
				}
			}
		}
	}
	for _, name := range keys(spec.Outputs) {
		output := spec.Outputs[name]
		if _, ok := exported[output.Node][output.Port]; !ok {
			return invalid("export_ref", at(path, "outputs/"+name))
		}
	}
	for _, name := range keys(spec.Contracts) {
		if !usedContracts[name] {
			return invalid("unused_contract", at(path, "contracts/"+name))
		}
	}
	for _, name := range keys(spec.Bindings) {
		if !usedBindings[name] {
			return invalid("unused_binding", at(path, "bindings/"+name))
		}
	}
	return nil
}

func compatiblePort(source, target Port, path string) error {
	if source.Contract != target.Contract || source.Delivery != target.Delivery {
		return invalid("port_contract", path)
	}
	if target.Required && !source.Required {
		return invalid("port_presence", path)
	}
	return nil
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for _, value := range left {
		if !slices.Contains(right, value) {
			return false
		}
	}
	return true
}
