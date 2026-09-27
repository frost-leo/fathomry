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

package project

import (
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

// Definition supplies this command's declarations to the explicit CLI composition.
func Definition() failure.ModuleDefinition {
	module := failure.ModuleDefinition{ID: "fathomry.cli.project", Source: "cli.project"}
	for _, condition := range []failure.Condition{ErrArguments, ErrIdentity, ErrSource, ErrDestination, ErrExists, ErrOverlap, ErrPreparation, ErrCreation, ErrPresentation} {
		module.Conditions = append(module.Conditions, failure.ConditionDefinition{Condition: condition, Contract: "v1"})
	}
	module.Contracts = []failure.FactContract{{
		ID: "fathomry.cli.project:creation", Revision: "v1", Use: failure.PresentationInput, Access: failure.OwnerFacts,
		Fields: []failure.FactField{{Name: "observation", Kind: failure.EnumFact, Required: true, UnknownAllowed: true, Values: []string{"unknown", "not_started", "unconfirmed", "completed"}}},
	}}
	return module
}

// Bindings describes only supported owned associations; it never extracts errors.
func Bindings() []i18n.Binding {
	var result []i18n.Binding
	for _, key := range requiredText {
		result = append(result, i18n.Binding{ID: "fathomry.cli.project:" + key, Surface: "cli", Role: "text", Message: "fathomry.cli.project:" + key, MessageContract: "v1"})
	}
	for _, condition := range Definition().Conditions {
		key := conditionKey(condition.Condition)
		message := "notStarted"
		switch condition.Condition {
		case ErrSource:
			message = "sourceUnavailable"
		case ErrExists:
			message = "destinationExists"
		case ErrCreation:
			message = "partial"
		}
		result = append(result, i18n.Binding{
			ID: "fathomry.cli.project:" + key, Condition: condition.Condition, ConditionContract: condition.Contract,
			Input: "fathomry.cli.project:creation", InputRevision: "v1", Surface: "cli", Role: "explanation",
			Message: "fathomry.cli.project:" + message, MessageContract: "v1",
		})
	}
	return result
}

func conditionKey(condition failure.Condition) string {
	switch condition {
	case ErrArguments:
		return "invalid_arguments"
	case ErrIdentity:
		return "conflicting_identity"
	case ErrSource:
		return "source_unusable"
	case ErrDestination:
		return "destination_unusable"
	case ErrExists:
		return "destination_exists"
	case ErrOverlap:
		return "source_destination_overlap"
	case ErrPreparation:
		return "preparation_failed"
	case ErrCreation:
		return "creation_incomplete"
	case ErrPresentation:
		return "invalid_presentation"
	}
	return ""
}
