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

package errorcatalog

import (
	"context"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

type explanation struct {
	Definition      failure.Definition `json:"definition"`
	Domain          failure.Domain     `json:"domain"`
	Facility        failure.Facility   `json:"facility"`
	Number          uint16             `json:"number"`
	Message         string             `json:"message"`
	RequestedLocale string             `json:"requested_locale"`
	CanonicalLocale string             `json:"canonical_locale"`
	MatchedLocale   string             `json:"matched_locale"`
	Locale          string             `json:"locale"`
	Fallback        string             `json:"fallback,omitempty"`
}

func describe(value i18n.Explanation) explanation {
	return explanation{
		Definition: value.Definition, Domain: value.Definition.Code.Domain(),
		Facility: value.Definition.Code.Facility(), Number: value.Definition.Code.Number(),
		Message: value.Message.Text, RequestedLocale: value.Selection.Requested,
		CanonicalLocale: value.Selection.Canonical, MatchedLocale: value.Selection.Matched,
		Locale: value.Message.Locale, Fallback: string(value.Selection.Fallback),
	}
}

func listErrors(ctx context.Context, catalogs command.Catalogs, input listOptions) ([]explanation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	options, err := prepareList(input)
	if err != nil {
		return nil, err
	}
	definitions, err := catalogs.Errors.Inspect()
	if err != nil {
		return nil, err
	}
	result := make([]explanation, 0, len(definitions))
	ownerFound := false
	for _, definition := range definitions {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !options.owners.Match(definition.Module, definition.Component) {
			continue
		}
		ownerFound = true
		value, found, err := catalogs.Messages.Explain(definition.Code, options.locale)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, command.Fail(command.ErrNotFound)
		}
		if command.ContainsQuery(options.query, definition.Code.String(), string(definition.Identifier),
			definition.Message, definition.Description, value.Message.Text) {
			result = append(result, describe(value))
		}
	}
	if err := options.owners.RequireMatch(ownerFound); err != nil {
		return nil, err
	}
	return result, nil
}

func explainError(ctx context.Context, catalogs command.Catalogs, identity, locale string) (explanation, error) {
	if err := ctx.Err(); err != nil {
		return explanation{}, err
	}
	options, err := prepareExplanation(identity, locale)
	if err != nil {
		return explanation{}, err
	}
	var definition failure.Definition
	var found bool
	if options.identifier != "" {
		definition, found, err = catalogs.Errors.LookupIdentifier(options.identifier)
	} else {
		definition, found, err = catalogs.Errors.Lookup(options.code)
	}
	if err != nil {
		return explanation{}, err
	}
	if !found {
		return explanation{}, command.Fail(command.ErrNotFound)
	}
	value, found, err := catalogs.Messages.Explain(definition.Code, options.locale)
	if err != nil {
		return explanation{}, err
	}
	if !found {
		return explanation{}, command.Fail(command.ErrNotFound)
	}
	return describe(value), nil
}

func listComponents(ctx context.Context, catalog *failure.Catalog, owners command.OwnerFilter) ([]failure.Component, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := owners.Validate(); err != nil {
		return nil, err
	}
	values, err := catalog.Components()
	if err != nil {
		return nil, err
	}
	result := make([]failure.Component, 0, len(values))
	for _, value := range values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if owners.Match(value.Module, value.Name) {
			result = append(result, value)
		}
	}
	if err := owners.RequireMatch(len(result) != 0); err != nil {
		return nil, err
	}
	return result, nil
}
