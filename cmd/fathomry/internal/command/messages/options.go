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

package messages

import (
	"strconv"
	"strings"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/i18n/v1"
	"golang.org/x/text/language"
)

type listOptions struct {
	owners command.OwnerFilter
	query  string
	locale *string
}

type renderOptions struct {
	arguments []string
	count     *string
}

func prepareList(input listOptions) (listOptions, error) {
	if err := input.owners.Validate(); err != nil {
		return listOptions{}, err
	}
	if input.locale != nil {
		locale, err := canonicalLocale(*input.locale)
		if err != nil {
			return listOptions{}, err
		}
		input.locale = &locale
	}
	input.query = strings.ToLower(input.query)
	return input, nil
}

func canonicalLocale(value string) (string, error) {
	if err := (i18n.Preferences{Locale: value}).Validate(); err != nil {
		return "", command.Fail(command.ErrUsage, err)
	}
	tag, err := language.BCP47.Parse(value)
	if err != nil {
		return "", command.Fail(command.ErrUsage, err)
	}
	return tag.String(), nil
}

func prepareRender(input renderOptions, parameters []i18n.Parameter) (i18n.Input, error) {
	arguments, err := parseArguments(input.arguments, parameters)
	if err != nil {
		return i18n.Input{}, err
	}
	result := i18n.Input{Arguments: arguments}
	if input.count != nil {
		number, err := strconv.ParseUint(*input.count, 10, 64)
		if err != nil {
			return i18n.Input{}, command.Fail(command.ErrUsage, err)
		}
		result.Count = &number
	}
	return result, nil
}

func parseArguments(values []string, parameters []i18n.Parameter) ([]i18n.Argument, error) {
	if len(values) > i18n.MaxArguments {
		return nil, command.Fail(command.ErrLimit)
	}
	kinds := make(map[string]string, len(parameters))
	for _, parameter := range parameters {
		kinds[parameter.Name] = parameter.Kind
	}
	seen := make(map[string]bool, len(values))
	result := make([]i18n.Argument, 0, len(values))
	for _, input := range values {
		name, text, found := strings.Cut(input, "=")
		if !found || seen[name] {
			return nil, command.Fail(command.ErrUsage)
		}
		seen[name] = true
		var value any
		var err error
		switch kinds[name] {
		case "string":
			value = text
		case "int64":
			value, err = strconv.ParseInt(text, 10, 64)
		case "uint64":
			value, err = strconv.ParseUint(text, 10, 64)
		case "bool":
			if text != "true" && text != "false" {
				return nil, command.Fail(command.ErrUsage)
			}
			value = text == "true"
		default:
			return nil, command.Fail(command.ErrUsage)
		}
		if err != nil {
			return nil, command.Fail(command.ErrUsage, err)
		}
		result = append(result, i18n.Argument{Name: name, Value: value})
	}
	return result, nil
}
