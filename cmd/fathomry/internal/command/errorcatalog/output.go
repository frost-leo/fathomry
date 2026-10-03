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
	"fmt"
	"io"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/failure/v1"
)

func writeList(ctx context.Context, writer io.Writer, values []explanation) error {
	for _, value := range values {
		if err := ctx.Err(); err != nil {
			return err
		}
		locale := value.Locale
		if value.Fallback != "" {
			locale += "; " + value.Fallback
		}
		if _, err := fmt.Fprintf(writer, "%s  %s  [%s]  %s\n",
			value.Definition.Code.String(), value.Definition.Identifier, locale, value.Message); err != nil {
			return err
		}
	}
	return nil
}

func writeExplanation(writer io.Writer, invocation *command.Invocation, result explanation) error {
	if _, err := fmt.Fprintf(writer, "%s  %s\n%s\n", result.Definition.Code.String(),
		result.Definition.Identifier, result.Message); err != nil {
		return err
	}
	fields := []struct {
		label string
		value any
	}{
		{"owner", result.Definition.Module + "." + result.Definition.Component},
		{"domain", result.Domain}, {"facility", fmt.Sprintf("0x%03X", uint16(result.Facility))},
		{"number", result.Number}, {"revision", result.Definition.Revision},
		{"requested_locale", result.RequestedLocale}, {"canonical_locale", result.CanonicalLocale},
		{"matched_locale", result.MatchedLocale}, {"locale", result.Locale}, {"fallback", result.Fallback},
	}
	for _, field := range fields {
		if _, err := fmt.Fprintf(writer, "%s: %v\n", invocation.Text("fathomry.error_catalog."+field.label), field.value); err != nil {
			return err
		}
	}
	if result.Definition.Description != "" {
		if _, err := fmt.Fprintf(writer, "%s: %s\n", invocation.Text("fathomry.error_catalog.description"), result.Definition.Description); err != nil {
			return err
		}
	}
	if result.Definition.Details.ID != "" {
		_, err := fmt.Fprintf(writer, "%s: %s / %d\n", invocation.Text("fathomry.error_catalog.details"),
			result.Definition.Details.ID, result.Definition.Details.Version)
		return err
	}
	return nil
}

func writeComponents(writer io.Writer, values []failure.Component) error {
	for _, value := range values {
		if _, err := fmt.Fprintf(writer, "%s.%s  %s  0x%03X  %d\n", value.Module, value.Name, value.Domain, uint16(value.Facility), len(value.Codes)); err != nil {
			return err
		}
	}
	return nil
}
