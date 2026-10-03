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
	"context"
	"fmt"
	"io"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
)

func writeList(ctx context.Context, writer io.Writer, values []entry) error {
	for _, value := range values {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(writer, "%s  %s\n", value.ID, value.Locale); err != nil {
			return err
		}
	}
	return nil
}

func writeLookup(writer io.Writer, invocation *command.Invocation, id string, result lookup) error {
	if _, err := fmt.Fprintf(writer, "%s  %s\n", id, result.CanonicalLocale); err != nil {
		return err
	}
	if !result.TranslationExists {
		_, err := fmt.Fprintln(writer, invocation.Text("fathomry.message_catalog.missing_translation"))
		return err
	}
	item := result.Definition
	fields := []struct {
		label string
		value any
	}{
		{"owner", item.Module + "." + item.Component}, {"base_locale", item.BaseLocale},
		{"contract", item.Contract}, {"context", item.Context},
		{"source_digest", item.SourceDigest}, {"cardinal", item.Cardinal},
	}
	for _, field := range fields {
		if _, err := fmt.Fprintf(writer, "%s: %v\n", invocation.Text("fathomry.message_catalog."+field.label), field.value); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(writer, "%s: %d\n", invocation.Text("fathomry.message_catalog.arguments"), len(item.Arguments)); err != nil {
		return err
	}
	for _, argument := range item.Arguments {
		if _, err := fmt.Fprintf(writer, "  %s (%s): %s\n", argument.Name, argument.Kind, argument.Meaning); err != nil {
			return err
		}
	}
	for _, variant := range item.Forms {
		if _, err := fmt.Fprintf(writer, "%s  %s\n", variant.Category, variant.Pattern); err != nil {
			return err
		}
	}
	return nil
}

func writeLocales(writer io.Writer, invocation *command.Invocation, values []localeOwner) error {
	for _, item := range values {
		if _, err := fmt.Fprintf(writer, "%s.%s  %s: %s\n", item.Module, item.Component,
			invocation.Text("fathomry.message_catalog.base_locale"), item.BaseLocale); err != nil {
			return err
		}
		for _, locale := range item.Locales {
			if _, err := fmt.Fprintf(writer, "  %s  %s: %d/%d\n", locale.Locale,
				invocation.Text("fathomry.message_catalog.translated"), locale.Translated, item.Total); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeCoverage(writer io.Writer, invocation *command.Invocation, result coverageReport) error {
	if _, err := fmt.Fprintf(writer, "%s  %s: %d/%d  %s: %d\n", result.Locale,
		invocation.Text("fathomry.message_catalog.translated"), result.Translated, result.Total,
		invocation.Text("fathomry.message_catalog.missing"), result.Missing); err != nil {
		return err
	}
	for _, value := range result.Components {
		if _, err := fmt.Fprintf(writer, "%s.%s  %s: %d/%d  %s: %d\n", value.Module, value.Component,
			invocation.Text("fathomry.message_catalog.translated"), value.Translated, value.Total,
			invocation.Text("fathomry.message_catalog.missing"), len(value.Missing)); err != nil {
			return err
		}
		for _, id := range value.Missing {
			if _, err := fmt.Fprintf(writer, "  %s\n", id); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeRendered(writer io.Writer, invocation *command.Invocation, result rendered) error {
	if _, err := fmt.Fprintln(writer, result.Text); err != nil {
		return err
	}
	fields := []struct {
		label string
		value any
	}{
		{"requested_locale", result.RequestedLocale}, {"canonical_locale", result.CanonicalLocale},
		{"matched_locale", result.MatchedLocale}, {"locale", result.Locale}, {"fallback", result.Fallback},
		{"category", result.Category}, {"variant", result.Variant}, {"form_fallback", result.FormFallback},
	}
	for _, field := range fields {
		if _, err := fmt.Fprintf(writer, "%s: %v\n", invocation.Text("fathomry.message_catalog."+field.label), field.value); err != nil {
			return err
		}
	}
	return nil
}
