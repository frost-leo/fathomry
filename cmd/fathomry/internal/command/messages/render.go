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
	"errors"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/i18n/v1"
)

type rendered struct {
	ID              string    `json:"id"`
	Text            string    `json:"text"`
	RequestedLocale string    `json:"requested_locale"`
	CanonicalLocale string    `json:"canonical_locale"`
	MatchedLocale   string    `json:"matched_locale"`
	Locale          string    `json:"locale"`
	Fallback        string    `json:"fallback,omitempty"`
	Category        i18n.Form `json:"category"`
	Variant         i18n.Form `json:"variant"`
	FormFallback    bool      `json:"form_fallback"`
}

func renderMessage(ctx context.Context, catalog *i18n.Catalog, id, locale string, options renderOptions) (rendered, error) {
	if err := ctx.Err(); err != nil {
		return rendered{}, err
	}
	selection, err := catalog.Resolve(id, locale)
	if err != nil {
		switch {
		case errors.Is(err, i18n.ErrMessage):
			return rendered{}, command.Fail(command.ErrNotFound, err)
		case errors.Is(err, i18n.ErrLimit):
			return rendered{}, command.Fail(command.ErrLimit, err)
		default:
			return rendered{}, command.Fail(command.ErrUsage, err)
		}
	}
	info, err := selection.Info()
	if err != nil {
		return rendered{}, err
	}
	exact, err := catalog.Lookup(id, info.Locale)
	if err != nil {
		return rendered{}, err
	}
	if !exact.TranslationExists {
		return rendered{}, command.Fail(command.ErrNotFound)
	}
	input, err := prepareRender(options, exact.Definition.Arguments)
	if err != nil {
		return rendered{}, err
	}
	value, err := selection.Render(input.Arguments, input.Count)
	if err != nil {
		if errors.Is(err, i18n.ErrLimit) {
			return rendered{}, command.Fail(command.ErrLimit, err)
		}
		return rendered{}, command.Fail(command.ErrUsage, err)
	}
	if err := ctx.Err(); err != nil {
		return rendered{}, err
	}
	return rendered{ID: id, Text: value.Text, RequestedLocale: info.Requested,
		CanonicalLocale: info.Canonical, MatchedLocale: info.Matched, Locale: value.Locale,
		Fallback: string(info.Fallback), Category: value.Category, Variant: value.Variant, FormFallback: value.FormFallback}, nil
}
