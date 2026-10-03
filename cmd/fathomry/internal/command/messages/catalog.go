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

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/i18n/v1"
)

type parameter struct {
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Meaning string `json:"meaning"`
}
type form struct {
	Category string `json:"category"`
	Pattern  string `json:"pattern"`
}
type entry struct {
	Module       string      `json:"module"`
	Component    string      `json:"component"`
	ID           string      `json:"id"`
	Locale       string      `json:"locale"`
	BaseLocale   string      `json:"base_locale"`
	Contract     string      `json:"contract"`
	Context      string      `json:"context"`
	SourceDigest string      `json:"source_digest"`
	Cardinal     bool        `json:"cardinal"`
	Arguments    []parameter `json:"arguments"`
	Forms        []form      `json:"forms"`
}

func describe(value i18n.Definition) entry {
	result := entry{Module: value.Module, Component: value.Component, ID: value.ID, Locale: value.Locale,
		BaseLocale: value.BaseLocale, Contract: value.Contract, Context: value.Context,
		SourceDigest: value.SourceDigest, Cardinal: value.Cardinal, Arguments: []parameter{}, Forms: []form{}}
	for _, argument := range value.Arguments {
		result.Arguments = append(result.Arguments, parameter{argument.Name, argument.Kind, argument.Meaning})
	}
	for _, variant := range value.Forms {
		result.Forms = append(result.Forms, form{string(variant.Category), variant.Pattern})
	}
	return result
}

type lookup struct {
	MessageExists     bool   `json:"message_exists"`
	TranslationExists bool   `json:"translation_exists"`
	RequestedLocale   string `json:"requested_locale"`
	CanonicalLocale   string `json:"canonical_locale"`
	Definition        *entry `json:"definition,omitempty"`
}
type coverage struct {
	Module     string   `json:"module"`
	Component  string   `json:"component"`
	Locale     string   `json:"locale"`
	Total      int      `json:"total"`
	Translated int      `json:"translated"`
	Missing    []string `json:"missing"`
}
type coverageReport struct {
	Locale     string     `json:"locale"`
	Total      int        `json:"total"`
	Translated int        `json:"translated"`
	Missing    int        `json:"missing"`
	Complete   bool       `json:"complete"`
	Components []coverage `json:"components"`
}
type localeCount struct {
	Locale     string `json:"locale"`
	Translated int    `json:"translated"`
}
type localeOwner struct {
	Module     string        `json:"module"`
	Component  string        `json:"component"`
	BaseLocale string        `json:"base_locale"`
	Total      int           `json:"total"`
	Locales    []localeCount `json:"locales"`
}
type owner struct{ module, component string }

func selectComponents(ctx context.Context, catalog *i18n.Catalog, owners command.OwnerFilter) ([]i18n.ComponentInfo, error) {
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
	result := make([]i18n.ComponentInfo, 0, len(values))
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

func listMessages(ctx context.Context, catalog *i18n.Catalog, input listOptions) ([]entry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	options, err := prepareList(input)
	if err != nil {
		return nil, err
	}
	if _, err := selectComponents(ctx, catalog, options.owners); err != nil {
		return nil, err
	}
	values, err := catalog.Inspect()
	if err != nil {
		return nil, err
	}
	result := make([]entry, 0, len(values))
	for _, value := range values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !options.owners.Match(value.Module, value.Component) || options.locale != nil && value.Locale != *options.locale {
			continue
		}
		matched := command.ContainsQuery(options.query, value.ID, value.Context)
		for _, variant := range value.Forms {
			matched = matched || command.ContainsQuery(options.query, variant.Pattern)
		}
		if matched {
			result = append(result, describe(value))
		}
	}
	return result, nil
}

func lookupMessage(ctx context.Context, catalog *i18n.Catalog, id, locale string) (lookup, error) {
	if err := ctx.Err(); err != nil {
		return lookup{}, err
	}
	canonical, err := canonicalLocale(locale)
	if err != nil {
		return lookup{}, err
	}
	value, err := catalog.Lookup(id, locale)
	if err != nil {
		return lookup{}, command.Fail(command.ErrUsage, err)
	}
	if !value.MessageExists {
		return lookup{}, command.Fail(command.ErrNotFound)
	}
	result := lookup{MessageExists: value.MessageExists, TranslationExists: value.TranslationExists,
		RequestedLocale: locale, CanonicalLocale: canonical}
	if value.TranslationExists {
		item := describe(value.Definition)
		result.Definition = &item
	}
	return result, nil
}

func inspectLocales(ctx context.Context, catalog *i18n.Catalog, owners command.OwnerFilter) ([]localeOwner, error) {
	components, err := selectComponents(ctx, catalog, owners)
	if err != nil {
		return nil, err
	}
	values, err := catalog.Inspect()
	if err != nil {
		return nil, err
	}
	counts := map[owner]map[string]int{}
	for _, value := range values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !owners.Match(value.Module, value.Component) {
			continue
		}
		key := owner{value.Module, value.Component}
		if counts[key] == nil {
			counts[key] = map[string]int{}
		}
		counts[key][value.Locale]++
	}
	result := make([]localeOwner, 0, len(components))
	for _, component := range components {
		item := localeOwner{Module: component.Module, Component: component.Name,
			BaseLocale: component.BaseLocale, Total: len(component.Messages), Locales: []localeCount{}}
		for _, locale := range component.Locales {
			item.Locales = append(item.Locales, localeCount{locale, counts[owner{component.Module, component.Name}][locale]})
		}
		result = append(result, item)
	}
	return result, nil
}

func inspectCoverage(ctx context.Context, catalog *i18n.Catalog, owners command.OwnerFilter, locale string) (coverageReport, error) {
	components, err := selectComponents(ctx, catalog, owners)
	if err != nil {
		return coverageReport{}, err
	}
	values, err := catalog.Coverage(locale)
	if err != nil {
		return coverageReport{}, command.Fail(command.ErrUsage, err)
	}
	totals := map[owner]int{}
	for _, component := range components {
		totals[owner{component.Module, component.Name}] = len(component.Messages)
	}
	result := coverageReport{Complete: true, Components: []coverage{}}
	for _, value := range values {
		if err := ctx.Err(); err != nil {
			return coverageReport{}, err
		}
		total, selected := totals[owner{value.Module, value.Component}]
		if !selected {
			continue
		}
		result.Locale = value.Locale
		translated := total - len(value.Missing)
		result.Total += total
		result.Translated += translated
		result.Missing += len(value.Missing)
		result.Components = append(result.Components, coverage{Module: value.Module, Component: value.Component,
			Locale: value.Locale, Total: total, Translated: translated, Missing: append([]string{}, value.Missing...)})
	}
	result.Complete = result.Missing == 0
	return result, nil
}
