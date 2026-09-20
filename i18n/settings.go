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

package i18n

// Settings is pure project configuration for the existing resource-first
// renderer. It contains no catalog, SDK options, locale registry or live handle.
// Projects may compose it in their own typed configuration schema.
type Settings struct {
	// DefaultLocale is an explicit language-script-region tag. Empty or malformed
	// values are invalid; valid unsupported languages use Render's English fallback.
	DefaultLocale string `json:"default_locale"`
}

// DefaultSettings returns independent data using the required English source.
// Projects may override it without changing the renderer's fallback contract.
func DefaultSettings() Settings { return Settings{DefaultLocale: "en"} }

// Validate checks syntax and bounds without loading resources or rendering.
// Success does not certify that a translation exists in a particular catalog.
func (settings Settings) Validate() error {
	if settings.DefaultLocale == "" {
		return problem(InvalidLocale)
	}
	_, err := parseLocale(settings.DefaultLocale)
	return err
}

// Locale returns the canonical invocation override, or the project's configured
// default when override is empty. Both inputs must obey the locale contract.
// Defaults and overrides never change catalog ownership or global process state.
func (settings Settings) Locale(override string) (string, error) {
	if err := settings.Validate(); err != nil {
		return "", err
	}
	if override == "" {
		override = settings.DefaultLocale
	}
	tag, err := parseLocale(override)
	if err != nil {
		return "", err
	}
	return tag.String(), nil
}
