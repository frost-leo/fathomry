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

package cli

import (
	"sync"
	"unicode"

	"github.com/frost-leo/fathomry/i18n/v1"
)

type text struct {
	language     string
	catalog      *i18n.Catalog
	bindings     *i18n.Bindings
	boundCatalog *i18n.Catalog
	mutex        sync.Mutex
	err          error
}

func newText() *text {
	catalogs, err := Catalogs()
	return &text{language: "en", catalog: catalogs.Messages, boundCatalog: catalogs.Messages, bindings: catalogs.Bindings, err: err}
}

func (words *text) retain(err error) error {
	words.mutex.Lock()
	defer words.mutex.Unlock()
	if words.err == nil {
		words.err = err
	}
	return err
}

func (words *text) failure() error {
	words.mutex.Lock()
	defer words.mutex.Unlock()
	return words.err
}

func (words *text) render(id, locale string) (string, error) {
	words.mutex.Lock()
	if words.boundCatalog != words.catalog {
		var err error
		words.bindings, err = prepareHostBindings(words.catalog)
		words.boundCatalog = words.catalog
		if words.err == nil {
			words.err = err
		}
	}
	bindings := words.bindings
	words.mutex.Unlock()
	if bindings != nil {
		if _, exists, err := bindings.Lookup(id); err != nil {
			return "", words.retain(err)
		} else if exists {
			selection, err := bindings.Resolve(id, locale)
			if err != nil {
				return "", words.retain(err)
			}
			result, err := selection.Render(nil)
			if err != nil {
				return "", words.retain(err)
			}
			return words.safeText(result.Text)
		}
	}
	selection, err := words.catalog.Resolve(id, locale)
	if err != nil {
		return "", words.retain(err)
	}
	result, err := selection.Render(nil, nil)
	if err != nil {
		return "", words.retain(err)
	}
	return words.safeText(result.Text)
}

func (words *text) safeText(value string) (string, error) {
	for _, char := range value {
		if unicode.IsControl(char) {
			return "", words.retain(hostFailure(ErrDefinition))
		}
	}
	return value, nil
}

func (words *text) get(key string) string {
	value, _ := words.render("fathomry.cli:"+key, words.language)
	return value
}

func (words *text) english(key string) string {
	value, _ := words.render("fathomry.cli:"+key, "en")
	return value
}

func (words *text) validate() error {
	if err := words.failure(); err != nil {
		return err
	}
	definitions, err := words.catalog.Inspect()
	if err != nil {
		return words.retain(err)
	}
	for _, definition := range definitions {
		if definition.Cardinal || len(definition.Arguments) != 0 {
			return words.retain(hostFailure(ErrDefinition))
		}
		for _, form := range definition.Forms {
			for _, char := range form.Pattern {
				if unicode.IsControl(char) {
					return words.retain(hostFailure(ErrDefinition))
				}
			}
		}
	}
	for _, key := range []string{"root", "help", "helpFlag", "language", "usage", "commands", "flags", "invalid", "failed", "canceled"} {
		found, err := words.catalog.Lookup("fathomry.cli:"+key, "en")
		if err != nil {
			return words.retain(err)
		}
		if !found.TranslationExists || found.Definition.Contract != "v1" {
			return words.retain(hostFailure(ErrDefinition))
		}
	}
	return nil
}
