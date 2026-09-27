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
	"unicode"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

func renderText(bindings *i18n.Bindings, locale, key string) (string, error) {
	selection, err := bindings.Resolve("fathomry.cli.project:"+key, locale)
	if err != nil {
		return "", err
	}
	result, err := selection.Render(nil)
	if err != nil {
		return "", err
	}
	for _, char := range result.Text {
		if unicode.IsControl(char) {
			return "", failed(ErrPresentation)
		}
	}
	return result.Text, nil
}

// presentationKey uses only this operation's observation and selected core.
// Unknown is not evidence of either success or creation not having started.
func (result creationResult) presentationKey() (string, error) {
	if result.observation < untouched || result.observation > complete || result.err == nil && result.observation != complete {
		return "", failed(ErrPresentation)
	}
	if result.err == nil {
		return "created", nil
	}
	if result.observation == complete {
		return "completeDelivery", nil
	}
	if result.observation == partial {
		if core, ok := result.err.(*failure.Error); ok && core != nil && core.Diagnostic().Condition == ErrCreation {
			return conditionKey(ErrCreation), nil
		}
		return "partial", nil
	}
	if result.observation == untouched {
		if core, ok := result.err.(*failure.Error); ok && core != nil {
			if key := conditionKey(core.Diagnostic().Condition); key != "" && core.Diagnostic().Condition != ErrCreation {
				return key, nil
			}
		}
	}
	return "notStarted", nil
}
