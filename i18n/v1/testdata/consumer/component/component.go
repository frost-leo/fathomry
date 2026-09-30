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

package component

import (
	"embed"
	"errors"
	"time"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

//go:embed messages/*.json
var resources embed.FS

const SourceFailed failure.Code = 0xA4410001

type details struct {
	label string
	stamp time.Time
}

func definition() failure.Definition {
	return failure.Definition{Code: SourceFailed, Identifier: "example.source.failed", Module: "example", Component: "source", Revision: 1, Message: "The source failed.",
		Details: failure.Contract{ID: "example.source.details_contract", Version: 1}}
}

// Bundle supplies owned resources and an explicit private-data projection.
func Bundle() i18n.Component {
	return i18n.Component{Module: "example", Name: "source", BaseLocale: "en", Resources: resources, Directory: "messages", Definitions: []failure.Definition{definition()},
		Bindings: []i18n.Binding{{Code: SourceFailed, Message: "example.source.details", MessageContract: "v1", Details: definition().Details, Project: func(value error) (i18n.Input, error) {
			occurrence, ok := value.(*failure.Detailed[details])
			if !ok {
				return i18n.Input{}, errors.New("wrong direct occurrence")
			}
			fields, ok := occurrence.Details()
			if !ok {
				return i18n.Input{}, errors.New("missing details")
			}
			return i18n.Input{Arguments: []i18n.Argument{{Name: "label", Value: fields.label}}}, nil
		}}},
	}
}

// Fail is a fixture operation with no locale parameter. Native remains inspectable.
func Fail(presenter i18n.Presenter, native error) error {
	occurrence, err := failure.NewDetailed(definition(), failure.Location{Operation: "read"}, details{label: "public", stamp: time.Unix(100, 0)},
		func(value details) details { return value }, native)
	if err != nil {
		return err
	}
	return presenter.Present(occurrence)
}
