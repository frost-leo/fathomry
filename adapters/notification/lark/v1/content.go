/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package lark

import native "github.com/frost-leo/fathomry/internal/notification/lark/v3"

// JSON is a frozen bounded object/array. It preserves exact numbers, native fields,
// null and empty collections. Bytes returns a detached, sensitive copy.
type JSON struct {
	private
	native native.JSON
}

func NewJSON(data []byte) (JSON, error) {
	value, err := native.NewJSON(data)
	return JSON{native: value}, translate(err, "json")
}
func (value JSON) Bytes() []byte { return value.native.Bytes() }

// Content retains native message/card representation without presentation policy.
type Content struct {
	private
	native native.Content
}

func NewContent(kind string, data []byte) (Content, error) {
	value, err := native.NewContent(kind, data)
	return Content{native: value}, translate(err, "content")
}
func (value Content) Type() string { return value.native.Type() }
func (value Content) JSON() JSON   { return JSON{native: value.native.JSON()} }
func Text(text string) (Content, error) {
	value, err := native.Text(text)
	return Content{native: value}, translate(err, "text")
}
func Card(data []byte) (Content, error) {
	value, err := native.Card(data)
	return Content{native: value}, translate(err, "card")
}
func Template(id, version string, variables JSON) (Content, error) {
	value, err := native.Template(id, version, variables.native)
	return Content{native: value}, translate(err, "template")
}
func CardInstance(id string) (Content, error) {
	value, err := native.CardInstance(id)
	return Content{native: value}, translate(err, "card-instance")
}
func Chart(id string, spec JSON) (JSON, error) {
	value, err := native.Chart(id, spec.native)
	return JSON{native: value}, translate(err, "chart")
}
func Table(id string, columns, rows JSON) (JSON, error) {
	value, err := native.Table(id, columns.native, rows.native)
	return JSON{native: value}, translate(err, "table")
}

// CardOptions leaves styling and update behavior caller-owned; nil omits.
type CardOptions struct {
	HeaderTemplate string
	UpdateMulti    *bool
}

func ComposeCard(title string, options CardOptions, elements ...JSON) (Content, error) {
	if len(elements) > 200 {
		return Content{}, fail(ErrLimit, "elements")
	}
	items := make([]native.JSON, len(elements))
	for index, element := range elements {
		items[index] = element.native
	}
	value, err := native.ComposeCardWithOptions(title, native.CardOptions{HeaderTemplate: options.HeaderTemplate, UpdateMulti: options.UpdateMulti}, items...)
	return Content{native: value}, translate(err, "compose-card")
}

type Recipient struct {
	private
	Type, ID string
}
type Page struct {
	private
	Size  int
	Token string
}
type Revision struct {
	private
	Sequence int
	UUID     string
}
