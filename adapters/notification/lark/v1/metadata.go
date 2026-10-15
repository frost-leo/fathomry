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

// Profile reports declared technical modes, not service, subscription or
// recipient-rendering readiness. Option values are deliberately inspectable.
type Profile struct {
	private
	SDKMode, ServiceMode, Protocol string
	Options                        []ProfileOption
}
type ProfileOption struct{ Name, Value string }

func (owner *Owner) Profile() Profile { return owner.Handle().Profile() }
func (handle Handle) Profile() Profile {
	if handle.state == nil || handle.state.inspection == nil {
		return Profile{}
	}
	value := handle.state.inspection.Profile()
	result := Profile{SDKMode: value.SDKMode, ServiceMode: value.ServiceMode.Value, Protocol: value.Protocol.Value}
	for _, option := range value.Options {
		result.Options = append(result.Options, ProfileOption{Name: option.Name, Value: option.Value})
	}
	return result
}
