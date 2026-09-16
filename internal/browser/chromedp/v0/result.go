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

package chromedp

// Result is immutable technical evidence. CallbackCompleted says the callback
// returned nil before its deadline, not that browser traffic stopped or data is
// complete. ContextReleased is positive disposal evidence, independently of the
// invocation receipt's local producer release. Saved data has caller-chosen meaning.
type Result struct {
	private
	data *resultData
}
type resultData struct {
	callbackCompleted, contextReleased bool
	commands, navigations              uint64
	requestEvents                      uint64
	eventOverflow                      bool
	output                             map[string][]byte
}

func (result Result) CallbackCompleted() bool {
	return result.data != nil && result.data.callbackCompleted
}
func (result Result) ContextReleased() bool { return result.data != nil && result.data.contextReleased }
func (result Result) Commands() uint64 {
	if result.data == nil {
		return 0
	}
	return result.data.commands
}
func (result Result) Navigations() uint64 {
	if result.data == nil {
		return 0
	}
	return result.data.navigations
}

// RequestEvents counts observed target Network.requestWillBeSent events only.
// Redirects, cache/service workers, other targets and background traffic prevent
// interpreting this as an exact physical HTTP count. Attempts stay inexact zero.
func (result Result) RequestEvents() uint64 {
	if result.data == nil {
		return 0
	}
	return result.data.requestEvents
}
func (result Result) EventOverflow() bool { return result.data != nil && result.data.eventOverflow }

// DataCopy distinguishes a missing key (false) from explicit empty output (true).
func (result Result) DataCopy(name string) ([]byte, bool) {
	if result.data == nil {
		return nil, false
	}
	data, ok := result.data.output[name]
	if !ok {
		return nil, false
	}
	return append([]byte{}, data...), true
}

// Event contains a frozen native event encoded with the selected CDP bindings.
// Type is the native Go event type, not a URL, physical request or business result.
// Native payloads may be sensitive; only deliberate DataCopy inspection exposes them.
type Event struct {
	private
	name string
	data []byte
}

func (event Event) Type() string     { return event.name }
func (event Event) DataCopy() []byte { return append([]byte(nil), event.data...) }
