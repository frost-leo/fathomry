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

import (
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/frost-leo/fathomry/internal/fault"
)

const ProviderID = "chromedp-v0"

const (
	ErrInput        = fault.Kind("fathomry.chromedp.input")
	ErrState        = fault.Kind("fathomry.chromedp.state")
	ErrDisconnected = fault.Kind("fathomry.chromedp.disconnected")
	ErrLimit        = fault.Kind("fathomry.chromedp.limit")
	ErrNative       = fault.Kind("fathomry.chromedp.native")
	ErrUnsupported  = fault.Kind("fathomry.chromedp.unsupported")
	ErrCleanup      = fault.Kind("fathomry.chromedp.cleanup")
	ErrPanic        = fault.Kind("fathomry.chromedp.callback_panic")
)

func failure(kind fault.Kind, operation string, causes ...error) error {
	return kind.New(fault.Context{Provider: ProviderID, Operation: operation}, causes...)
}

type private struct{}

func (private) Format(state fmt.State, verb rune) {
	_, _ = io.WriteString(state, "chromedp[restricted]")
}
func (private) LogValue() slog.Value { return slog.StringValue("chromedp[restricted]") }
func (private) MarshalJSON() ([]byte, error) {
	return nil, errors.New("chromedp: runtime serialization is unsupported")
}
func (private) UnmarshalJSON([]byte) error {
	return errors.New("chromedp: runtime reconstruction is unsupported")
}
