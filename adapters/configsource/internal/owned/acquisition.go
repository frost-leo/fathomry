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

package owned

import (
	"fmt"
	"log/slog"
	"strings"

	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/failure/v1"
)

type acquisitionFailure struct {
	core *failure.Error
	info source.AcquisitionInfo
}

// Acquisition adds validated logical facts at the producing Adapter boundary.
// Invalid bootstrap text never enters these facts; callers first admit labels.
func Acquisition(condition failure.Condition, info source.AcquisitionInfo, causes ...error) error {
	if !Label(info.Source) || info.Document != "" && !Label(info.Document) {
		return Fail(source.ErrValue)
	}
	switch info.Phase {
	case source.SelectPhase, source.CapturePhase, source.ObservePhase, source.ClosePhase:
	default:
		return Fail(source.ErrValue)
	}
	core, err := failure.New(condition, causes...)
	if err != nil {
		return Fail(source.ErrValue)
	}
	return &acquisitionFailure{core: core, info: source.AcquisitionInfo{Source: strings.Clone(info.Source), Document: strings.Clone(info.Document), Phase: info.Phase}}
}
func (value *acquisitionFailure) Failure() *failure.Error {
	if value == nil {
		return nil
	}
	return value.core
}
func (value *acquisitionFailure) Acquisition() (source.AcquisitionInfo, bool) {
	if value == nil || value.core == nil {
		return source.AcquisitionInfo{}, false
	}
	return value.info, true
}
func (value *acquisitionFailure) Error() string { return value.Failure().Error() }
func (value *acquisitionFailure) Unwrap() error {
	if value == nil {
		return nil
	}
	return value.core
}
func (value *acquisitionFailure) Format(state fmt.State, verb rune) {
	core := value.Failure()
	if core == nil {
		Guard{}.Format(state, verb)
		return
	}
	core.Format(state, verb)
}
func (value *acquisitionFailure) LogValue() slog.Value   { return value.Failure().LogValue() }
func (*acquisitionFailure) MarshalJSON() ([]byte, error) { return nil, failure.ErrSerialization }
func (*acquisitionFailure) UnmarshalJSON([]byte) error   { return failure.ErrSerialization }
