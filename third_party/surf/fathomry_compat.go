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

package surf

import (
	"errors"

	"github.com/enetx/http"
)

// FathomryCompatibilityRevision identifies the local changes, not SDK provenance.
const FathomryCompatibilityRevision = "v2"

// ErrFathomryBodyLimit rejects a prefix that cannot represent a complete body.
var ErrFathomryBodyLimit = errors.New("surf: response body limit exceeded")

// ErrFathomryProfileLimit identifies an owned lazy output outside its declaration.
var ErrFathomryProfileLimit = errors.New("surf: profile output exceeds configured limit")

// FathomryFailure retains distinct native operation and cleanup occurrences.
// Unwrap preserves both original causes for intentional inspection.
type FathomryFailure struct {
	Primary error
	Cleanup error
}

func (*FathomryFailure) Error() string { return "surf: operation or cleanup failed" }
func (failure *FathomryFailure) Unwrap() []error {
	var causes []error
	if failure.Primary != nil {
		causes = append(causes, failure.Primary)
	}
	if failure.Cleanup != nil {
		causes = append(causes, failure.Cleanup)
	}
	return causes
}
func fathomryFailure(primary, cleanup error) error {
	if cleanup == nil {
		return primary
	}
	return &FathomryFailure{Primary: primary, Cleanup: cleanup}
}

func fathomryValidateH2(settings *HTTP2Settings) error {
	if settings == nil {
		return nil
	}
	maximum := settings.builder.cli.fathomry.control.MaxHeaderBytes
	if maximum > 0 && (int64(settings.maxHeaderListSize) > maximum || int64(settings.headerTableSize) > maximum || int64(settings.maxFrameSize) > maximum) {
		return ErrFathomryProfileLimit
	}
	control := settings.builder.cli.fathomry.control
	window := int64(4 << 20)
	if settings.headerTableSize != 0 || settings.usePush || settings.maxConcurrentStreams != 0 || settings.initialWindowSize != 0 || settings.maxFrameSize != 0 || settings.maxHeaderListSize != 0 || settings.noRFC7540Priorities != 0 {
		window = 65535
		if settings.initialWindowSize != 0 {
			window = int64(settings.initialWindowSize)
		}
	}
	if settings.initialWindowSize > (1<<31)-1 || settings.connectionFlow > (1<<31)-1-65535 ||
		control.MaxHTTP2StreamBytes > 0 && window > control.MaxHTTP2StreamBytes ||
		control.MaxProfileBytes > 0 && int64(len(settings.priorityFrames))*32+256 > control.MaxProfileBytes {
		return ErrFathomryProfileLimit
	}
	return nil
}

func closeFathomryBody(request *http.Request) error {
	if request == nil || request.Body == nil {
		return nil
	}
	return request.Body.Close()
}
