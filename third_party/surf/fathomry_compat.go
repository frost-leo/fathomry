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
const FathomryCompatibilityRevision = "v1"

// ErrFathomryBodyLimit rejects a prefix that cannot represent a complete body.
var ErrFathomryBodyLimit = errors.New("surf: response body limit exceeded")

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
		return errors.New("surf: native HTTP/2 receive settings exceed configured limit")
	}
	return nil
}

func closeFathomryBody(request *http.Request) error {
	if request == nil || request.Body == nil {
		return nil
	}
	return request.Body.Close()
}
