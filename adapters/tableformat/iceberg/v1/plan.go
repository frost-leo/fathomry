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

package iceberg

import (
	"github.com/apache/arrow-go/v18/arrow"
	native "github.com/frost-leo/fathomry/internal/tableformat/iceberg/v0"
)

// Plan is an origin-bound task set. It cannot be rebound to another scan/source.
// Close uses already retained capacity; family termination closes ignored plans.
type Plan struct {
	private
	native *native.ManagedPlan
}

func (s *Scan) Plan() (*Plan, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	value, err := s.native.Plan()
	if value == nil {
		return nil, translate(err, "plan")
	}
	return &Plan{native: value}, translate(err, "plan")
}
func (p *Plan) Records(visit func(arrow.RecordBatch) (bool, error)) (ScanSummary, error) {
	if p == nil || p.native == nil {
		return ScanSummary{}, fail(ErrState, "plan")
	}
	value, err := p.native.Records(visit)
	return ScanSummary(value), translate(err, "planned-records")
}
func (p *Plan) Close() error {
	if p == nil || p.native == nil {
		return fail(ErrState, "plan")
	}
	return translate(p.native.Close(), "plan-close")
}
