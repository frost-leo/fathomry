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

package table

import (
	"context"
	"errors"
	"reflect"
	"testing"

	icebergio "github.com/apache/iceberg-go/io"
)

type partialOrphanIO struct {
	icebergio.IO
	failure error
}

func (f partialOrphanIO) DeleteFiles(_ context.Context, paths []string) ([]string, error) {
	return []string{paths[1]}, f.failure
}

func TestOrphanCleanupPreservesAcknowledgedDeletionsOnFailure(t *testing.T) {
	failure := errors.New("first deletion response unknown")
	files := partialOrphanIO{failure: failure}
	tbl := Table{fsF: func(context.Context) (icebergio.IO, error) { return files, nil }}
	plan := OrphanCleanupPlan{
		orphanFileLocations: []string{"s3://owned/unknown", "s3://owned/confirmed"},
		orphanFiles:         []OrphanFile{{Path: "s3://owned/unknown", SizeBytes: 7}, {Path: "s3://owned/confirmed", SizeBytes: 11}},
		totalSizeBytes:      18,
	}
	result, err := tbl.ExecuteOrphanCleanup(t.Context(), plan)
	if !errors.Is(err, failure) || !reflect.DeepEqual(result.DeletedFiles, []string{"s3://owned/confirmed"}) {
		t.Fatalf("partial deletion facts lost: %#v, %v", result, err)
	}
	if !reflect.DeepEqual(result.OrphanFiles, plan.OrphanFiles()) || result.TotalSizeBytes != 18 || !reflect.DeepEqual(result.OrphanFileLocations, plan.Files()) {
		t.Fatal("failure discarded the inspected deletion plan")
	}
}
