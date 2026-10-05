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

package objectstore_test

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestSharedDependencyBoundary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	output, err := exec.CommandContext(ctx, "go", "list", "-mod=readonly", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Fatal("dependency inspection failed")
	}
	for _, path := range strings.Fields(string(output)) {
		if strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/") || strings.HasPrefix(path, "github.com/frost-leo/fathomry/adapters/objectstore/minio/") ||
			strings.HasPrefix(path, "github.com/minio/") || strings.HasPrefix(path, "github.com/frost-leo/fathomry/framework/") {
			t.Fatal("shared contracts acquired a provider or host dependency")
		}
	}
}
