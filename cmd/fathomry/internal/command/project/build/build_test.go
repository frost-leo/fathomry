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

package build

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBuildAdmission(t *testing.T) {
	if !supportedHost {
		t.Skip("native build host not supported")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if result, err := Run(ctx, Options{Directory: "."}); result.Published || !errors.Is(err, context.Canceled) {
		t.Fatal(result, err)
	}
	for _, options := range []Options{{}, {Directory: ".", Timeout: -time.Second}, {Directory: ".", Timeout: 31 * time.Minute}, {Directory: t.TempDir()}} {
		if _, err := Run(context.Background(), options); !errors.Is(err, ErrOptions) {
			t.Fatal(options, err)
		}
	}
}
