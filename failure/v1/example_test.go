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

package failure_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/frost-leo/fathomry/failure/v1"
)

func ExampleNew() {
	const unavailable failure.Condition = "example.inventory.unavailable"
	err, rejected := failure.New(unavailable, context.DeadlineExceeded)
	if rejected != nil {
		panic(rejected)
	}
	fmt.Println(err)
	fmt.Println(errors.Is(err, unavailable), errors.Is(err, context.DeadlineExceeded))
	// Output:
	// example.inventory.unavailable
	// true true
}

func ExampleInspect() {
	primary, _ := failure.New("example.source.absent")
	cleanup, _ := failure.New("example.source.cleanup_failed")
	combined := errors.Join(primary, cleanup)
	_, direct := failure.Inspect(combined)
	fmt.Println("direct:", direct)
	fmt.Println("absence is a member:", errors.Is(combined, failure.Condition("example.source.absent")))
	selected, _ := failure.Inspect(cleanup)
	fmt.Println("explicitly selected cleanup:", selected.Diagnostic().Condition)
	// Output:
	// direct: false
	// absence is a member: true
	// explicitly selected cleanup: example.source.cleanup_failed
}
