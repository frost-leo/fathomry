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
	"errors"
	"fmt"

	"github.com/frost-leo/fathomry/failure"
)

func ExampleNew() {
	const unsupported failure.Code = "example.query.unsupported"
	err := failure.New(unsupported, nil)
	fmt.Println(err)
	fmt.Println(errors.Is(fmt.Errorf("query: %w", err), unsupported))
	// Output:
	// example.query.unsupported
	// true
}

func ExampleInspect() {
	primary := failure.New("example.operation.failed", nil)
	cleanup := failure.New("example.cleanup.failed", nil)
	_, ok := failure.Inspect(errors.Join(cleanup, primary))
	fmt.Println(ok)
	described, ok := failure.Inspect(primary)
	fmt.Println(ok, described.Code())
	// Output:
	// false
	// true example.operation.failed
}

func ExampleNew_diagnostics() {
	original := failure.New("example.query.unsupported", nil)
	enriched := failure.New(original.Code(), nil, failure.Attribute{Name: "operator", Value: "contains"})
	fmt.Println(enriched)
	fmt.Println(enriched.Diagnostic().Attributes)
	refused := failure.New(original.Code(), nil, failure.Attribute{Name: "invalid name"})
	fmt.Println(refused.Code(), refused.Diagnostic().Omitted)
	fmt.Println(len(original.Diagnostic().Attributes))
	// Output:
	// example.query.unsupported
	// [{operator contains}]
	// example.query.unsupported true
	// 0
}
