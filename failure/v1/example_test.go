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

	"github.com/frost-leo/fathomry/failure/v1"
)

func ExampleNewDetailed() {
	const denied failure.Code = failure.ErrorPrefix | failure.Code(0x442)<<16 | 0x0001
	type AccessDetails struct {
		Document string
		Phase    string
	}
	definition := failure.Definition{
		Code: denied, Identifier: "example.configsource.denied", Module: "example", Component: "configsource",
		Revision: 1, Message: "Remote configuration access was denied.",
		Details: failure.Contract{ID: "example.configsource.access_details", Version: 1},
	}
	native := errors.New("private server detail")
	occurrence, err := failure.NewDetailed(definition,
		failure.Location{Operation: "read", Instance: "primary"},
		AccessDetails{Document: "business", Phase: "capture"}, func(value AccessDetails) AccessDetails { return value }, native)
	if err != nil {
		panic(err)
	}
	core, ok := failure.Inspect(occurrence)
	if !ok {
		panic("missing occurrence")
	}
	details, _ := occurrence.Details()
	fmt.Println(core.Diagnostic().Definition.Code)
	fmt.Println(core.Diagnostic().Definition.Identifier)
	fmt.Println(details.Document)
	fmt.Println(errors.Is(occurrence, denied), errors.Is(occurrence, native))
	// Output:
	// 0xA4420001
	// example.configsource.denied
	// business
	// true true
}

func ExamplePrepare() {
	catalog, err := failure.Prepare(failure.Definitions()...)
	if err != nil {
		panic(err)
	}
	code, err := failure.ParseCode("0xA0010001")
	if err != nil {
		panic(err)
	}
	definition, found, err := catalog.Lookup(code)
	if err != nil || !found {
		panic("definition not found")
	}
	fmt.Println(definition.Identifier)
	fmt.Println(definition.Message)
	// Output:
	// fathomry.failure.invalid_code
	// The public error code is invalid.
}
