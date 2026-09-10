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

package invocation_test

import (
	"errors"
	"fmt"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

func ExampleResult_Err() {
	// A result value alone cannot complete a live framework operation.
	cause := errors.New("private-native-detail")
	result := invocation.Result[int]{Outcome: invocation.Outcome[int]{
		Present: true, Value: 2, Primary: invocation.ErrFailed.New(fault.Context{}, cause),
	}}
	fmt.Println("partial:", result.Outcome.Value, "cause retained:", errors.Is(result.Err(), cause))
	fmt.Println("final:", result.Final, "released:", result.Released)
	// Output:
	// partial: 2 cause retained: true
	// final: false released: false
}
