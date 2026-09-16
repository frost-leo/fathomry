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

package chromedp_test

import (
	"context"
	"fmt"

	chromedp "github.com/frost-leo/fathomry/internal/browser/chromedp/v0"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Preparing/assembling without CheckReady does not execute the configured path.
func ExampleSelect() {
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	options := chromedp.OptionsV1{Name: "render", ExecPath: "/explicit/path/to/chrome", NewWindow: true}
	selected, err := chromedp.Select(lifetime, options)
	if err != nil {
		panic(err)
	}
	limits, err := chromedp.LimitsV1(options)
	if err != nil {
		panic(err)
	}
	selected = resource.WithLimits(selected, limits)
	assembly, err := resource.Assemble(context.Background(), context.Background(), "application", selected)
	if err != nil {
		panic(err)
	}
	defer assembly.Close(context.Background())
	inbox, err := invocation.NewInbox[chromedp.Result](1, 8<<20)
	if err != nil {
		panic(err)
	}
	client, err := chromedp.Bind(assembly, selected, inbox, nil)
	if err != nil {
		panic(err)
	}
	_, launched := client.LaunchArgumentsCopy()
	fmt.Println(chromedp.ProviderID, launched)
	// Output: chromedp-v0 false
}
