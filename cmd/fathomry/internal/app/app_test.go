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

package app

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
)

func TestConcurrentInvocations(t *testing.T) {
	var wait sync.WaitGroup
	for range 16 {
		wait.Go(func() {
			var output, diagnostic bytes.Buffer
			err := Run(context.Background(), []string{"--help"}, command.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic})
			if err != nil || output.Len() == 0 || diagnostic.Len() != 0 {
				t.Error(err, output.String(), diagnostic.String())
			}
		})
	}
	wait.Wait()
}
