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

package nacos

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func BenchmarkRead(b *testing.B) {
	for _, size := range []int{1024, MaxDocumentBytes - 1024} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			fixture := newFixture(b, false)
			content := `{"payload":"` + strings.Repeat("x", size-14) + `"}`
			fixture.mu.Lock()
			fixture.values[key{"DEFAULT_GROUP", "settings.yaml"}] = content
			fixture.mu.Unlock()
			client := openClient(b, fixture.options())
			for _, mode := range []string{"owned-sdk-session", "integration"} {
				b.Run(mode, func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(len(content)))
					for b.Loop() {
						var got string
						if mode == "integration" {
							value, err := client.Read(context.Background(), KeyV1{DataID: "settings.yaml"})
							if err != nil {
								b.Fatal(err)
							}
							got = string(value.RawCopy())
						} else {
							ctx, cancel := context.WithTimeout(context.Background(), client.settings.Timeout)
							current, err := client.newSession(ctx, 0, nil)
							if err != nil {
								cancel()
								b.Fatal(err)
							}
							value, err := current.query(ctx, key{"DEFAULT_GROUP", "settings.yaml"})
							current.close()
							cancel()
							if err != nil {
								b.Fatal(err)
							}
							got = value.Content
						}
						if got != content {
							b.Fatal("benchmarked useful result changed")
						}
					}
				})
			}
		})
	}
}
