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

package viper

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
	sdk "github.com/spf13/viper"
)

type benchmarkSettings struct {
	Payload  string            `json:"payload"`
	Labels   map[string]string `json:"labels"`
	Optional *string           `json:"optional"`
	Large    uint64            `json:"large"`
}

func benchmarkRead(ctx context.Context, inputs []LoadInput, native bool) ([][]byte, error) {
	if err := validateInputs(inputs); err != nil {
		return nil, err
	}
	raws := make([][]byte, 0, len(inputs))
	var clients []*sdk.Viper
	if native {
		clients = make([]*sdk.Viper, 0, len(inputs))
	}
	remaining := MaxTotalBytes
	for _, input := range inputs {
		var raw []byte
		var err error
		limit := min(MaxDocumentBytes, remaining)
		if input.File != "" {
			raw, err = readFile(ctx, strings.Clone(input.File), limit)
		} else {
			raw, err = readBounded(ctx, input.Reader, limit)
		}
		if err != nil {
			return nil, err
		}
		raws = append(raws, raw)
		remaining -= len(raw)
		if !native {
			continue
		}
		if err := checkStructure(raw, input.Options.Encoding); err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, fail(ErrRead, "load", err)
		}
		client := sdk.New()
		client.SetConfigType(strings.Clone(input.Options.Encoding))
		client.AllowEmptyEnv(input.Options.AllowEmptyEnv)
		for _, entry := range input.Options.Defaults {
			value := entry.Value
			if text, ok := value.(string); ok {
				value = strings.Clone(text)
			}
			client.SetDefault(strings.Clone(entry.Key), value)
		}
		for _, binding := range input.Options.Environment {
			if err := client.BindEnv(strings.Clone(binding.Key), strings.Clone(binding.Name)); err != nil {
				return nil, fail(ErrInput, "bind", err)
			}
		}
		if err := client.ReadConfig(bytes.NewReader(raw)); err != nil {
			return nil, fail(ErrDecode, "decode", err)
		}
		if err := ctx.Err(); err != nil {
			return nil, fail(ErrRead, "load", err)
		}
		clients = append(clients, client)
	}
	for _, client := range clients {
		if copyValue(client.Get("labels")).(map[string]any)["x-case"] != "selected" ||
			client.Get("optional") != "fallback" {
			return nil, errors.New("direct native benchmark oracle changed")
		}
	}
	runtime.KeepAlive(raws)
	return raws, nil
}

func BenchmarkLoading(b *testing.B) {
	ctx := context.Background()
	for _, size := range []struct {
		name  string
		bytes int
	}{{"typical", 1024}, {"near-limit", MaxDocumentBytes - 1024}} {
		raws := make([]string, 4)
		for index := range raws {
			raws[index] = `{"payload":"` + strings.Repeat(string(rune('a'+index)), size.bytes-128) + `","labels":{"X-Case":"selected"},"optional":null,"large":9007199254740993}`
		}
		wantPayload := strings.Repeat("d", size.bytes-128)
		paths := make([]string, len(raws))
		directory := b.TempDir()
		total := 0
		for index, raw := range raws {
			paths[index] = filepath.Join(directory, fmt.Sprint(index))
			if err := os.WriteFile(paths[index], []byte(raw), 0600); err != nil {
				b.Fatal(err)
			}
			total += len(raw)
		}
		for _, medium := range []string{"reader", "file"} {
			inputs := func() []LoadInput {
				result := make([]LoadInput, len(raws))
				for index, raw := range raws {
					encoding := "json"
					if index%2 == 0 {
						encoding = "yaml"
					}
					result[index].Options = OptionsV1{Encoding: encoding, Defaults: []Default{{Key: "optional", Value: "fallback"}}}
					if medium == "reader" {
						result[index].Reader = strings.NewReader(raw)
					} else {
						result[index].File = paths[index]
					}
				}
				return result
			}
			for _, phase := range []string{"native", "prepare"} {
				for _, execution := range []string{"repeated", "concurrent"} {
					for _, path := range []string{"bounded-direct", "integration"} {
						b.Run(strings.Join([]string{phase, size.name, medium, execution, path}, "/"), func(b *testing.B) {
							operation := func() error {
								var content [][]byte
								if path == "bounded-direct" {
									var err error
									content, err = benchmarkRead(ctx, inputs(), phase == "native")
									if err != nil {
										return err
									}
								} else {
									documents, err := Load(ctx, inputs())
									if err != nil {
										return err
									}
									if phase == "native" {
										for _, document := range documents {
											labels, err := document.ValueCopy("labels")
											if err != nil || labels.(map[string]any)["x-case"] != "selected" {
												return errors.New("integration benchmark native oracle changed")
											}
											optional, err := document.ValueCopy("optional")
											if err != nil || optional != "fallback" {
												return errors.New("integration benchmark fallback changed")
											}
										}
									} else {
										content = make([][]byte, len(documents))
										for index, document := range documents {
											content[index] = document.RawCopy()
										}
									}
								}
								if phase == "native" {
									return nil
								}
								layers := make([]resource.Layer, len(content))
								for index, raw := range content {
									layers[index] = resource.Layer{Kind: resource.LayerKind(index + 1), Content: raw}
								}
								fallback := "fallback"
								checked := false
								prepared, err := resource.Prepare(resource.Schema[benchmarkSettings]{
									Format: 1, Defaults: benchmarkSettings{Optional: &fallback},
									Validate: func(value benchmarkSettings) error {
										checked = true
										if value.Payload != wantPayload || len(value.Labels) != 1 || value.Labels["X-Case"] != "selected" ||
											value.Optional != nil || value.Large != 9007199254740993 {
											return errors.New("benchmark preparation oracle changed")
										}
										return nil
									},
								}, resource.Input{Identity: resource.Identity{Provider: "benchmark.config", Name: "selected"}, Format: 1, Layers: layers})
								if err != nil {
									return err
								}
								if !checked || prepared.Description().Revision == "" {
									return errors.New("benchmark preparation did not execute")
								}
								return nil
							}
							// Untimed independent oracles reject incomparable/broken paths first.
							if err := operation(); err != nil {
								b.Fatal(err)
							}
							var samples [128]time.Duration
							var sampleCount atomic.Uint64
							measure := func() error {
								slot := sampleCount.Add(1) - 1
								if slot >= uint64(len(samples)) {
									return operation()
								}
								start := time.Now()
								err := operation()
								samples[slot] = time.Since(start)
								return err
							}
							b.ReportAllocs()
							b.SetBytes(int64(total))
							b.ResetTimer()
							if execution == "concurrent" {
								b.RunParallel(func(pb *testing.PB) {
									for pb.Next() {
										if err := measure(); err != nil {
											b.Error(err)
											return
										}
									}
								})
							} else {
								for b.Loop() {
									if err := measure(); err != nil {
										b.Fatal(err)
									}
								}
							}
							b.StopTimer()
							count := min(int(sampleCount.Load()), len(samples))
							slices.Sort(samples[:count])
							b.ReportMetric(float64(samples[(count-1)*50/100]), "load-p50-ns")
							b.ReportMetric(float64(samples[(count-1)*95/100]), "load-p95-ns")
							b.ReportMetric(float64(count), "samples")
							b.ReportMetric(4, "sources/op")
						})
					}
				}
			}
		}
	}
}

func BenchmarkRejectedDocument(b *testing.B) {
	count := (MaxDocumentBytes - len(`{"value":[]}`)) / 2
	raw := `{"value":[` + strings.Repeat("0,", count-1) + "0]}"
	for _, encoding := range []string{"yaml", "json"} {
		b.Run(encoding, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			for b.Loop() {
				documents, err := Load(context.Background(), []LoadInput{{
					Options: OptionsV1{Encoding: encoding}, Reader: strings.NewReader(raw),
				}})
				if documents != nil || !errors.Is(err, ErrLimit) {
					b.Fatal("dense input did not fail the pre-SDK structure bound")
				}
			}
		})
	}
}
