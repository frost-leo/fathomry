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

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	logging "github.com/frost-leo/fathomry/internal/logging/zerolog/v1"
	"github.com/frost-leo/fathomry/internal/resource"
)

func main() {
	directory, err := os.MkdirTemp("", "fathomry-zerolog-consumer-")
	if err != nil {
		panic("consumer directory failed")
	}
	defer os.RemoveAll(directory)
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"github.com/rs/zerolog"}})
	if err != nil {
		panic("consumer build inspection failed")
	}
	summary := struct {
		Go                string
		Executed, Refused bool
		Records           int
		Modules           map[string]string
	}{
		Go: build.Go.Value, Modules: make(map[string]string)}
	for _, module := range build.SDKs {
		summary.Modules[module.Path.Value] = module.Version.Value
	}
	defer func() {
		if json.NewEncoder(os.Stdout).Encode(summary) != nil {
			panic("consumer summary failed")
		}
	}()
	var secondary bytes.Buffer
	options := logging.OptionsV1{Name: "consumer", MaxRecordBytes: 1024, Sinks: []logging.SinkV1{
		{Name: "file", File: &logging.FileOptionsV1{Directory: directory, MaxBytes: 1024, Backups: 2, Compress: true}},
		{Name: "secondary", Writer: &secondary}}}
	selected, err := logging.Select(options)
	if err != nil {
		panic("consumer selection failed")
	}
	selected = resource.WithLimits(selected, logging.LimitsV1(options))
	owner, err := resource.Assemble(context.Background(), context.Background(), "consumer", selected)
	if err != nil {
		if owner != nil {
			_ = owner.Close(context.Background())
		}
		if errors.Is(err, logging.ErrUnsupported) {
			entries, readErr := os.ReadDir(directory)
			if readErr != nil || len(entries) != 0 {
				panic("encoding refusal acquired files")
			}
			summary.Refused = true
			return
		}
		panic("consumer assembly failed")
	}
	defer owner.Close(context.Background())
	inbox, err := invocation.NewInbox[logging.Result](1, logging.EvidenceBytesV1())
	if err != nil {
		panic("consumer inbox failed")
	}
	logger, err := logging.Bind(owner, selected, inbox, nil)
	if err != nil {
		panic("consumer binding failed")
	}
	for _, id := range []string{"one", "two"} {
		receipt, err := logger.Log(context.Background(), fault.Correlation{Call: id, Owner: "execution"}, logging.Info, strings.Repeat("x", 520), slog.String("kind", "example"))
		if err != nil {
			panic("consumer call setup failed")
		}
		result, ok := receipt.Result()
		if !ok || result.Err() != nil || !result.Final || !result.Released {
			panic("consumer delivery failed")
		}
		for _, sink := range result.Outcome.Value.SinksCopy() {
			if !sink.Accepted {
				panic("consumer sink missing")
			}
		}
		delivery, err := inbox.Next(context.Background())
		if err != nil || delivery.Release() != nil {
			panic("consumer evidence handoff failed")
		}
	}
	if err := owner.Close(context.Background()); err != nil {
		panic("consumer close failed")
	}
	count := func(data []byte) int {
		decoder := json.NewDecoder(bytes.NewReader(data))
		total := 0
		for {
			var record map[string]any
			if err := decoder.Decode(&record); err == io.EOF {
				return total
			} else if err != nil {
				panic("invalid consumer output")
			}
			if record["level"] != "info" {
				panic("consumer severity lost")
			}
			total++
		}
	}
	if count(secondary.Bytes()) != 2 {
		panic("consumer secondary output incomplete")
	}
	names, err := filepath.Glob(filepath.Join(directory, "*.gz"))
	if err != nil || len(names) != 1 {
		panic("consumer compressed archive missing")
	}
	compressed, err := os.ReadFile(names[0])
	if err != nil {
		panic("consumer archive read failed")
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		panic("consumer gzip failed")
	}
	archive, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err != nil || closeErr != nil {
		panic("consumer gzip read failed")
	}
	active, err := os.ReadFile(filepath.Join(directory, "current.jsonl"))
	if err != nil {
		panic("consumer active read failed")
	}
	summary.Records = count(archive) + count(active)
	if summary.Records != 2 {
		panic("consumer file output incomplete")
	}
	summary.Executed = true
}
