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
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	logging "github.com/frost-leo/fathomry/internal/logging/zap/v1"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type key struct{}
type sink struct {
	context bool
	writes  int
}

func (value *sink) Write(ctx context.Context, entry zapcore.Entry, fields []zapcore.Field) error {
	value.context = ctx.Value(key{}) == "original" && entry.LoggerName == "consumer" && len(fields) == 7 && fields[0].Integer == 42
	value.writes++
	return nil
}
func (*sink) Sync(context.Context) error { return nil }
func main() {
	directory, err := os.MkdirTemp("", "fathomry-zap-consumer-")
	if err != nil {
		panic("fixture directory failed")
	}
	defer os.RemoveAll(directory)
	options := logging.OptionsV1{Name: "consumer", Outputs: []logging.OutputV1{{Name: "local", Kind: "file", Directory: directory, Compress: true}}}
	extension := &sink{}
	selected, err := logging.Select(options, extension)
	if err != nil {
		panic("selection failed")
	}
	selected = resource.WithLimits(selected, logging.LimitsV1(options))
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), key{}, "original"), 5*time.Second)
	defer cancel()
	assembly, err := resource.Assemble(ctx, ctx, "consumer", selected)
	if err != nil {
		panic("assembly failed")
	}
	defer assembly.Close(ctx)
	inbox, err := invocation.NewInbox[logging.Result](1, 16<<10)
	if err != nil {
		panic("inbox failed")
	}
	logger, err := logging.Bind(assembly, selected, inbox, nil)
	if err != nil {
		panic("binding failed")
	}
	logger, err = logger.Named("consumer")
	if err != nil {
		panic("derivation failed")
	}
	receipt, err := logger.Log(ctx, fault.Correlation{Call: "one"}, zapcore.InfoLevel, "message", sdk.Int64("count", 42))
	if err != nil {
		panic("log setup failed")
	}
	result, err := receipt.WaitReleased(ctx)
	if err != nil || result.Err() != nil {
		panic("log failed")
	}
	delivery, err := inbox.Next(ctx)
	if err != nil || delivery.Release() != nil {
		panic("evidence reception failed")
	}
	if err := assembly.Close(ctx); err != nil {
		panic("cleanup failed")
	}
	data, err := os.ReadFile(filepath.Join(directory, "current.log"))
	if err != nil {
		panic("file read failed")
	}
	var record map[string]any
	if json.Unmarshal(data, &record) != nil {
		panic("file decoding failed")
	}
	report := struct {
		Go                         string
		Written, Context, Released bool
		Modules                    map[string]string
	}{
		Go: runtime.Version(), Written: record["count"] == float64(42) && extension.writes == 1,
		Context: extension.context, Released: result.Released && !assembly.Snapshot().Sources[0].Pending, Modules: make(map[string]string)}
	build, ok := debug.ReadBuildInfo()
	if !ok {
		panic("build info absent")
	}
	for _, dependency := range build.Deps {
		report.Modules[dependency.Path] = dependency.Version
	}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		panic("fixture output failed")
	}
}
