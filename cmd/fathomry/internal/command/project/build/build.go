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

package build

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"time"

	workflows "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow"
	"github.com/frost-leo/fathomry/failure/v1"

	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

// DefaultTimeout bounds compilation, inspection and publication together.
const DefaultTimeout = 10 * time.Minute

// Options selects a trusted root Go module and a native build. Output is a file,
// relative to the project directory; empty means bin/<declared-name>. An existing
// regular output is replaced only after admission. Symlink outputs reject.
// Timeout zero means DefaultTimeout; the admitted range is 1ms through 30 minutes.
type Options struct {
	Directory string
	Output    string
	Timeout   time.Duration
	// DeclarationOnly explicitly selects legacy v1; a failed v2 never falls back.
	DeclarationOnly bool
}

// Result reports publication, not deployment or Temporal admission. On an error,
// Published may still be true if publication preceded a cleanup failure.
type Result struct {
	Name            string                           `json:"name"`
	Artifact        string                           `json:"artifact"`
	SHA256          string                           `json:"sha256"`
	Target          string                           `json:"target"`
	Published       bool                             `json:"published"`
	Workflows       int                              `json:"workflows"`
	Modes           int                              `json:"modes"`
	Entries         []wf.Entry                       `json:"entries"`
	Inspection      string                           `json:"inspection"`
	Implementations []workflows.ImplementationReport `json:"implementations"`
}

// Run freshly compiles the root package, executes its bounded offline inspection,
// checks the supplied declarations, then publishes those identical executable bytes.
// It borrows the caller's context, captures process environment, disables workspaces
// and GOFLAGS, and uses readonly modules. No arbitrary Go build flags are accepted.
// Native Linux/macOS builds are supported. The project must not change during Go's
// compilation; neither this operation nor Go provides a filesystem source snapshot.
func Run(ctx context.Context, options Options) (result Result, err error) {
	if err := canceled(ctx); err != nil {
		return result, err
	}
	if !supportedHost {
		return result, fail(ErrPlatform)
	}
	if options.Timeout == 0 {
		options.Timeout = DefaultTimeout
	}
	if options.Timeout < time.Millisecond || options.Timeout > 30*time.Minute || options.Directory == "" {
		return result, fail(ErrOptions)
	}
	ctx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()
	directory, err := filepath.Abs(options.Directory)
	if err != nil {
		return result, fail(ErrOptions, err)
	}
	directory, err = filepath.EvalSymlinks(directory)
	if err != nil {
		return result, fail(ErrOptions, err)
	}
	info, err := os.Lstat(filepath.Join(directory, "go.mod"))
	if err != nil || !info.Mode().IsRegular() {
		return result, fail(ErrOptions, err)
	}
	environment := buildEnvironment()
	raw, err := runProcess(ctx, directory, environment, maxToolOutput, "go", "env", "-json", "GOOS", "GOARCH")
	if err != nil {
		return result, phaseError(ctx, ErrCompile, err)
	}
	var target struct {
		GOOS   string
		GOARCH string
	}
	if err := json.Unmarshal(raw, &target); err != nil {
		return result, fail(ErrCompile, err)
	}
	if target.GOOS != runtime.GOOS || target.GOARCH != runtime.GOARCH {
		return result, fail(ErrPlatform)
	}
	temporary, err := os.MkdirTemp("", "fathomry-build-")
	if err != nil {
		return result, fail(ErrCompile, err)
	}
	defer func() {
		if cleanup := os.RemoveAll(temporary); cleanup != nil {
			err = fail(ErrCleanup, err, cleanup)
		}
	}()
	binary := filepath.Join(temporary, "project")
	_, err = runProcess(ctx, directory, environment, maxToolOutput, "go", "build", "-mod=readonly", "-buildvcs=false", "-trimpath", "-o", binary, ".")
	if err != nil {
		return result, phaseError(ctx, ErrCompile, err)
	}
	before, err := executableHash(ctx, binary)
	if err != nil {
		return result, phaseError(ctx, ErrInspection, err)
	}
	inspectContext, stop := context.WithTimeout(ctx, 15*time.Second)
	argument, maximum := ImplementationInspectionArgument, maxImplementationInspectionBytes
	if options.DeclarationOnly {
		argument, maximum = InspectionArgument, maxInspectionBytes
	}
	raw, err = runProcess(inspectContext, directory, environment, maximum, binary, argument)
	stop()
	if err != nil {
		return result, phaseError(ctx, ErrInspection, err)
	}
	var document inspection
	var report wf.Report
	var implementations []workflows.ImplementationReport
	if options.DeclarationOnly {
		document, report, err = inspect(ctx, raw)
	} else {
		document, report, implementations, err = inspectImplementations(ctx, raw)
	}
	if err != nil {
		return result, err
	}
	after, err := executableHash(ctx, binary)
	if err != nil {
		return result, phaseError(ctx, ErrInspection, err)
	}
	if before != after {
		return result, fail(ErrInspection)
	}
	output := options.Output
	if output == "" {
		output = filepath.Join("bin", document.Name)
	}
	if !filepath.IsAbs(output) {
		output = filepath.Join(directory, output)
	}
	output = filepath.Clean(output)
	result = Result{Name: document.Name, Artifact: output, SHA256: after,
		Target: target.GOOS + "/" + target.GOARCH, Workflows: report.Workflows,
		Modes: report.Modes, Entries: report.Entries, Inspection: document.Schema, Implementations: implementations}
	result.Published, err = publish(ctx, binary, output, after)
	return result, err
}

func phaseError(ctx context.Context, code failure.Code, err error) error {
	if ctx.Err() != nil {
		return fail(ErrCanceled, ctx.Err(), context.Cause(ctx), err)
	}
	if errors.Is(err, ErrCanceled) {
		return fail(ErrCanceled, err)
	}
	if errors.Is(err, ErrLimit) {
		return fail(ErrLimit, err)
	}
	return fail(code, err)
}
