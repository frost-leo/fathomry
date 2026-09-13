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

package zerolog

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Source is an opaque assembly token, never a native logger or writer.
type Source struct {
	private
	owner *outputs
}
type output struct {
	settings sinkSettings
	borrowed borrowedSink
	file     *fileSink
	failed   error
}
type outputs struct {
	settings settings
	sinks    []output
	closed   int
}

// Select freezes configuration and validates all layers before any output is
// opened. Writer/Records handles remain explicitly borrowed, not copied. Handles
// reused across independent assemblies require caller-supplied synchronization.
// Overlays cannot invent, rename or change the kind of a runtime-bound sink.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	if len(options.Sinks) == 0 || len(options.Sinks) > MaxSinks {
		return resource.Selection[Source]{}, failure(ErrInput, "sink-count")
	}
	for _, sink := range options.Sinks {
		if sink.File != nil && len(sink.File.Directory) > 4096 {
			return resource.Selection[Source]{}, failure(ErrInput, "directory")
		}
	}
	if !withinBootstrapBudget(options) {
		location := fault.Context{Provider: ProviderID, Operation: "prepare"}
		if label(options.Name) {
			location.Source = options.Name
		}
		return resource.Selection[Source]{}, resource.ErrConfiguration.New(location, failure(ErrLimit, "bootstrap-size"))
	}
	handles := make(map[string]borrowedSink, len(options.Sinks))
	for _, sink := range options.Sinks {
		count := 0
		if sink.Writer != nil {
			count++
		}
		if sink.Records != nil {
			count++
		}
		if sink.File != nil {
			count++
		}
		if count != 1 || sink.Writer != nil && nilHandle(sink.Writer) || sink.Records != nil && nilHandle(sink.Records) {
			return resource.Selection[Source]{}, failure(ErrInput, "sink-handles")
		}
		if sink.Writer != nil || sink.Records != nil {
			if _, duplicate := handles[sink.Name]; duplicate {
				return resource.Selection[Source]{}, failure(ErrInput, "sink-name")
			}
			handles[sink.Name] = borrowedSink{sink.Writer, sink.Records}
		}
	}
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: func(value settings) error {
		if err := validate(value); err != nil {
			return err
		}
		for _, sink := range value.Sinks {
			handle, found := handles[sink.Name]
			if sink.Kind == "writer" && (!found || handle.writer == nil) ||
				sink.Kind == "record" && (!found || handle.records == nil) ||
				sink.Kind == "file" && found {
				return failure(ErrInput, "sink-binding")
			}
		}
		return nil
	}}, resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: 1, Layers: layers})
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return resource.Select(prepared, func(ctx context.Context, value settings) (resource.Resource[Source], error) {
		owner := &outputs{settings: value}
		owned := resource.Resource[Source]{Acquired: true, Capability: Source{owner: owner}, Release: owner.close}
		if err := nativeProbe(); err != nil {
			return owned, err
		}
		for _, sink := range value.Sinks {
			if err := ctx.Err(); err != nil {
				return owned, joined(ErrState, "construct", err, context.Cause(ctx))
			}
			item := output{settings: sink, borrowed: handles[sink.Name]}
			var err error
			if sink.File != nil {
				item.file, err = openFile(*sink.File)
			}
			owner.sinks = append(owner.sinks, item)
			if err != nil {
				return owned, err
			}
		}
		return owned, nil
	}), nil
}

// Logger is a non-owning concurrent facade. Every operation uses the original
// resource admission and independent evidence inbox, including borrowing aliases.
type Logger struct {
	private
	owner    *outputs
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
}

func Bind(assembly *resource.Assembly, selection resource.Selection[Source], inbox *invocation.Inbox[Result], observer *invocation.Observer) (*Logger, error) {
	source, _, err := resource.Bind(assembly, selection)
	if err != nil {
		return nil, err
	}
	access, err := resource.AccessFor(assembly, selection)
	if err != nil {
		return nil, err
	}
	if source.owner == nil || inbox == nil {
		return nil, failure(ErrInput, "bind")
	}
	value, limits := source.owner.settings, access.Limits()
	if limits.Active != 1 || limits.Bytes < value.reservation() || limits.Queued > value.QueuedCalls ||
		limits.Queued > 0 && limits.QueuedBytes < value.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Logger{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
}

// SinkResult reports one selected sink, without paths, record data or context
// values. Written is the byte writer's reported count, not remote durability.
// Attempted means actual output entry; Accepted requires full byte write or nil
// structured-sink acknowledgement. Failures may have effects despite either flag.
// Rotated/Synced report successful local operations, never power-loss guarantees.
type SinkResult struct {
	private
	Name             string
	Filtered         bool
	Attempted        bool
	Accepted         bool
	Written          int
	BytesKnown       bool
	Rotated          bool
	Synced           bool
	WriteError       error
	MaintenanceError error
}

// Result is immutable evidence shared by a receipt and independent inbox.
// Inspect SinksCopy and receipt errors, not merely the operation's setup error.
type Result struct {
	private
	sinks []SinkResult
}

func (result Result) SinksCopy() []SinkResult { return append([]SinkResult(nil), result.sinks...) }

func (logger *Logger) begin(ctx context.Context, id fault.Correlation, operation string) (*invocation.Call[Result], error) {
	if logger == nil || logger.owner == nil || ctx == nil {
		return nil, failure(ErrInput, operation)
	}
	value := logger.owner.settings
	// SDK attempts are unobserved. Sink entries and maintenance are separate
	// facts, not wrapper-derived SDK counts or bounds on opaque sink retries.
	return invocation.Begin(ctx, logger.access, invocation.Request{Name: operation, Correlation: id, Shape: invocation.Finite,
		Bytes: value.reservation(), EvidenceBytes: value.evidenceReservation(), Admission: invocation.Budget{Limit: value.Timeout}}, logger.inbox, logger.observer)
}
func (owner *outputs) result() Result {
	result := Result{sinks: make([]SinkResult, len(owner.sinks))}
	for index, item := range owner.sinks {
		result.sinks[index].Name = item.settings.Name
	}
	return result
}
func outcome(result Result, extra error) invocation.Outcome[Result] {
	causes := []error{extra}
	for _, sink := range result.sinks {
		causes = append(causes, sink.WriteError, sink.MaintenanceError)
	}
	return invocation.Outcome[Result]{Value: result, Present: true, Primary: joined(ErrWrite, "outputs", causes...)}
}

// Log validates closed, bounded slog attribute kinds without calling user
// marshalers/LogValuers. Attributes and message are borrowed until return, then
// independently frozen for structured sinks. Canonical slog groups stay nested;
// duplicate keys and noncanonical nested empty groups are rejected before
// admission. User attributes cannot overwrite fixed metadata.
// Setup errors have no receipt; accepted errors/filtering remain in the receipt
// and Inbox. Sinks run in order without retry; an error does not hide later sinks.
// A blocked io.Writer/filesystem call cannot be forcibly canceled or released.
func (logger *Logger) Log(ctx context.Context, id fault.Correlation, level Level, message string, attributes ...slog.Attr) (*invocation.Receipt[Result], error) {
	if logger == nil || logger.owner == nil {
		return nil, failure(ErrInput, "log")
	}
	if err := validateRecord(level, message, attributes, logger.owner.settings.MaxRecordBytes); err != nil {
		return nil, err
	}
	call, err := logger.begin(ctx, id, "log")
	if err != nil {
		return nil, err
	}
	receipt := call.Receipt()
	_ = call.Execute(ctx, invocation.Budget{Limit: logger.owner.settings.Timeout}, func(work context.Context, _ invocation.Scope) invocation.Outcome[Result] {
		result := logger.owner.result()
		enabled := false
		for index, item := range logger.owner.sinks {
			filtered := level.rank() < Level(logger.owner.settings.MinLevel).rank() || level.rank() < Level(item.settings.MinLevel).rank()
			result.sinks[index].Filtered = filtered
			enabled = enabled || !filtered
		}
		if !enabled {
			return outcome(result, nil)
		}
		data := &recordData{time: time.Now().UTC(), level: level, message: strings.Clone(message), source: logger.access.Info(),
			correlation: id, attributes: copyAttributes(attributes)}
		if err := encodeRecord(work, data, logger.owner.settings.MaxRecordBytes); err != nil {
			return outcome(result, err)
		}
		record := Record{data: data}
		for index := range logger.owner.sinks {
			if result.sinks[index].Filtered {
				continue
			}
			item, report := &logger.owner.sinks[index], &result.sinks[index]
			if work.Err() != nil {
				report.WriteError = joined(ErrWrite, "canceled", work.Err(), context.Cause(work))
				continue
			}
			if item.failed != nil {
				report.WriteError = failure(ErrState, "sink-stopped", item.failed)
				continue
			}
			if item.file != nil {
				item.file.write(work, record.JSONCopy(), report)
			} else {
				report.Attempted = true
				item.borrowed.write(work, record, report)
				item.failed = report.WriteError
			}
		}
		return outcome(result, nil)
	})
	return receipt, nil
}

func (sink borrowedSink) write(ctx context.Context, record Record, report *SinkResult) {
	returned := false
	defer func() {
		_ = recover()
		if !returned {
			report.WriteError = failure(ErrWrite, "sink-panic")
		}
	}()
	if sink.writer != nil {
		data := record.JSONCopy()
		count, err := sink.writer.Write(data)
		report.Written, report.BytesKnown = count, count >= 0 && count <= len(data)
		if count != len(data) {
			err = errors.Join(err, io.ErrShortWrite)
		}
		report.WriteError = joined(ErrWrite, "writer", err)
		report.Accepted = report.BytesKnown && count == len(data) && err == nil
	} else {
		err := sink.records.WriteRecord(ctx, record)
		report.WriteError = joined(ErrWrite, "record-writer", err)
		report.Accepted = err == nil
	}
	returned = true
}

// Sync synchronously syncs owned active files only. Borrowed outputs retain their
// own flush contract. Successful Sync is not a durable multi-sink transaction.
func (logger *Logger) Sync(ctx context.Context, id fault.Correlation) (*invocation.Receipt[Result], error) {
	return logger.maintain(ctx, id, false)
}

// Rotate archives each nonempty owned active file using its selected compression
// and retention policy. Empty files are unchanged. There is no rotation timer.
func (logger *Logger) Rotate(ctx context.Context, id fault.Correlation) (*invocation.Receipt[Result], error) {
	return logger.maintain(ctx, id, true)
}
func (logger *Logger) maintain(ctx context.Context, id fault.Correlation, rotate bool) (*invocation.Receipt[Result], error) {
	operation := "sync"
	if rotate {
		operation = "rotate"
	}
	if logger == nil || logger.owner == nil {
		return nil, failure(ErrInput, operation)
	}
	found := false
	for _, item := range logger.owner.sinks {
		found = found || item.file != nil
	}
	if !found {
		return nil, failure(ErrUnsupported, operation)
	}
	call, err := logger.begin(ctx, id, operation)
	if err != nil {
		return nil, err
	}
	_ = call.Execute(ctx, invocation.Budget{Limit: logger.owner.settings.Timeout}, func(work context.Context, _ invocation.Scope) invocation.Outcome[Result] {
		result := logger.owner.result()
		for index, item := range logger.owner.sinks {
			if item.file == nil {
				continue
			}
			report := &result.sinks[index]
			switch {
			case work.Err() != nil:
				report.MaintenanceError = joined(ErrMaintenance, "canceled", work.Err(), context.Cause(work))
			case item.file.failed != nil:
				report.MaintenanceError = failure(ErrState, "file-stopped", item.file.failed)
			default:
				if rotate {
					report.Rotated, report.MaintenanceError = item.file.rotate(work)
				} else {
					report.MaintenanceError = item.file.sync()
					report.Synced = report.MaintenanceError == nil
				}
			}
		}
		return outcome(result, nil)
	})
	return call.Receipt(), nil
}

func (owner *outputs) close(ctx context.Context) resource.ReleaseResult {
	var causes []error
	for owner.closed < len(owner.sinks) {
		if ctx.Err() != nil {
			return resource.ReleaseResult{Err: joined(ErrCleanup, "close", append(causes, ctx.Err(), context.Cause(ctx))...), Continue: owner.close}
		}
		item := &owner.sinks[len(owner.sinks)-1-owner.closed]
		if item.file != nil {
			causes = append(causes, item.file.close())
		}
		owner.closed++
	}
	return resource.ReleaseResult{Quiescent: true, Released: true, Err: joined(ErrCleanup, "close", causes...)}
}
