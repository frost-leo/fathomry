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
	"log/slog"
	"runtime"
	"strings"
	"sync"
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
	prepared         Prepared
	settings         settings
	sinks            []output
	closed           int
	derivationMu     sync.Mutex
	derivations      int
	derivationBytes  int64
	derivationClosed bool
}

// Select freezes configuration and validates all layers before any output is
// opened. Writer/Records handles remain explicitly borrowed, not copied. Handles
// reused across independent assemblies require caller-supplied synchronization.
// Overlays cannot invent, rename or change the kind of a runtime-bound sink.
func Select(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	return legacySelect(options, layers...)
}

// Logger is a non-owning concurrent facade. Every operation uses the original
// resource admission and independent evidence inbox, including borrowing aliases.
type Logger struct {
	private
	owner      *outputs
	access     *resource.Access
	inbox      *invocation.Inbox[Result]
	observer   *invocation.Observer
	policy     *preparedState
	attributes []slog.Attr
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
	return &Logger{owner: source.owner, access: access, inbox: inbox, observer: observer, policy: source.owner.prepared.state}, nil
}

// SinkResult reports one selected sink, without paths, record data or context
// values. Written is the byte writer's reported count, not remote durability.
// Attempted means actual output entry; Accepted requires full byte write or nil
// structured-sink acknowledgement. Failures may have effects despite either flag.
// Rotated/Synced report successful local operations, never power-loss guarantees.
// Stopped is output failure state, not record rejection; a managed record may
// fail without stopping later independent records.
type SinkResult struct {
	private
	Name             string
	Filtered         bool
	Attempted        bool
	Accepted         bool
	Stopped          bool
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
		result.sinks[index].Stopped = item.failed != nil || item.file != nil && item.file.failed != nil
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

// Entry is explicit business ingress metadata. Time zero means no timestamp;
// PC is a runtime.Callers return PC, not a stack-skip value. No caller callback
// or native SDK object is admitted. Input is borrowed until LogEntry returns.
type Entry struct {
	private
	Time    time.Time
	PC      uintptr
	Level   Level
	Message string
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
	var program [1]uintptr
	runtime.Callers(2, program[:])
	return logger.LogEntry(ctx, id, Entry{Time: time.Now(), PC: program[0], Level: level, Message: message}, attributes...)
}

// LogEntry preserves explicit time/PC across a trusted wrapper. It honors caller
// cancellation just like Log; a restricted slog entry may choose a distinct
// cancellation context before entering this method. Zero time is not replaced.
func (logger *Logger) LogEntry(ctx context.Context, id fault.Correlation, input Entry, attributes ...slog.Attr) (*invocation.Receipt[Result], error) {
	if logger == nil || logger.owner == nil {
		return nil, failure(ErrInput, "log")
	}
	if len(attributes) > MaxAttributes-len(logger.attributes) {
		return nil, failure(ErrLimit, "attributes")
	}
	combined := make([]slog.Attr, 0, len(logger.attributes)+len(attributes))
	combined = append(combined, logger.attributes...)
	combined = append(combined, attributes...)
	if err := validateRecord(input.Level, input.Message, combined, logger.owner.settings.MaxRecordBytes); err != nil {
		return nil, err
	}
	if !validTime(input.Time) {
		return nil, failure(ErrInput, "entry-time")
	}
	caller, err := resolveCaller(input.PC, logger.owner.settings.Caller)
	if err != nil {
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
		for index := range logger.owner.sinks {
			filtered := !logger.enabled(index, input.Level)
			result.sinks[index].Filtered = filtered
			enabled = enabled || !filtered
		}
		if !enabled {
			return outcome(result, nil)
		}
		data := &recordData{time: input.Time.UTC().Round(0), pc: input.PC, caller: caller, level: input.Level,
			message: strings.Clone(input.Message), source: logger.access.Info(), correlation: fault.Correlation{
				Call: strings.Clone(id.Call), Parent: strings.Clone(id.Parent), Owner: strings.Clone(id.Owner)}, attributes: copyAttributes(combined)}
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
				report.Stopped = true
				continue
			}
			if item.file != nil {
				item.file.write(work, record.JSONCopy(), report)
				report.Stopped = item.file.failed != nil
			} else {
				report.Attempted = true
				item.borrowed.write(work, record, report)
				if report.Stopped {
					item.failed = report.WriteError
				}
			}
		}
		return outcome(result, nil)
	})
	return receipt, nil
}

func resolveCaller(pc uintptr, enabled bool) (Caller, error) {
	if !enabled || pc == 0 {
		return Caller{}, nil
	}
	frame, _ := runtime.CallersFrames([]uintptr{pc}).Next()
	if frame.PC == 0 || frame.File == "" {
		return Caller{}, nil
	}
	if len(frame.File) > 4096 || len(frame.Function) > 4096 {
		return Caller{}, failure(ErrLimit, "entry-caller")
	}
	return Caller{Defined: true, PC: frame.PC, File: strings.Clone(frame.File), Line: frame.Line, Function: strings.Clone(frame.Function)}, nil
}

// With returns a non-owning view with independently frozen cumulative fields.
// Views share all physical admission, ordering and failure state. A source has
// one cumulative bounded view allowance; policy updates never reset it.
func (logger *Logger) With(attributes ...slog.Attr) (*Logger, error) {
	if logger == nil || logger.owner == nil {
		return nil, failure(ErrInput, "with")
	}
	if len(attributes) > MaxAttributes-len(logger.attributes) {
		return nil, failure(ErrLimit, "attributes")
	}
	combined := make([]slog.Attr, 0, len(logger.attributes)+len(attributes))
	combined = append(combined, logger.attributes...)
	combined = append(combined, attributes...)
	bytes, err := validateAttributes(combined, logger.owner.settings.MaxRecordBytes)
	if err != nil {
		return nil, err
	}
	frozen, err := logger.owner.freezeDerivation(combined, int64(bytes+retainedAttributeHeaders(combined))+256)
	if err != nil {
		return nil, err
	}
	view := *logger
	view.attributes = frozen
	return &view, nil
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
			report.Stopped = item.file.failed != nil || item.file.closed
		}
		return outcome(result, nil)
	})
	return call.Receipt(), nil
}

func (owner *outputs) close(ctx context.Context) resource.ReleaseResult {
	owner.derivationMu.Lock()
	owner.derivationClosed = true
	owner.derivationMu.Unlock()
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
