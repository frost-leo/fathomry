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

package zap

import (
	"context"
	"io"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
	sdk "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// StructuredSink is a trusted composition-only, non-owning extension boundary,
// not an arbitrary native Core/Logger supplied by an operation caller. Write
// receives an entry and isolated supported fields, never preformatted JSON.
// Implementations may retain those copies; *fault.Error graphs remain borrowed.
// Context is borrowed for the call and must propagate through nested logging/I/O.
// No context values are serialized by this integration.
//
// Composition owns the sink's resources, bounds, errors and termination; arrange
// dependencies before this source and release them after it. Methods must return
// normally, must not reenter with a replacement context, and must not require
// progress from this source. An uncooperative method blocks its caller and lease.
// Nil Write/Sync means only the sink's own documented local acceptance, never
// remote export/durability. Shared implementations must be concurrency-safe.
// Errors must not expose owning handles or new callback authority.
// This package does not call an extension's Close.
type StructuredSink interface {
	Write(context.Context, zapcore.Entry, []zapcore.Field) error
	Sync(context.Context) error
}

// Source is an opaque resource capability. Bind alone supplies controlled logging.
type Source struct {
	private
	owner *owner
}

// Logger is a concurrent, non-owning facade. Admission, evidence and sink writes
// share the same authoritative source across derived loggers and borrowing aliases.
type Logger struct {
	private
	owner    *owner
	access   *resource.Access
	inbox    *invocation.Inbox[Result]
	observer *invocation.Observer
	name     string
	fields   []zapcore.Field
	policy   *preparedState
}

type owner struct {
	prepared        Prepared
	settings        settings
	extension       StructuredSink
	branches        []*branch
	closed          bool
	closeErr        error
	derivationMu    sync.Mutex
	derivations     int
	derivationBytes int64
}
type branch struct {
	zapcore.Core
	name      string
	writer    *checkedWriter
	file      *rotatingFile
	extension StructuredSink
	minimum   zapcore.Level
	ctx       context.Context
	result    SinkResult
}

// Select prepares the entire configuration before any file is opened. extension
// may be nil; no implicit output, global logger, registry or exporter is selected.
// The explicit extension is borrowed for each assembled source's lifetime.
func Select(options OptionsV1, extension StructuredSink, layers ...resource.Layer) (resource.Selection[Source], error) {
	prepared, err := PrepareV1(options, extension != nil, layers...)
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return prepared.Select(extension)
}

func encoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{TimeKey: "ts", LevelKey: "level", NameKey: "logger", MessageKey: "msg", CallerKey: "caller",
		LineEnding: "\n", EncodeLevel: zapcore.LowercaseLevelEncoder, EncodeTime: zapcore.RFC3339NanoTimeEncoder,
		EncodeDuration: zapcore.NanosDurationEncoder, EncodeCaller: zapcore.ShortCallerEncoder}
}

// Bind attaches the independent evidence inbox. The source must have a
// single-active-call policy: aliases cannot create parallel writes or more quota.
func Bind(assembly *resource.Assembly, selected resource.Selection[Source], inbox *invocation.Inbox[Result], observer *invocation.Observer) (*Logger, error) {
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		return nil, err
	}
	access, err := resource.AccessFor(assembly, selected)
	if err != nil {
		return nil, err
	}
	if source.owner == nil || inbox == nil {
		return nil, failure(ErrInput, "bind")
	}
	value, limits := source.owner.settings, access.Limits()
	if limits.Active != 1 || limits.Queued > value.QueuedCalls || limits.Bytes < value.reservation() ||
		limits.Queued > 0 && limits.QueuedBytes < value.reservation() {
		return nil, failure(ErrInput, "limits")
	}
	return &Logger{owner: source.owner, access: access, inbox: inbox, observer: observer, policy: source.owner.prepared.state}, nil
}

// SinkState describes a single native branch observation. Failure may follow
// partial output; neither Written nor Synced certifies storage or remote delivery.
type SinkState uint8

const (
	NotAttempted SinkState = iota
	Filtered
	Written
	Synced
	Failed
)

// SinkResult is a copied branch observation. Bytes is the native writer return,
// not confirmed durability. It is zero/unknown for StructuredSink. Err preserves
// native cause identity, while ordinary formatting omits native text and graphs.
type SinkResult struct {
	private
	Name  string
	State SinkState
	Bytes int
	Err   error
}

// Result holds immutable branch evidence, not messages, context values or fields.
// A later successful Sync cannot change a previous log receipt or its inbox copy.
type Result struct {
	private
	sinks []SinkResult
}

// SinksCopy returns independent observation storage; native error causes remain
// borrowed for deliberate inspection. A zero result has no observations.
func (result Result) SinksCopy() []SinkResult { return append([]SinkResult(nil), result.sinks...) }

type emissionKey struct{}

// Entry preserves the caller's event metadata. Time zero deliberately omits the
// native timestamp, and PC zero means no supplied caller. PC is a return program
// counter from runtime.Callers, resolved only with runtime.CallersFrames. Name
// optionally extends the Logger's frozen instrumentation name without deriving
// another retained view. Entry is borrowed until LogEntry returns.
type Entry struct {
	private
	Time    time.Time
	PC      uintptr
	Level   zapcore.Level
	Message string
	Name    string
}

type entryKey struct{}

// EntryMetadata exposes this integration's captured ingress metadata to its
// trusted synchronous structured destination. PC remains the original return PC,
// while zapcore.Entry.Caller follows native adjusted-PC semantics. It does not
// inspect arbitrary caller context values or grant native ownership.
func EntryMetadata(ctx context.Context) (Entry, bool) {
	if ctx == nil {
		return Entry{}, false
	}
	value, ok := ctx.Value(entryKey{}).(Entry)
	return value, ok
}

func (logger *Logger) begin(ctx context.Context, id fault.Correlation, name string) (*invocation.Call[Result], error) {
	if logger == nil || logger.owner == nil || ctx == nil {
		return nil, failure(ErrInput, name)
	}
	if active, _ := ctx.Value(emissionKey{}).(bool); active {
		return nil, failure(ErrRecursion, name)
	}
	if id.Call == "" || !id.Valid() {
		return nil, failure(ErrInput, name)
	}
	value := logger.owner.settings
	id = fault.Correlation{Call: strings.Clone(id.Call), Parent: strings.Clone(id.Parent), Owner: strings.Clone(id.Owner)}
	return invocation.Begin(ctx, logger.access, invocation.Request{Name: name, Correlation: id, Shape: invocation.Finite,
		Bytes: value.reservation(), EvidenceBytes: value.evidenceReservation(), Admission: invocation.Budget{Limit: value.Timeout}},
		logger.inbox, logger.observer)
}

// Log accepts debug through error only; panic/fatal/development actions are
// rejected before admission. Native primitive/binary/time fields and non-nil
// *fault.Error values are supported. Reflection, object/array/stringer callbacks,
// arbitrary errors, duplicate/reserved keys and lazy fields are rejected.
// Native no-op fields, including zap.Error(nil), are ignored.
//
// Setup failures return no receipt. Accepted operations return their errors and
// per-sink effects through both Receipt and Inbox, not the immediate Go error.
// Outputs are synchronous, ordered and non-atomic. Cancellation is checked before
// each sink; it does not interrupt an entered syscall or erase earlier effects.
func (logger *Logger) Log(ctx context.Context, id fault.Correlation, severity zapcore.Level, message string, fields ...zapcore.Field) (*invocation.Receipt[Result], error) {
	input := Entry{Time: time.Now().UTC(), Level: severity, Message: message}
	if logger != nil && logger.owner != nil && logger.owner.settings.Caller {
		var program [1]uintptr
		runtime.Callers(2, program[:])
		input.PC = program[0]
	}
	return logger.LogEntry(ctx, id, input, fields...)
}

// LogEntry accepts explicit ingress metadata without recapturing a wrapper's
// caller or replacing a zero event time. Direct context cancellation remains
// authoritative; a restricted slog gateway must supply its own bounded owner
// lifetime while preserving caller association separately.
func (logger *Logger) LogEntry(ctx context.Context, id fault.Correlation, input Entry, fields ...zapcore.Field) (*invocation.Receipt[Result], error) {
	if logger == nil || logger.owner == nil || input.Level < zapcore.DebugLevel || input.Level > zapcore.ErrorLevel ||
		len(input.Message) > MaxMessageBytes || !utf8.ValidString(input.Message) || len(logger.fields)+len(fields) > MaxFields ||
		input.Name != "" && !fieldKey(input.Name) {
		return nil, failure(ErrInput, "log")
	}
	name := logger.name
	if input.Name != "" {
		if name != "" {
			name += "."
		}
		name += input.Name
	}
	if len(name) > 128 {
		return nil, failure(ErrLimit, "name")
	}
	combined := make([]zapcore.Field, 0, len(logger.fields)+len(fields))
	combined = append(combined, logger.fields...)
	combined = append(combined, fields...)
	if err := validateFields(combined); err != nil {
		return nil, err
	}
	call, err := logger.begin(ctx, id, "log")
	if err != nil {
		return nil, err
	}
	frozen, err := freezeFields(combined)
	if err != nil {
		call.Complete(invocation.Outcome[Result]{Primary: err})
		return call.Receipt(), nil
	}
	info := logger.access.Info()
	frozen = append(frozen, sdk.String("fathomry.call", strings.Clone(id.Call)), sdk.String("fathomry.parent", strings.Clone(id.Parent)),
		sdk.String("fathomry.owner", strings.Clone(id.Owner)), sdk.String("fathomry.source", info.Configuration.Identity.Name),
		sdk.String("fathomry.provider", ProviderID), sdk.String("fathomry.scope", info.Scope))
	input.Message, input.Name = strings.Clone(input.Message), strings.Clone(name)
	entry := zapcore.Entry{Time: input.Time.UTC().Round(0), Level: input.Level, Message: input.Message, LoggerName: input.Name}
	if logger.owner.settings.Caller && input.PC != 0 {
		frames := runtime.CallersFrames([]uintptr{input.PC})
		frame, _ := frames.Next()
		if frame.PC != 0 {
			if len(frame.File) > 4096 || len(frame.Function) > 4096 {
				call.Complete(invocation.Outcome[Result]{Primary: failure(ErrLimit, "caller")})
				return call.Receipt(), nil
			}
			entry.Caller = zapcore.EntryCaller{Defined: true, PC: frame.PC, File: frame.File, Line: frame.Line, Function: frame.Function}
		}
	}
	_ = call.Execute(ctx, invocation.Budget{Limit: logger.owner.settings.Timeout}, func(work context.Context, _ invocation.Scope) invocation.Outcome[Result] {
		work = context.WithValue(work, emissionKey{}, true)
		work = context.WithValue(work, entryKey{}, input)
		results := make([]SinkResult, 0, len(logger.owner.branches))
		var causes []error
		for index, branch := range logger.owner.branches {
			branch.ctx = work
			branch.result = SinkResult{Name: branch.name, State: Filtered}
			if branch.writer != nil {
				branch.writer.bytes = 0
			}
			var checked *zapcore.CheckedEntry
			if logger.minimum(index).Enabled(entry.Level) {
				checked = checked.AddCore(entry, branch)
			}
			if checked != nil {
				// Leave ErrorOutput nil: native diagnostics would format write
				// causes and lose per-call attribution. The branch retains them.
				_, _ = call.Attempt()
				checked.Write(frozen...)
			}
			results = append(results, branch.result)
			causes = append(causes, branch.result.Err)
			branch.ctx = nil
			branch.result = SinkResult{}
		}
		return invocation.Outcome[Result]{Present: true, Value: Result{sinks: results}, Primary: failureOrNil(ErrWrite, "log", causes...)}
	})
	return call.Receipt(), nil
}

// Sync executes each destination's native sync under the same allowance and
// evidence protocol. Stream Sync errors are preserved, not silently ignored.
// No export-provider shutdown or retry is implied by a successful result.
func (logger *Logger) Sync(ctx context.Context, id fault.Correlation) (*invocation.Receipt[Result], error) {
	call, err := logger.begin(ctx, id, "sync")
	if err != nil {
		return nil, err
	}
	_ = call.Execute(ctx, invocation.Budget{Limit: logger.owner.settings.Timeout}, func(work context.Context, _ invocation.Scope) invocation.Outcome[Result] {
		work = context.WithValue(work, emissionKey{}, true)
		results := make([]SinkResult, 0, len(logger.owner.branches))
		var causes []error
		for _, branch := range logger.owner.branches {
			result := SinkResult{Name: branch.name, State: NotAttempted}
			if work.Err() != nil {
				result.Err = failure(ErrSync, "canceled", work.Err(), context.Cause(work))
			} else {
				_, _ = call.Attempt()
				if branch.extension != nil {
					result.Err = branch.extension.Sync(work)
				} else {
					result.Err = branch.Core.Sync()
				}
				result.Err = nativeFailure(ErrSync, "sink", work, result.Err)
				result.State = Synced
				if result.Err != nil {
					result.State = Failed
				}
			}
			results = append(results, result)
			causes = append(causes, result.Err)
		}
		return invocation.Outcome[Result]{Present: true, Value: Result{sinks: results}, Primary: failureOrNil(ErrSync, "sync", causes...)}
	})
	return call.Receipt(), nil
}

func (branch *branch) Enabled(value zapcore.Level) bool { return branch.minimum.Enabled(value) }
func (branch *branch) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if branch.Enabled(entry.Level) {
		return checked.AddCore(entry, branch)
	}
	return checked
}
func (branch *branch) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	if branch.ctx.Err() != nil {
		branch.result.State = NotAttempted
		branch.result.Err = failure(ErrWrite, "canceled", branch.ctx.Err(), context.Cause(branch.ctx))
		return branch.result.Err
	}
	var err error
	if branch.extension != nil {
		isolated := copyFields(fields)
		err = branch.extension.Write(branch.ctx, entry, isolated)
	} else {
		err = branch.Core.Write(entry, fields)
		branch.result.Bytes = branch.writer.bytes
	}
	branch.result.State = Written
	branch.result.Err = nativeFailure(ErrWrite, "sink", branch.ctx, err)
	if err != nil {
		branch.result.State = Failed
	}
	return branch.result.Err
}

type checkedWriter struct {
	native  zapcore.WriteSyncer
	maximum int
	bytes   int
}

func (writer *checkedWriter) Write(data []byte) (int, error) {
	if len(data) > writer.maximum {
		return 0, failure(ErrLimit, "encoded-entry")
	}
	count, err := writer.native.Write(data)
	writer.bytes = count
	if count < 0 || count > len(data) {
		return count, failure(ErrWrite, "writer-count", err)
	}
	if count != len(data) && err == nil {
		err = io.ErrShortWrite
	}
	return count, err
}
func (writer *checkedWriter) Sync() error { return writer.native.Sync() }

func (owner *owner) close(ctx context.Context) resource.ReleaseResult {
	if owner.closed {
		return resource.ReleaseResult{Quiescent: true, Released: true, Err: owner.closeErr}
	}
	owner.closed = true
	var causes []error
	for index := len(owner.branches) - 1; index >= 0; index-- {
		branch := owner.branches[index]
		if branch.extension != nil {
			err := branch.extension.Sync(context.WithValue(ctx, emissionKey{}, true))
			causes = append(causes, nativeFailure(ErrCleanup, "extension-sync", ctx, err))
		}
		if branch.file != nil {
			causes = append(causes, branch.file.Close())
		}
	}
	owner.closeErr = failureOrNil(ErrCleanup, "close", causes...)
	return resource.ReleaseResult{Quiescent: true, Released: true, Err: owner.closeErr}
}
