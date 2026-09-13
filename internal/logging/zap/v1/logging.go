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
	"os"
	"reflect"
	"runtime"
	"strings"
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
}

type owner struct {
	settings  settings
	extension StructuredSink
	branches  []*branch
	closed    bool
	closeErr  error
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
	if len(options.Outputs) > MaxSinks || len(options.ExtensionLevel) > 16 {
		return resource.Selection[Source]{}, failure(ErrLimit, "bootstrap")
	}
	for _, output := range options.Outputs {
		if len(output.Name) > 64 || len(output.Kind) > 16 || len(output.Level) > 16 || len(output.Directory) > 4096 {
			return resource.Selection[Source]{}, failure(ErrLimit, "bootstrap")
		}
	}
	if extension != nil {
		value := reflect.ValueOf(extension)
		switch value.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			if value.IsNil() {
				return resource.Selection[Source]{}, failure(ErrInput, "extension")
			}
		}
	}
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options),
		Validate: func(value settings) error { return validate(value, extension != nil) }},
		resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: 1, Layers: layers})
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return resource.Select(prepared, func(ctx context.Context, value settings) (resource.Resource[Source], error) {
		owner := &owner{settings: value, extension: extension}
		resourceValue := resource.Resource[Source]{Acquired: true, Capability: Source{owner: owner}, Release: owner.close}
		for _, output := range value.Outputs {
			if ctx.Err() != nil {
				return resourceValue, failure(ErrState, "construct", ctx.Err(), context.Cause(ctx))
			}
			minimum, _ := level(output.Level)
			branch := &branch{name: output.Name, minimum: minimum}
			var writer zapcore.WriteSyncer
			switch output.Kind {
			case "stdout":
				writer = os.Stdout
			case "stderr":
				writer = os.Stderr
			case "file":
				file, err := openRotatingFile(output)
				branch.file = file
				owner.branches = append(owner.branches, branch)
				if err != nil {
					return resourceValue, failure(ErrFile, "open", err)
				}
				writer = file
			}
			branch.writer = &checkedWriter{native: writer, maximum: value.MaxEntryBytes}
			branch.Core = zapcore.NewCore(zapcore.NewJSONEncoder(encoderConfig()), branch.writer, minimum)
			if output.Kind != "file" {
				owner.branches = append(owner.branches, branch)
			}
		}
		if extension != nil {
			minimum, _ := level(value.ExtensionLevel)
			owner.branches = append(owner.branches, &branch{name: "structured", extension: extension, minimum: minimum})
		}
		return resourceValue, nil
	}), nil
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
	return &Logger{owner: source.owner, access: access, inbox: inbox, observer: observer}, nil
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
	if logger == nil || logger.owner == nil || severity < zapcore.DebugLevel || severity > zapcore.ErrorLevel ||
		len(message) > MaxMessageBytes || !utf8.ValidString(message) || len(logger.fields)+len(fields) > MaxFields {
		return nil, failure(ErrInput, "log")
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
	entry := zapcore.Entry{Time: time.Now().UTC(), Level: severity, Message: strings.Clone(message), LoggerName: logger.name}
	if logger.owner.settings.Caller {
		program, file, line, ok := runtime.Caller(1)
		if ok {
			function := ""
			if frame := runtime.FuncForPC(program); frame != nil {
				function = frame.Name()
			}
			if len(file) > 4096 || len(function) > 4096 {
				call.Complete(invocation.Outcome[Result]{Primary: failure(ErrLimit, "caller")})
				return call.Receipt(), nil
			}
			entry.Caller = zapcore.EntryCaller{Defined: true, PC: program, File: file, Line: line, Function: function}
		}
	}
	_ = call.Execute(ctx, invocation.Budget{Limit: logger.owner.settings.Timeout}, func(work context.Context, _ invocation.Scope) invocation.Outcome[Result] {
		work = context.WithValue(work, emissionKey{}, true)
		results := make([]SinkResult, 0, len(logger.owner.branches))
		var causes []error
		for _, branch := range logger.owner.branches {
			branch.ctx = work
			branch.result = SinkResult{Name: branch.name, State: Filtered}
			if branch.writer != nil {
				branch.writer.bytes = 0
			}
			checked := branch.Check(entry, nil)
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
		isolated := append([]zapcore.Field(nil), fields...)
		for index, field := range isolated {
			if field.Type == zapcore.BinaryType || field.Type == zapcore.ByteStringType {
				isolated[index].Interface = append([]byte(nil), field.Interface.([]byte)...)
			}
		}
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
