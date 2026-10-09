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
	"encoding/json"
	"os"
	"reflect"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
	"go.uber.org/zap/zapcore"
)

const (
	MaxDerivedViews       = 128
	MaxDerivedBytes int64 = 8 << 20
)

// Prepared freezes final configuration without opening a file or calling a sink.
// Copies share immutable data only. Select constructs a separate physical owner;
// WithPolicy instead shares an existing owner's original allowance and outputs.
type Prepared struct {
	private
	state *preparedState
}

type preparedState struct {
	configuration resource.Prepared[settings]
	value         settings
	structured    bool
	metadata      Metadata
	levels        []zapcore.Level
}

// Metadata describes authoritative accounting for this exact frozen selection.
// SourceBytes includes the one physical source and its shared DerivationBytes
// reservation. PolicyBytes covers a separately retained frozen level policy;
// sharing a policy does not create another source or queue. ViewBytes bounds one
// With/Named facade and copied fields; accepted derivations cumulatively consume
// MaxDerivedViews and DerivationBytes until that physical owner is discarded.
// Limits describe the single physical active call and shared FIFO queue.
//
// FileBytes and MaintenanceFileBytes are logical managed-content ceilings, not
// disk quotas or free-space reservations. Memory figures are declared envelopes,
// not measured RSS or arbitrary borrowed-sink/caller/error-graph storage.
type Metadata struct {
	Limits                                  resource.Limits
	WorkBytes, EvidenceBytes, SourceBytes   int64
	PolicyBytes, ViewBytes, DerivationBytes int64
	FileBytes, MaintenanceFileBytes         int64
	Outputs, Files, MaxDerivedViews         int
	MaxEntryBytes                           int
	Timeout                                 time.Duration
	Structured                              bool
}

// PrepareV1 freezes the resolved settings and budget before native acquisition.
// structured declares whether Select must receive an explicit borrowed sink.
// Inputs are borrowed until return and may not be mutated concurrently.
func PrepareV1(options OptionsV1, structured bool, layers ...resource.Layer) (Prepared, error) {
	if err := bootstrapBound(options); err != nil {
		return Prepared{}, err
	}
	state := &preparedState{structured: structured}
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: func(value settings) error {
		if err := validate(value, structured); err != nil {
			return err
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return failure(ErrInput, "prepare", err)
		}
		state.value = value
		state.metadata = value.metadata(structured, len(encoded))
		for _, output := range value.Outputs {
			minimum, _ := level(output.Level)
			state.levels = append(state.levels, minimum)
		}
		if structured {
			minimum, _ := level(value.ExtensionLevel)
			state.levels = append(state.levels, minimum)
		}
		return nil
	}}, resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: 1, Layers: layers})
	if err != nil {
		return Prepared{}, err
	}
	state.configuration = prepared
	return Prepared{state: state}, nil
}

func (prepared Prepared) Metadata() Metadata {
	if prepared.state == nil {
		return Metadata{}
	}
	return prepared.state.metadata
}

// Description returns detached preparation identity, separately from the
// original physical source attribution retained by WithPolicy views.
func (prepared Prepared) Description() resource.Description {
	if prepared.state == nil {
		return resource.Description{}
	}
	return prepared.state.configuration.Description()
}

// Options returns detached, deliberately sensitive effective configuration.
// It includes native defaults but no borrowed runtime dependency. Explicit zero
// overlays must still be retained when using this snapshot for another Prepare.
func (prepared Prepared) Options() OptionsV1 {
	if prepared.state == nil {
		return OptionsV1{}
	}
	value := prepared.state.value
	options := OptionsV1{Name: prepared.Description().Identity.Name, ExtensionLevel: value.ExtensionLevel,
		QueuedCalls: value.QueuedCalls, Timeout: value.Timeout, MaxEntryBytes: value.MaxEntryBytes, Caller: value.Caller}
	if value.Outputs != nil {
		options.Outputs = make([]OutputV1, len(value.Outputs))
		for index, output := range value.Outputs {
			options.Outputs[index] = OutputV1{Name: output.Name, Kind: output.Kind, Level: output.Level,
				Directory: output.Directory, MaxFileBytes: output.MaxFileBytes, MaxBackups: output.MaxBackups, Compress: output.Compress}
		}
	}
	return options
}

// Select validates the declared dependency presence before returning an inert
// source selection. Each assembly acquires an independent physical owner.
func (prepared Prepared) Select(extension StructuredSink) (resource.Selection[Source], error) {
	if prepared.state == nil || prepared.state.structured != (extension != nil) {
		return resource.Selection[Source]{}, failure(ErrInput, "extension")
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
	return resource.Select(prepared.state.configuration, func(ctx context.Context, value settings) (resource.Resource[Source], error) {
		owner := &owner{settings: value, extension: extension, prepared: prepared}
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

// PhysicalEquivalent admits only threshold changes. Name, output order,
// dependency presence, caller/encoding, quotas and file policy remain identical.
// Preparation revisions/provenance are identities, not physical configuration.
func (prepared Prepared) PhysicalEquivalent(other Prepared) bool {
	if prepared.state == nil || other.state == nil || prepared.state.structured != other.state.structured ||
		prepared.Description().Identity != other.Description().Identity {
		return false
	}
	left, right := prepared.state.value, other.state.value
	if left.QueuedCalls != right.QueuedCalls || left.Timeout != right.Timeout || left.MaxEntryBytes != right.MaxEntryBytes ||
		left.Caller != right.Caller || len(left.Outputs) != len(right.Outputs) {
		return false
	}
	for index, output := range left.Outputs {
		candidate := right.Outputs[index]
		output.Level, candidate.Level = "", ""
		if output != candidate {
			return false
		}
	}
	return true
}

// WithPolicy makes an immutable filtering view without another assembly, sink,
// queue, allowance or dependency. The caller owns its bounded retained lifetime.
// Physical source attribution is unchanged; PolicyDescription identifies this
// logical policy. Repeated policy adoption does not consume With/Named quotas.
func (logger *Logger) WithPolicy(prepared Prepared) (*Logger, error) {
	if logger == nil || logger.owner == nil || logger.policy == nil {
		return nil, failure(ErrState, "policy")
	}
	if prepared.state == nil {
		return nil, failure(ErrInput, "policy")
	}
	if !logger.owner.prepared.PhysicalEquivalent(prepared) {
		return nil, failure(ErrUnsupported, "policy")
	}
	view := *logger
	view.policy = prepared.state
	return &view, nil
}

func (logger *Logger) PolicyDescription() resource.Description {
	if logger == nil || logger.policy == nil {
		return resource.Description{}
	}
	return logger.policy.configuration.Description()
}

func (logger *Logger) minimum(index int) zapcore.Level { return logger.policy.levels[index] }

// Enabled is a low-cost immutable threshold hint, not admission, source readiness
// or a promised later Follow generation. Unsupported terminal levels are false.
func (logger *Logger) Enabled(severity zapcore.Level) bool {
	if logger == nil || logger.policy == nil || severity < zapcore.DebugLevel || severity > zapcore.ErrorLevel {
		return false
	}
	for _, minimum := range logger.policy.levels {
		if minimum.Enabled(severity) {
			return true
		}
	}
	return false
}

func (owner *owner) reserveDerivation(bytes int64) error {
	owner.derivationMu.Lock()
	defer owner.derivationMu.Unlock()
	if bytes < 1 || bytes > MaxDerivedBytes-owner.derivationBytes || owner.derivations == MaxDerivedViews {
		return failure(ErrLimit, "derivation")
	}
	owner.derivations++
	owner.derivationBytes += bytes
	return nil
}

func (value settings) metadata(structured bool, encodedBytes int) Metadata {
	outputs := len(value.Outputs)
	if structured {
		outputs++
	}
	// Every admissible threshold view must fit the original source reservation,
	// including a later change from four-byte "info" to five-byte "debug".
	policyBytes := int64(encodedBytes + len("debug") - len(value.ExtensionLevel))
	for _, output := range value.Outputs {
		policyBytes += int64(len("debug") - len(output.Level))
	}
	metadata := Metadata{Limits: value.limits(), WorkBytes: value.reservation(), EvidenceBytes: value.evidenceReservation(),
		PolicyBytes: 2*policyBytes + 16<<10, ViewBytes: MaxFieldBytes + 512,
		DerivationBytes: MaxDerivedBytes, MaxDerivedViews: MaxDerivedViews,
		Outputs: outputs, Structured: structured, MaxEntryBytes: value.MaxEntryBytes, Timeout: value.Timeout}
	// Frozen/decoded settings, physical native cores and bounded file metadata
	// coexist with cumulatively reserved derived field storage. Borrowed sink
	// resources are independently budgeted by their owner, never charged twice.
	metadata.SourceBytes = metadata.PolicyBytes + metadata.DerivationBytes + int64(outputs)*(16<<10) + 64<<10
	for _, output := range value.Outputs {
		if output.Kind != "file" {
			continue
		}
		metadata.Files++
		metadata.SourceBytes += int64(output.MaxBackups+2)*128 + 8192
		maximum := output.MaxFileBytes
		if output.Compress {
			compressed := maximum + maximum/100 + 64<<10
			metadata.FileBytes += int64(output.MaxBackups)*compressed + maximum
			metadata.MaintenanceFileBytes += int64(output.MaxBackups+1)*compressed + maximum
		} else {
			metadata.FileBytes += int64(output.MaxBackups+1) * maximum
			metadata.MaintenanceFileBytes += int64(output.MaxBackups+1) * maximum
		}
	}
	return metadata
}

func bootstrapBound(options OptionsV1) error {
	if len(options.Outputs) > MaxSinks || len(options.ExtensionLevel) > 16 {
		return failure(ErrLimit, "bootstrap")
	}
	for _, output := range options.Outputs {
		if len(output.Name) > 64 || len(output.Kind) > 16 || len(output.Level) > 16 || len(output.Directory) > 4096 {
			return failure(ErrLimit, "bootstrap")
		}
	}
	return nil
}
