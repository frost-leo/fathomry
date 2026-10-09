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
	"encoding/json"
	"io"
	"log/slog"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/resource"
)

const (
	MaxDerivedViews       = 128
	MaxDerivedBytes int64 = 8 << 20
)

// BindingsV1 supplies explicitly borrowed authority separately from preparation.
// Every entry must match a selected name and kind exactly. Select copies these
// maps, not the handles; borrowed resources remain caller-owned and concurrent
// sharing across independent physical sources needs caller synchronization.
type BindingsV1 struct {
	private
	Writers        map[string]io.Writer
	Records        map[string]RecordWriter
	ManagedRecords map[string]ManagedRecordWriter
}

// Prepared holds immutable final data and authoritative preconstruction budgets.
// It contains no writer, SDK handle, native probe, file or background worker.
type Prepared struct {
	private
	state *preparedState
}

type preparedState struct {
	configuration resource.Prepared[settings]
	value         settings
	metadata      Metadata
}

// Metadata describes one physical source's declared envelopes, not measured RSS,
// filesystem allocation/free space or storage retained by borrowed sink owners.
// SourceBytes includes the cumulative DerivationBytes allowance; PolicyBytes
// covers a separately retained level policy without a new output or allowance.
// ViewBytes includes the eight additional retained bytes per attribute beyond
// legacy input charging, plus its facade. All logical views share Limits' single
// active operation and original FIFO queue. FileBytes is steady
// managed content; MaintenanceFileBytes includes conservative transition/failure
// content. Neither is a physical disk quota or a durability guarantee.
type Metadata struct {
	Limits                                         resource.Limits
	WorkBytes, EvidenceBytes, SourceBytes          int64
	PolicyBytes, ViewBytes, DerivationBytes        int64
	FileBytes, MaintenanceFileBytes                int64
	Sinks, Files, Writers, Records, ManagedRecords int
	MaxRecordBytes, MaxDerivedViews                int
	Timeout                                        time.Duration
}

// PrepareV1 validates and freezes inert declarations and final overlays. Legacy
// live Writer/Records fields belong to Select, not this data-only preparation.
// Inputs are borrowed until return and must not be concurrently mutated.
func PrepareV1(options OptionsV1, layers ...resource.Layer) (Prepared, error) {
	if err := preflight(options); err != nil {
		return Prepared{}, err
	}
	for _, sink := range options.Sinks {
		if sink.Writer != nil || sink.Records != nil {
			return Prepared{}, failure(ErrInput, "live-preparation-binding")
		}
	}
	state := &preparedState{}
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: defaults(options), Validate: func(value settings) error {
		if err := validate(value); err != nil {
			return err
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return failure(ErrInput, "prepare", err)
		}
		state.value = value
		state.metadata = value.metadata(len(encoded))
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

// Description returns detached logical preparation identity. WithPolicy retains
// the original physical source identity in invocation evidence independently.
func (prepared Prepared) Description() resource.Description {
	if prepared.state == nil {
		return resource.Description{}
	}
	return prepared.state.configuration.Description()
}

// Options returns detached, deliberately sensitive effective data, including
// kinds and file defaults but never Writer/Records/managed runtime handles.
func (prepared Prepared) Options() OptionsV1 {
	if prepared.state == nil {
		return OptionsV1{}
	}
	value := prepared.state.value
	options := OptionsV1{Name: prepared.Description().Identity.Name, MinLevel: Level(value.MinLevel),
		MaxRecordBytes: value.MaxRecordBytes, Timeout: value.Timeout, QueuedCalls: value.QueuedCalls, Caller: value.Caller,
		Sinks: make([]SinkV1, len(value.Sinks))}
	for index, sink := range value.Sinks {
		options.Sinks[index] = SinkV1{Name: sink.Name, Kind: sink.Kind, MinLevel: Level(sink.MinLevel)}
		if sink.File != nil {
			options.Sinks[index].File = &FileOptionsV1{Directory: sink.File.Directory, MaxBytes: sink.File.MaxBytes,
				Backups: sink.File.Backups, Compress: sink.File.Compress}
		}
	}
	return options
}

// Select freezes exact bindings and creates only an inert selection. Assembly
// receives the same prepared data and acquires each selected physical output.
func (prepared Prepared) Select(bindings BindingsV1) (resource.Selection[Source], error) {
	if prepared.state == nil {
		return resource.Selection[Source]{}, failure(ErrInput, "unprepared")
	}
	handles, err := prepared.bindings(bindings)
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	return resource.Select(prepared.state.configuration, func(ctx context.Context, value settings) (resource.Resource[Source], error) {
		owner := &outputs{prepared: prepared, settings: value}
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

func (prepared Prepared) bindings(bindings BindingsV1) (map[string]borrowedSink, error) {
	if len(bindings.Writers)+len(bindings.Records)+len(bindings.ManagedRecords) > MaxSinks {
		return nil, failure(ErrLimit, "sink-bindings")
	}
	handles := make(map[string]borrowedSink)
	for _, sink := range prepared.state.value.Sinks {
		writer, hasWriter := bindings.Writers[sink.Name]
		records, hasRecords := bindings.Records[sink.Name]
		managed, hasManaged := bindings.ManagedRecords[sink.Name]
		switch sink.Kind {
		case "writer":
			if !hasWriter || hasRecords || hasManaged || nilHandle(writer) {
				return nil, failure(ErrInput, "sink-binding")
			}
			handles[sink.Name] = borrowedSink{writer: writer}
		case "record":
			if hasWriter || !hasRecords || hasManaged || nilHandle(records) {
				return nil, failure(ErrInput, "sink-binding")
			}
			handles[sink.Name] = borrowedSink{records: records}
		case "managed-record":
			if hasWriter || hasRecords || !hasManaged || nilHandle(managed) {
				return nil, failure(ErrInput, "sink-binding")
			}
			handles[sink.Name] = borrowedSink{managed: managed}
		case "file":
			if hasWriter || hasRecords || hasManaged {
				return nil, failure(ErrInput, "sink-binding")
			}
		}
	}
	if len(handles) != len(bindings.Writers)+len(bindings.Records)+len(bindings.ManagedRecords) {
		return nil, failure(ErrInput, "unused-sink-binding")
	}
	return handles, nil
}

// PhysicalEquivalent permits only source/sink threshold changes. Source name,
// binding kinds/order, record/caller/queue policy and every file option must match.
func (prepared Prepared) PhysicalEquivalent(other Prepared) bool {
	if prepared.state == nil || other.state == nil || prepared.Description().Identity != other.Description().Identity {
		return false
	}
	left, right := prepared.state.value, other.state.value
	if left.MaxRecordBytes != right.MaxRecordBytes || left.Timeout != right.Timeout || left.QueuedCalls != right.QueuedCalls ||
		left.Caller != right.Caller || len(left.Sinks) != len(right.Sinks) {
		return false
	}
	for index, sink := range left.Sinks {
		candidate := right.Sinks[index]
		if sink.Name != candidate.Name || sink.Kind != candidate.Kind || (sink.File == nil) != (candidate.File == nil) {
			return false
		}
		if sink.File != nil && *sink.File != *candidate.File {
			return false
		}
	}
	return true
}

// WithPolicy reuses the exact original owner, Access, outputs, failure state,
// queue and evidence binding. It supplies no replacement runtime dependency.
// Composition bounds retained policy lifetimes; policy adoption never consumes
// the separate cumulative With-attribute allowance.
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

func (logger *Logger) enabled(index int, severity Level) bool {
	return severity.rank() >= Level(logger.policy.value.MinLevel).rank() &&
		severity.rank() >= Level(logger.policy.value.Sinks[index].MinLevel).rank()
}

// Enabled is a frozen-threshold hint only, not source/native-global readiness,
// allowance or eventual output. Seven supported values are severities only.
func (logger *Logger) Enabled(severity Level) bool {
	if logger == nil || logger.policy == nil || severity.rank() < 0 {
		return false
	}
	for index := range logger.policy.value.Sinks {
		if logger.enabled(index, severity) {
			return true
		}
	}
	return false
}

func (owner *outputs) freezeDerivation(attributes []slog.Attr, bytes int64) ([]slog.Attr, error) {
	owner.derivationMu.Lock()
	defer owner.derivationMu.Unlock()
	if owner.derivationClosed {
		return nil, failure(ErrState, "derivation")
	}
	if bytes < 1 || owner.derivations == MaxDerivedViews || bytes > MaxDerivedBytes-owner.derivationBytes {
		return nil, failure(ErrLimit, "derivation")
	}
	owner.derivations++
	owner.derivationBytes += bytes
	return copyAttributes(attributes), nil
}

func (value settings) metadata(encodedBytes int) Metadata {
	// A future level-only view must fit the original reserved policy envelope.
	policyBytes := int64(encodedBytes + len("panic") - len(value.MinLevel))
	for _, sink := range value.Sinks {
		policyBytes += int64(len("panic") - len(sink.MinLevel))
	}
	metadata := Metadata{Limits: value.limits(), WorkBytes: value.reservation(), EvidenceBytes: value.evidenceReservation(),
		PolicyBytes: 2*policyBytes + 16<<10, ViewBytes: int64(value.MaxRecordBytes) + int64(MaxAttributes)*8 + 256, DerivationBytes: MaxDerivedBytes,
		MaxDerivedViews: MaxDerivedViews, MaxRecordBytes: value.MaxRecordBytes, Sinks: len(value.Sinks), Timeout: value.Timeout}
	metadata.SourceBytes = metadata.PolicyBytes + metadata.DerivationBytes + int64(len(value.Sinks))*(16<<10) + 64<<10
	for _, sink := range value.Sinks {
		switch sink.Kind {
		case "writer":
			metadata.Writers++
		case "record":
			metadata.Records++
		case "managed-record":
			metadata.ManagedRecords++
		case "file":
			metadata.Files++
			file := sink.File
			metadata.SourceBytes += int64(file.Backups+2)*128 + 12<<10
			archive := file.MaxBytes
			if file.Compress {
				archive = 2*file.MaxBytes + 64<<10
			}
			metadata.FileBytes += file.MaxBytes + int64(file.Backups)*archive
			metadata.MaintenanceFileBytes += file.MaxBytes + int64(file.Backups+2)*archive
		}
	}
	return metadata
}

func preflight(options OptionsV1) error {
	if len(options.Sinks) > MaxSinks {
		return failure(ErrInput, "sink-count")
	}
	for _, sink := range options.Sinks {
		if sink.File != nil && len(sink.File.Directory) > 4096 {
			return failure(ErrInput, "directory")
		}
	}
	if !withinBootstrapBudget(options) {
		location := fault.Context{Provider: ProviderID, Operation: "prepare"}
		if label(options.Name) {
			location.Source = options.Name
		}
		return resource.ErrConfiguration.New(location, failure(ErrLimit, "bootstrap-size"))
	}
	return nil
}

func legacySelect(options OptionsV1, layers ...resource.Layer) (resource.Selection[Source], error) {
	if len(options.Sinks) == 0 {
		return resource.Selection[Source]{}, failure(ErrInput, "sink-count")
	}
	if err := preflight(options); err != nil {
		return resource.Selection[Source]{}, err
	}
	inert := options
	inert.Sinks = make([]SinkV1, len(options.Sinks))
	original := make(map[string]borrowedSink)
	for index, sink := range options.Sinks {
		count := 0
		kind := ""
		if sink.Writer != nil {
			count++
			kind = "writer"
		}
		if sink.Records != nil {
			count++
			kind = "record"
		}
		if sink.File != nil {
			count++
			kind = "file"
		}
		if count != 1 || sink.Kind != "" && sink.Kind != kind || sink.Writer != nil && nilHandle(sink.Writer) || sink.Records != nil && nilHandle(sink.Records) {
			return resource.Selection[Source]{}, failure(ErrInput, "sink-handles")
		}
		if kind != "file" {
			if _, exists := original[sink.Name]; exists {
				return resource.Selection[Source]{}, failure(ErrInput, "sink-name")
			}
			original[sink.Name] = borrowedSink{writer: sink.Writer, records: sink.Records}
		}
		inert.Sinks[index] = sink
		inert.Sinks[index].Kind = kind
		inert.Sinks[index].Writer, inert.Sinks[index].Records = nil, nil
	}
	prepared, err := PrepareV1(inert, layers...)
	if err != nil {
		return resource.Selection[Source]{}, err
	}
	bindings := BindingsV1{Writers: make(map[string]io.Writer), Records: make(map[string]RecordWriter)}
	invalidBinding := func() (resource.Selection[Source], error) {
		return resource.Selection[Source]{}, resource.ErrConfiguration.New(fault.Context{Provider: ProviderID, Source: prepared.Description().Identity.Name, Operation: "prepare"}, failure(ErrInput, "sink-binding"))
	}
	for _, sink := range prepared.state.value.Sinks {
		handle, found := original[sink.Name]
		switch sink.Kind {
		case "writer":
			if !found || handle.writer == nil {
				return invalidBinding()
			}
			bindings.Writers[sink.Name] = handle.writer
		case "record":
			if !found || handle.records == nil {
				return invalidBinding()
			}
			bindings.Records[sink.Name] = handle.records
		case "file":
			if found {
				return invalidBinding()
			}
		case "managed-record":
			return invalidBinding()
		}
	}
	return prepared.Select(bindings)
}
