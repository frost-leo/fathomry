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

package doris

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
)

// Batch is one native-table JSON-array load, not an external Iceberg write.
// JSON is borrowed, without concurrent mutation, until StreamLoad returns.
// Label is caller-assigned (1–128 ASCII letters/digits/_/-), database-scoped;
// the caller owns payload/label reconciliation and retention policy.
type Batch struct {
	private
	Table string
	Label string
	JSON  []byte
}

// LoadState reports effect evidence, not a retry or business-item policy.
type LoadState uint8

const (
	LoadUnknown LoadState = iota
	LoadNotDispatched
	LoadPending
	LoadCommitted
	LoadVisible
	LoadRejected
	LoadAborted
)

// LoadEvidence is a value copy. Inspection deliberately exposes identifiers.
// State is the server's load/label evidence; rows are known only when RowsKnown.
// InspectLabel has no payload, table, transaction or row-quality witness.
type LoadEvidence struct {
	private
	Database      string
	Table         string
	Label         string
	PayloadSHA256 [32]byte
	State         LoadState
	// HTTPStatus is the last observed response status; zero is unobserved.
	HTTPStatus       int
	TransactionID    int64
	TransactionKnown bool
	RowsKnown        bool
	TotalRows        int64
	LoadedRows       int64
	FilteredRows     int64
	UnselectedRows   int64
	Duplicate        bool
	// ExistingJobStatus is a recognized server token, not proof that this
	// caller's payload equals the prior job's payload.
	ExistingJobStatus string
}

// StreamLoad validates and sends one bounded JSON physical batch synchronously.
// strict_mode=true, max_filter_ratio=0, group_commit=off_mode and single-phase
// loading are fixed. Filtered/unselected rows remain failures even if committed.
// Malformed replies never become success; mutation retries are never automatic.
func (c *Client) StreamLoad(ctx context.Context, id fault.Correlation, batch Batch) (*invocation.Receipt[Result], error) {
	if c == nil || c.owner == nil || !identifier(batch.Table, 128) || !validLabel(batch.Label) ||
		len(batch.JSON) == 0 || len(batch.JSON) > c.owner.settings.MaxBatchBytes {
		return nil, failure(ErrInput, "batch")
	}
	if len(c.owner.settings.HTTPOrigins) == 0 {
		return nil, failure(ErrUnsupported, "stream-load")
	}
	call, work, cancel, err := c.begin(ctx, id, "stream-load")
	if err != nil {
		return nil, err
	}
	if work == nil {
		return call.Receipt(), nil
	}
	defer cancel()
	payload := bytes.Clone(batch.JSON)
	data := &resultData{loadPresent: true, load: LoadEvidence{Database: c.owner.settings.Database, Table: batch.Table,
		Label: batch.Label, PayloadSHA256: sha256.Sum256(payload), State: LoadNotDispatched}}
	expected, err := validateBatch(payload, c.owner.settings.MaxRows)
	var cleanup error
	if err == nil {
		headers := http.Header{}
		for key, value := range map[string]string{"Content-Type": "application/json", "format": "json", "strip_outer_array": "true",
			"read_json_by_line": "false", "strict_mode": "true", "max_filter_ratio": "0", "group_commit": "off_mode", "two_phase_commit": "false",
			"Expect": "100-continue", "label": batch.Label, "timeout": strconv.Itoa(max(1, int(c.owner.settings.Timeout.Seconds())))} {
			headers.Set(key, value)
		}
		var body []byte
		body, err, cleanup = c.exchange(work, call, http.MethodPut, "/api/"+c.owner.settings.Database+"/"+batch.Table+"/_stream_load", "", headers, payload, data)
		if err == nil {
			err = parseLoad(body, expected, &data.load)
		}
	}
	data.complete = err == nil && data.load.State == LoadVisible && data.load.RowsKnown
	call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: err, Cleanup: cleanup})
	return call.Receipt(), nil
}

// InspectLabel makes one bounded observation; it neither polls nor resubmits.
// UNKNOWN (including expired/count-evicted labels) is uncertainty, not absence.
// Visible describes a retained label, never independently verified payload rows.
func (c *Client) InspectLabel(ctx context.Context, id fault.Correlation, label string) (*invocation.Receipt[Result], error) {
	if c == nil || c.owner == nil || !validLabel(label) {
		return nil, failure(ErrInput, "label")
	}
	if len(c.owner.settings.HTTPOrigins) == 0 {
		return nil, failure(ErrUnsupported, "label")
	}
	call, work, cancel, err := c.begin(ctx, id, "inspect-label")
	if err != nil {
		return nil, err
	}
	if work == nil {
		return call.Receipt(), nil
	}
	defer cancel()
	data := &resultData{loadPresent: true, load: LoadEvidence{Database: c.owner.settings.Database, Label: label, State: LoadUnknown}}
	body, err, cleanup := c.exchange(work, call, http.MethodGet, "/api/"+c.owner.settings.Database+"/get_load_state",
		url.Values{"label": []string{label}}.Encode(), http.Header{}, nil, data)
	if err == nil {
		err = parseLabel(body, &data.load)
	}
	data.complete = err == nil
	call.Complete(invocation.Outcome[Result]{Present: true, Value: Result{data: data}, Primary: err, Cleanup: cleanup})
	return call.Receipt(), nil
}
func validateBatch(payload []byte, maxRows int) (int, error) {
	if !utf8.Valid(payload) {
		return 0, failure(ErrInput, "json-encoding")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('[') {
		return 0, failure(ErrInput, "json-array")
	}
	rows := 0
	for decoder.More() {
		rows++
		if rows > maxRows {
			return 0, failure(ErrLimit, "batch-rows")
		}
		var row json.RawMessage
		if err := decoder.Decode(&row); err != nil {
			return 0, failure(ErrInput, "json-row")
		}
		if _, err := jsonObject(row); err != nil {
			return 0, failure(ErrInput, "json-row")
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim(']') || rows == 0 {
		return 0, failure(ErrInput, "json-array")
	}
	if _, err = decoder.Token(); err != io.EOF {
		return 0, failure(ErrInput, "json-tail")
	}
	return rows, nil
}
func jsonObject(body []byte) (map[string]json.RawMessage, error) {
	if !utf8.Valid(body) {
		return nil, failure(ErrProtocol, "json-encoding")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, failure(ErrProtocol, "json-object")
	}
	object := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err = decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || len(object) >= 128 {
			return nil, failure(ErrProtocol, "json-field")
		}
		if _, exists := object[key]; exists {
			return nil, failure(ErrProtocol, "duplicate-field")
		}
		var value json.RawMessage
		if err = decoder.Decode(&value); err != nil {
			return nil, failure(ErrProtocol, "json-value")
		}
		object[key] = value
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, failure(ErrProtocol, "json-object")
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, failure(ErrProtocol, "json-tail")
	}
	return object, nil
}
func stringField(object map[string]json.RawMessage, key string) (string, bool) {
	var value string
	raw, exists := object[key]
	if !exists || bytes.Equal(raw, []byte("null")) || json.Unmarshal(raw, &value) != nil {
		return "", false
	}
	return value, true
}
func numberField(object map[string]json.RawMessage, key string) (int64, bool) {
	raw, exists := object[key]
	var value int64
	if !exists || bytes.Equal(raw, []byte("null")) || json.Unmarshal(raw, &value) != nil || value < 0 {
		return 0, false
	}
	return value, true
}
func parseLoad(body []byte, expected int, evidence *LoadEvidence) error {
	object, err := jsonObject(body)
	if err != nil {
		return err
	}
	label, ok := stringField(object, "Label")
	if !ok || label != evidence.Label {
		return failure(ErrProtocol, "load-identity")
	}
	status, ok := stringField(object, "Status")
	if !ok {
		return failure(ErrProtocol, "load-status")
	}
	if raw, exists := object["TwoPhaseCommit"]; exists && !bytes.Equal(raw, []byte(`"false"`)) && !bytes.Equal(raw, []byte("false")) {
		return failure(ErrUnsupported, "two-phase-response")
	}
	txn, known := numberField(object, "TxnId")
	evidence.TransactionID, evidence.TransactionKnown = txn, known && txn > 0
	switch status {
	case "Label Already Exists":
		evidence.Duplicate = true
		existing, _ := stringField(object, "ExistingJobStatus")
		if existing == "RUNNING" || existing == "FINISHED" {
			evidence.ExistingJobStatus = existing
		}
		return failure(ErrDuplicate, "load")
	case "Fail":
		evidence.State = LoadRejected
		_ = parseCounts(object, evidence)
		return failure(ErrLoad, "load")
	case "Success", "Publish Timeout":
		if !evidence.TransactionKnown {
			return failure(ErrProtocol, "transaction")
		}
		if status == "Success" {
			evidence.State = LoadVisible
		} else {
			evidence.State = LoadCommitted
		}
	default:
		return failure(ErrProtocol, "load-status")
	}
	if err := parseCounts(object, evidence); err != nil {
		return err
	}
	if evidence.TotalRows != int64(expected) || evidence.FilteredRows != 0 || evidence.UnselectedRows != 0 {
		return failure(ErrRowQuality, "load")
	}
	if evidence.State != LoadVisible {
		return failure(ErrUncertain, "visibility")
	}
	return nil
}
func parseCounts(object map[string]json.RawMessage, evidence *LoadEvidence) error {
	total, totalOK := numberField(object, "NumberTotalRows")
	loaded, loadedOK := numberField(object, "NumberLoadedRows")
	filtered, filteredOK := numberField(object, "NumberFilteredRows")
	unselected, unselectedOK := numberField(object, "NumberUnselectedRows")
	if !totalOK || !loadedOK || !filteredOK || !unselectedOK ||
		loaded > total || filtered > total-loaded || unselected != total-loaded-filtered {
		return failure(ErrProtocol, "row-counts")
	}
	evidence.RowsKnown = true
	evidence.TotalRows, evidence.LoadedRows, evidence.FilteredRows, evidence.UnselectedRows = total, loaded, filtered, unselected
	return nil
}
func parseLabel(body []byte, evidence *LoadEvidence) error {
	object, err := jsonObject(body)
	if err != nil {
		return err
	}
	code, ok := object["code"]
	if !ok || (!bytes.Equal(code, []byte("0")) && !bytes.Equal(code, []byte(`"0"`))) {
		return failure(ErrProtocol, "label-code")
	}
	message, ok := stringField(object, "msg")
	if !ok || message != "success" {
		return failure(ErrProtocol, "label-message")
	}
	state, ok := stringField(object, "data")
	if !ok {
		return failure(ErrProtocol, "label-state")
	}
	switch state {
	case "UNKNOWN":
		evidence.State = LoadUnknown
	case "PREPARE", "PRECOMMITTED":
		evidence.State = LoadPending
	case "COMMITTED":
		evidence.State = LoadCommitted
	case "VISIBLE":
		evidence.State = LoadVisible
		return nil
	case "ABORTED":
		evidence.State = LoadAborted
		return failure(ErrLoad, "label")
	default:
		return failure(ErrProtocol, "label-state")
	}
	return failure(ErrUncertain, "label")
}
