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

package catalog

import (
	"bytes"
	"context"
	"encoding/hex"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

// MaxComparisonRows is the full-profile anchor row budget, including its root
// observation. The caller's Adapter MaxRows must cover the actual selected shape;
// the CLI opts into the required selected shape for compare, not other operations.
const MaxComparisonRows = 1 + declaration.MaxWorkflows + declaration.MaxModes + MaxResources
const MaxComparisonProblems = 128
const maxComparisonInputBytes = 6 << 20

// ComparisonProblem identifies a missing or different selected input without
// exposing stored/local payloads, English text, translator notes or resource keys.
// Paths index canonical prepared contents/resources, not arbitrary object names.
type ComparisonProblem struct {
	Kind     string                `json:"kind"`
	Path     string                `json:"path"`
	Identity *declaration.Identity `json:"identity,omitempty"`
}

// Comparison is one completed read observation, not registration acknowledgement,
// source freshness or Worker readiness. Counts concern selected document/resource
// units only. Initialization-only category/tags and all management state are ignored.
// An absent definition is a completed non-match; schema/transport/integrity failures
// return no completed report. Matching does not acknowledge an earlier lost COMMIT.
type Comparison struct {
	Namespace        string              `json:"namespace"`
	Code             string              `json:"code"`
	DefinitionExists bool                `json:"definition_exists"`
	DefinitionID     string              `json:"definition_id,omitempty"`
	Complete         bool                `json:"complete"`
	Equal            bool                `json:"equal"`
	Contents         int                 `json:"contents"`
	Resources        int                 `json:"resources"`
	Matched          int                 `json:"matched"`
	Missing          int                 `json:"missing"`
	Different        int                 `json:"different"`
	Problems         []ComparisonProblem `json:"problems"`
	Truncated        bool                `json:"truncated"`
}

func (Comparison) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("catalog.Comparison")) }
func (Comparison) LogValue() slog.Value           { return slog.StringValue("catalog.Comparison") }

type comparisonDocument struct {
	Index    int    `json:"i"`
	Version  string `json:"v"`
	Mode     string `json:"m"`
	Revision string `json:"r"`
}
type comparisonResource struct {
	Index int    `json:"i"`
	Scope string `json:"s"`
	Key   string `json:"k"`
}
type comparisonInput struct {
	Documents []comparisonDocument `json:"d"`
	Resources []comparisonResource `json:"t"`
}
type comparisonAnchor struct {
	definitionID string
	documents    [][][]byte
	resources    [][][]byte
}

func prepareComparison(prepared *PreparedRegistration) ([]byte, error) {
	if prepared == nil || prepared.state == nil {
		return nil, readError(ErrHandle, "prepared")
	}
	value := comparisonInput{Documents: []comparisonDocument{}, Resources: []comparisonResource{}}
	for index, content := range prepared.state.contents {
		id := content.Entry.Identity
		value.Documents = append(value.Documents, comparisonDocument{index, id.Version, id.Mode, id.Revision})
	}
	for index, resource := range prepared.state.resources {
		value.Resources = append(value.Resources, comparisonResource{index, resource.scope, resource.key})
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, readError(ErrInput, "comparison_input", err)
	}
	if len(raw) > maxComparisonInputBytes {
		return nil, readError(ErrLimit, "comparison_input")
	}
	return raw, nil
}

// Compare observes only the prepared immutable selection and publisher-owned
// English/context. One anchor SELECT captures identities and mutable metadata;
// document bodies are fetched by exact anchored identity/profile/digest and
// validated together through declaration authority. It never registers, repairs,
// enables or retries anything. Close owns any unfinished read cleanup.
func (catalog *Catalog) Compare(ctx context.Context, prepared *PreparedRegistration) (Comparison, error) {
	var result Comparison
	raw, err := prepareComparison(prepared)
	if err != nil {
		return result, err
	}
	err = catalog.read(ctx, func(work context.Context, tx *publicationTx) error {
		query, args := tx.comparisonSQL(prepared.state, raw)
		rows, err := tx.readAnchor(work, MaxComparisonRows, query, args...)
		if err != nil {
			return err
		}
		anchor, err := tx.comparisonRows(rows, prepared.state)
		if err != nil {
			return err
		}
		result, err = tx.compareAnchored(work, prepared.state, anchor)
		return err
	})
	if err != nil {
		return Comparison{}, err
	}
	return result, nil
}

func (tx *publicationTx) comparisonSQL(input *registration, raw []byte) (string, []any) {
	args := []any{string(raw), input.info.Namespace, input.info.Code}
	request := "SELECT CAST($1 AS jsonb) AS payload,CAST($2 AS text) AS namespace,CAST($3 AS text) AS code"
	documents := "SELECT x.i,x.v,x.m,x.r FROM request q CROSS JOIN LATERAL jsonb_to_recordset(q.payload->'d') AS x(i integer,v text,m text,r text)"
	resources := "SELECT x.i,x.s,x.k FROM request q CROSS JOIN LATERAL jsonb_to_recordset(q.payload->'t') AS x(i integer,s text,k text)"
	if tx.my != nil {
		request = "SELECT CAST(? AS JSON) AS payload,CAST(? AS CHAR CHARACTER SET utf8mb4) AS namespace,CAST(? AS CHAR CHARACTER SET utf8mb4) AS code"
		documents = "SELECT x.i,x.v,x.m,x.r FROM request q JOIN JSON_TABLE(q.payload,'$.d[*]' COLUMNS(i INT PATH '$.i' ERROR ON EMPTY ERROR ON ERROR,v VARCHAR(128) PATH '$.v' ERROR ON EMPTY ERROR ON ERROR,m VARCHAR(128) PATH '$.m' ERROR ON EMPTY ERROR ON ERROR,r VARCHAR(128) PATH '$.r' ERROR ON EMPTY ERROR ON ERROR)) AS x ON 1=1"
		resources = "SELECT x.i,x.s,x.k FROM request q JOIN JSON_TABLE(q.payload,'$.t[*]' COLUMNS(i INT PATH '$.i' ERROR ON EMPTY ERROR ON ERROR,s VARCHAR(64) PATH '$.s' ERROR ON EMPTY ERROR ON ERROR,k VARCHAR(1024) PATH '$.k' ERROR ON EMPTY ERROR ON ERROR)) AS x ON 1=1"
	}
	nullID, nullText, nullBytes := "CAST(NULL AS BIGINT)", "CAST(NULL AS TEXT)", "CAST(NULL AS BYTEA)"
	if tx.my != nil {
		nullID, nullText, nullBytes = "CAST(NULL AS SIGNED)", "CAST(NULL AS CHAR CHARACTER SET utf8mb4)", "CAST(NULL AS BINARY)"
	}
	owner := "SELECT d.id FROM request q LEFT JOIN " + tx.table("workflow_definition") + " d ON d.namespace=q.namespace AND d.code=q.code"
	english := "SELECT id,enabled FROM " + tx.table("i18n_locale") + " WHERE code='en'"
	root := "SELECT 'definition' AS kind,-1 AS ordinal,o.id AS definition_id," + nullID + " AS version_id," + nullID + " AS mode_id," + nullID + " AS document_id," + nullText + " AS document_format," + nullText + " AS profile," + nullBytes + " AS digest," + nullID + " AS resource_id," + nullText + " AS note,en.id AS locale_id,en.enabled," + nullText + " AS english," + nullID + " AS text_id FROM owner o LEFT JOIN english_locale en ON 1=1"
	doc := "SELECT 'document',x.i,o.id,v.id,m.id,CASE WHEN x.m='' THEN v.id ELSE r.id END,CASE WHEN x.m='' THEN v.spec_format ELSE r.config_format END,CASE WHEN x.m='' THEN v.fingerprint_profile ELSE r.fingerprint_profile END,CASE WHEN x.m='' THEN v.spec_sha256 ELSE r.config_sha256 END,NULL,NULL,en.id,en.enabled,NULL,NULL FROM requested_documents x CROSS JOIN owner o" +
		" LEFT JOIN " + tx.table("workflow_version") + " v ON v.workflow_definition_id=o.id AND v.version_code=x.v" +
		" LEFT JOIN " + tx.table("workflow_mode") + " m ON x.m<>'' AND m.workflow_version_id=v.id AND m.code=x.m" +
		" LEFT JOIN " + tx.table("workflow_mode_version") + " r ON r.workflow_mode_id=m.id AND r.version_code=x.r LEFT JOIN english_locale en ON 1=1"
	text := "SELECT 'resource',x.i,o.id,NULL,NULL,NULL,NULL,NULL,NULL,res.id,res.translator_note,en.id,en.enabled,t.value,t.id FROM requested_resources x CROSS JOIN owner o" +
		" LEFT JOIN " + tx.table("i18n_resource") + " res ON res.scope=x.s AND res.code=x.k LEFT JOIN english_locale en ON 1=1" +
		" LEFT JOIN " + tx.table("i18n_text") + " t ON t.resource_id=res.id AND t.locale_id=en.id"
	query := "WITH request AS (" + request + "), requested_documents AS (" + documents + "), requested_resources AS (" + resources + "), owner AS (" + owner + "), english_locale AS (" + english + ") " + root + " UNION ALL " + doc + " UNION ALL " + text + " ORDER BY 1,2 LIMIT " + strconv.Itoa(MaxComparisonRows+1)
	return query, args
}

func (tx *publicationTx) comparisonRows(rows [][][]byte, input *registration) (comparisonAnchor, error) {
	value := comparisonAnchor{documents: make([][][]byte, len(input.contents)), resources: make([][][]byte, len(input.resources))}
	if len(rows) != 1+len(value.documents)+len(value.resources) {
		return value, readError(ErrStored, "comparison_shape")
	}
	rootSeen := false
	for _, row := range rows {
		if len(row) != 15 {
			return value, readError(ErrStored, "comparison_shape")
		}
		if string(row[0]) == "definition" {
			if rootSeen || string(row[1]) != "-1" {
				return value, readError(ErrStored, "comparison_shape")
			}
			rootSeen = true
			id, err := readID(row[2], true)
			if err != nil {
				return value, err
			}
			value.definitionID = id
			continue
		}
		index, err := strconv.Atoi(string(row[1]))
		if err != nil || index < 0 || strconv.Itoa(index) != string(row[1]) {
			return value, readError(ErrStored, "comparison_index")
		}
		var target [][][]byte
		switch string(row[0]) {
		case "document":
			target = value.documents
		case "resource":
			target = value.resources
		default:
			return value, readError(ErrStored, "comparison_shape")
		}
		if index >= len(target) || target[index] != nil {
			return value, readError(ErrStored, "comparison_shape")
		}
		target[index] = row
	}
	if !rootSeen {
		return value, readError(ErrStored, "comparison_shape")
	}
	for _, row := range rows {
		id, err := readID(row[2], true)
		if err != nil || id != value.definitionID {
			return value, readError(ErrStored, "comparison_owner", err)
		}
		if _, err := readID(row[11], false); err != nil {
			return value, err
		}
		active, err := enabled(row[12])
		if err != nil || !active {
			return value, readError(ErrStored, "english_locale", err)
		}
	}
	return value, nil
}

func (tx *publicationTx) compareAnchored(ctx context.Context, input *registration, anchor comparisonAnchor) (Comparison, error) {
	result := Comparison{Namespace: input.info.Namespace, Code: input.info.Code, DefinitionExists: anchor.definitionID != "", DefinitionID: anchor.definitionID, Contents: len(input.contents), Resources: len(input.resources), Problems: []ComparisonProblem{}}
	if !result.DefinitionExists {
		result.Missing = result.Contents + result.Resources
		result.Problems = append(result.Problems, ComparisonProblem{Kind: "definition_missing", Path: "/definition"})
		result.Complete = true
		return result, nil
	}
	stored := []declaration.Content{}
	positions := []int{}
	total := 0
	for index, row := range anchor.documents {
		if ctx.Err() != nil {
			return Comparison{}, readError(ErrCanceled, "context", ctx.Err(), context.Cause(ctx))
		}
		path := "/contents/" + strconv.Itoa(index)
		identity := input.contents[index].Entry.Identity
		if row[5] == nil {
			result.Missing++
			result.addComparisonProblem(ComparisonProblem{"content_missing", path, &identity})
			continue
		}
		id, err := readID(row[5], false)
		if err != nil {
			return Comparison{}, err
		}
		parentIndex, table, parentKey, label := 2, "workflow_version", "workflow_definition_id", identity.Version
		if identity.Mode != "" {
			parentIndex, table, parentKey, label = 4, "workflow_mode_version", "workflow_mode_id", identity.Revision
		}
		parent, err := readID(row[parentIndex], false)
		if err != nil {
			return Comparison{}, err
		}
		format, err := readString(row[6], 64, false)
		if err != nil {
			return Comparison{}, err
		}
		profile, err := readString(row[7], 64, false)
		if err != nil {
			return Comparison{}, err
		}
		hash, err := tx.binary(row[8], 32)
		if err != nil || len(hash) != 32 {
			return Comparison{}, readError(ErrStored, "document_digest", err)
		}
		reference := bodyReference{table: table, parentKey: parentKey, id: id, parentID: parent, label: label, format: format, profile: profile, sha: hex.EncodeToString(hash), identity: identity}
		content, err := tx.readBody(ctx, reference)
		if err != nil {
			return Comparison{}, err
		}
		if len(content.Bytes) > declaration.MaxBundleBytes-total {
			return Comparison{}, readError(ErrLimit, "comparison_documents")
		}
		total += len(content.Bytes)
		stored = append(stored, content)
		positions = append(positions, index)
	}
	if len(stored) > 0 {
		if _, err := declaration.ParseContents(ctx, stored); err != nil {
			if errors.Is(err, declaration.ErrLimit) {
				return Comparison{}, readError(ErrLimit, "comparison_documents", err)
			}
			reason := "document_integrity"
			if errors.Is(err, declaration.ErrUnsupported) {
				reason = "document_unsupported"
			}
			return Comparison{}, readError(ErrStored, reason, err)
		}
	}
	for index, content := range stored {
		position := positions[index]
		local := input.contents[position]
		if content.Entry.Profile == local.Entry.Profile && content.Entry.SHA256 == local.Entry.SHA256 && bytes.Equal(content.Bytes, local.Bytes) {
			result.Matched++
		} else {
			result.Different++
			identity := local.Entry.Identity
			result.addComparisonProblem(ComparisonProblem{"immutable_content", "/contents/" + strconv.Itoa(position), &identity})
		}
	}
	metadataBytes := 0
	for index, row := range anchor.resources {
		if ctx.Err() != nil {
			return Comparison{}, readError(ErrCanceled, "context", ctx.Err(), context.Cause(ctx))
		}
		path := "/resources/" + strconv.Itoa(index)
		local := input.resources[index]
		if row[9] == nil {
			result.Missing++
			result.addComparisonProblem(ComparisonProblem{Kind: "resource_missing", Path: path})
			continue
		}
		if _, err := readID(row[9], false); err != nil {
			return Comparison{}, err
		}
		if row[13] == nil || row[14] == nil {
			return Comparison{}, readError(ErrStored, "missing_english")
		}
		if _, err := readID(row[14], false); err != nil {
			return Comparison{}, err
		}
		text := Text{English: string(row[13]), Context: string(row[10])}
		size := len(text.English) + len(text.Context)
		if size > MaxMetadataBytes-metadataBytes {
			return Comparison{}, readError(ErrLimit, "comparison_metadata")
		}
		metadataBytes += size
		if err := admitText(local.field, text, path); err != nil {
			return Comparison{}, readError(ErrStored, "resource_integrity", err)
		}
		reason := ""
		if text.Context != local.text.Context {
			reason = "translator_context"
		} else if text.English != local.text.English {
			reason = "english_text"
		}
		if reason == "" {
			result.Matched++
		} else {
			result.Different++
			result.addComparisonProblem(ComparisonProblem{Kind: reason, Path: path})
		}
	}
	result.Complete = true
	result.Equal = result.Missing == 0 && result.Different == 0
	if err := viewBound(result); err != nil {
		return Comparison{}, err
	}
	return result, nil
}
func (result *Comparison) addComparisonProblem(value ComparisonProblem) {
	if len(result.Problems) == MaxComparisonProblems {
		result.Truncated = true
		return
	}
	result.Problems = append(result.Problems, value)
}
