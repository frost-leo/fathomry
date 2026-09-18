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

package lark

import (
	"context"
	"encoding/json"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	larkcard "github.com/larksuite/oapi-sdk-go/v3/service/cardkit/v1"
)

func elementIDValid(id string) bool {
	if len(id) < 1 || len(id) > 20 {
		return false
	}
	for index, char := range id {
		letter := char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z'
		if !letter && (index == 0 || char != '_' && (char < '0' || char > '9')) {
			return false
		}
	}
	return true
}
func elementPath(cardID, elementID string) (string, error) {
	path, err := cardPath(cardID)
	if err != nil {
		return "", err
	}
	if !elementIDValid(elementID) {
		return "", failure(ErrInput, "element-id")
	}
	return path + "/elements/" + elementID, nil
}

// Chart creates a native VChart component, preserving the specification's exact
// JSON numbers, data, labels, units and declarative interactions. No image
// fallback, remote data fetching or JavaScript execution occurs here.
func Chart(elementID string, spec JSON) (JSON, error) {
	if !elementIDValid(elementID) || !object(spec) {
		return JSON{}, failure(ErrInput, "chart")
	}
	if _, err := exactFields(spec.Bytes(), "type"); err != nil {
		return JSON{}, failure(ErrInput, "chart-type", err)
	}
	var kind struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(spec.Bytes(), &kind) != nil || !identifier(kind.Type) {
		return JSON{}, failure(ErrInput, "chart-type")
	}
	data, err := json.Marshal(map[string]any{"tag": "chart", "element_id": elementID, "chart_spec": json.RawMessage(spec.value)})
	if err != nil {
		return JSON{}, failure(ErrInput, "chart", err)
	}
	return NewJSON(data)
}

// Table constructs a native table, not a Markdown or HTML substitute. Columns and
// rows use the native JSON schema and may contain text, numbers or supported
// formatted cells. For optional styles/pagination pass a complete JSON component.
// Feishu requires tables at the card body's root, not inside other components.
func Table(elementID string, columns, rows JSON) (JSON, error) {
	if !elementIDValid(elementID) || len(columns.value) == 0 || columns.value[0] != '[' || len(rows.value) == 0 || rows.value[0] != '[' {
		return JSON{}, failure(ErrInput, "table")
	}
	data, err := json.Marshal(map[string]any{"tag": "table", "element_id": elementID, "columns": json.RawMessage(columns.value), "rows": json.RawMessage(rows.value)})
	if err != nil {
		return JSON{}, failure(ErrInput, "table", err)
	}
	return NewJSON(data)
}

// ComposeCard combines up to 200 native components in Card JSON 2.0. Use Card
// directly for native layouts, localized bodies, forms or other advanced settings.
func ComposeCard(title string, elements ...JSON) (Content, error) {
	if !shortText(title, 1024) || len(elements) == 0 || len(elements) > 200 {
		return Content{}, failure(ErrInput, "card")
	}
	total := len(title)
	parts := make([]json.RawMessage, len(elements))
	for index, element := range elements {
		total += len(element.value)
		if !object(element) || total > maxContentBytes {
			return Content{}, failure(ErrLimit, "elements")
		}
		parts[index] = json.RawMessage(element.value)
	}
	data, err := json.Marshal(map[string]any{"schema": "2.0", "config": map[string]bool{"update_multi": true},
		"header": map[string]any{"template": "blue", "title": map[string]string{"tag": "plain_text", "content": title}},
		"body":   map[string]any{"elements": parts}})
	if err != nil {
		return Content{}, failure(ErrInput, "card", err)
	}
	return Card(data)
}

// InsertElements inserts a native component array before/after a target, or
// appends to a container/body. Empty target is allowed only for append.
func (client *Client) InsertElements(ctx context.Context, id fault.Correlation, cardID string, revision Revision, position, target string, elements JSON) (*invocation.Receipt[Result], error) {
	path, err := cardPath(cardID)
	if err != nil {
		return nil, err
	}
	if !revision.valid() || len(elements.value) == 0 || elements.value[0] != '[' ||
		(position != "append" && position != "insert_before" && position != "insert_after") ||
		(target == "" && position != "append") || (target != "" && !elementIDValid(target)) {
		return nil, failure(ErrInput, "insert-elements")
	}
	return client.execute(ctx, id, requestSpec{operation: "insert-elements", method: "POST", path: path + "/elements", target: cardID, uuid: revision.UUID,
		body: &larkcard.CreateCardElementReqBody{Type: pointer(position), TargetElementId: optional(target), Elements: pointer(elements.value),
			Sequence: pointer(revision.Sequence), Uuid: optional(revision.UUID)}, bodyBytes: len(elements.value)})
}

// UpdateElement replaces one complete native element, including charts/tables.
func (client *Client) UpdateElement(ctx context.Context, id fault.Correlation, cardID, elementID string, revision Revision, element JSON) (*invocation.Receipt[Result], error) {
	path, err := elementPath(cardID, elementID)
	if err != nil {
		return nil, err
	}
	if !revision.valid() || !object(element) {
		return nil, failure(ErrInput, "update-element")
	}
	return client.execute(ctx, id, requestSpec{operation: "update-element", method: "PUT", path: path, target: cardID, uuid: revision.UUID,
		body: &larkcard.UpdateCardElementReqBody{Element: pointer(element.value), Sequence: pointer(revision.Sequence), Uuid: optional(revision.UUID)}, bodyBytes: len(element.value)})
}

// PatchElement updates native element fields (for example chart_spec or rows).
// The platform forbids changing tag through this partial-update endpoint.
func (client *Client) PatchElement(ctx context.Context, id fault.Correlation, cardID, elementID string, revision Revision, partial JSON) (*invocation.Receipt[Result], error) {
	path, err := elementPath(cardID, elementID)
	if err != nil {
		return nil, err
	}
	if !revision.valid() || !object(partial) {
		return nil, failure(ErrInput, "patch-element")
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(partial.Bytes(), &fields)
	if _, exists := fields["tag"]; exists {
		return nil, failure(ErrInput, "patch-tag")
	}
	return client.execute(ctx, id, requestSpec{operation: "patch-element", method: "PATCH", path: path, target: cardID, uuid: revision.UUID,
		body: &larkcard.PatchCardElementReqBody{PartialElement: pointer(partial.value), Sequence: pointer(revision.Sequence), Uuid: optional(revision.UUID)}, bodyBytes: len(partial.value)})
}

// DeleteElement removes an element and, for containers, its nested children.
func (client *Client) DeleteElement(ctx context.Context, id fault.Correlation, cardID, elementID string, revision Revision) (*invocation.Receipt[Result], error) {
	path, err := elementPath(cardID, elementID)
	if err != nil {
		return nil, err
	}
	if !revision.valid() {
		return nil, failure(ErrInput, "delete-element")
	}
	return client.execute(ctx, id, requestSpec{operation: "delete-element", method: "DELETE", path: path, target: cardID, uuid: revision.UUID,
		body: &larkcard.DeleteCardElementReqBody{Sequence: pointer(revision.Sequence), Uuid: optional(revision.UUID)}})
}

// ElementContent performs one finite text-stream update with the full new text.
// The caller owns pacing, sequence and streaming-mode completion; no runtime
// stream, timer or retry loop is hidden behind this method.
func (client *Client) ElementContent(ctx context.Context, id fault.Correlation, cardID, elementID string, revision Revision, content string) (*invocation.Receipt[Result], error) {
	path, err := elementPath(cardID, elementID)
	if err != nil {
		return nil, err
	}
	if !revision.valid() || !shortText(content, maxContentBytes) {
		return nil, failure(ErrInput, "element-content")
	}
	return client.execute(ctx, id, requestSpec{operation: "element-content", method: "PUT", path: path + "/content", target: cardID, uuid: revision.UUID,
		body: &larkcard.ContentCardElementReqBody{Content: pointer(content), Sequence: pointer(revision.Sequence), Uuid: optional(revision.UUID)}, bodyBytes: len(content)})
}
