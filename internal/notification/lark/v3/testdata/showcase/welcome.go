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

package main

import (
	_ "embed"
	"encoding/json"
	"errors"
	"strings"

	lark "github.com/frost-leo/fathomry/internal/notification/lark/v3"
)

//go:embed resources/welcome.json
var welcomeBytes []byte

type welcomePayload struct {
	card  lark.Content
	table []byte
}

func welcome(reportPath string, requireFresh bool) (*welcomePayload, error) {
	var source struct {
		Card json.RawMessage `json:"card"`
	}
	if json.Unmarshal(welcomeBytes, &source) != nil {
		return nil, errors.New("invalid welcome resource")
	}
	var table []byte
	if reportPath != "" {
		elements, csv, err := repositoryReport(reportPath, requireFresh)
		if err != nil {
			return nil, err
		}
		var card map[string]json.RawMessage
		if json.Unmarshal(source.Card, &card) != nil {
			return nil, errors.New("invalid welcome card")
		}
		var body map[string]json.RawMessage
		if json.Unmarshal(card["body"], &body) != nil {
			return nil, errors.New("invalid welcome body")
		}
		var original []json.RawMessage
		if json.Unmarshal(body["elements"], &original) != nil || len(original) == 0 {
			return nil, errors.New("invalid welcome elements")
		}
		insertion := -1
		for index, raw := range original {
			var element struct {
				ID string `json:"element_id"`
			}
			if json.Unmarshal(raw, &element) != nil {
				return nil, errors.New("invalid welcome element")
			}
			if element.ID == "closing" {
				if insertion != -1 {
					return nil, errors.New("ambiguous welcome report position")
				}
				insertion = index
			}
		}
		if insertion < 0 {
			return nil, errors.New("missing welcome report position")
		}
		combined := make([]json.RawMessage, 0, len(original)+len(elements))
		combined = append(combined, original[:insertion]...)
		combined = append(combined, elements...)
		combined = append(combined, original[insertion:]...)
		body["elements"], _ = json.Marshal(combined)
		card["body"], _ = json.Marshal(body)
		source.Card, _ = json.Marshal(card)
		table = csv
	}
	card, err := lark.Card(source.Card)
	if err != nil {
		return nil, err
	}
	// The message envelope has a separate platform limit, including JSON escaping.
	wire, err := json.Marshal(map[string]string{"receive_id": strings.Repeat("x", 256), "uuid": strings.Repeat("x", 36), "msg_type": "interactive", "content": string(card.JSON().Bytes())})
	if err != nil || len(wire) > 30<<10 {
		return nil, errors.New("welcome card exceeds the message envelope limit")
	}
	return &welcomePayload{card: card, table: table}, nil
}
