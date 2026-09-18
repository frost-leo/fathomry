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
	"bytes"
	_ "embed"
	"encoding/csv"
	"encoding/json"
	"errors"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"strconv"

	lark "github.com/frost-leo/fathomry/internal/notification/lark/v3"
)

//go:embed resources/catalog.json
var catalogBytes []byte

type catalog struct {
	Card        json.RawMessage `json:"card"`
	Post        json.RawMessage `json:"post"`
	Interaction json.RawMessage `json:"interaction_card"`
	Variables   json.RawMessage `json:"template_variables"`
	Data        []struct {
		Period string `json:"period"`
		Items  int    `json:"items"`
	} `json:"data"`
}

func report(imageKey string) (lark.Content, lark.Content, []byte, []byte, error) {
	var source catalog
	if json.Unmarshal(catalogBytes, &source) != nil || len(source.Data) != 4 {
		return lark.Content{}, lark.Content{}, nil, nil, errors.New("invalid report fixture")
	}
	var document map[string]json.RawMessage
	if json.Unmarshal(source.Card, &document) != nil {
		return lark.Content{}, lark.Content{}, nil, nil, errors.New("invalid card fixture")
	}
	if imageKey != "" {
		var body struct {
			Elements []json.RawMessage `json:"elements"`
		}
		_ = json.Unmarshal(document["body"], &body)
		element, _ := json.Marshal(map[string]any{"tag": "img", "img_key": imageKey, "alt": map[string]string{"tag": "plain_text", "content": "Explicit PNG mode: Jan 80, Feb 120, Mar 100, Apr 160 items."}})
		body.Elements = append(body.Elements, element)
		document["body"], _ = json.Marshal(body)
	}
	encoded, _ := json.Marshal(document)
	card, err := lark.Card(encoded)
	if err != nil {
		return lark.Content{}, lark.Content{}, nil, nil, err
	}
	post, err := lark.NewContent("post", source.Post)
	if err != nil {
		return lark.Content{}, lark.Content{}, nil, nil, err
	}
	var table bytes.Buffer
	writer := csv.NewWriter(&table)
	_ = writer.Write([]string{"period", "items"})
	for _, row := range source.Data {
		_ = writer.Write([]string{row.Period, strconv.Itoa(row.Items)})
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return lark.Content{}, lark.Content{}, nil, nil, err
	}
	// Deliberately static PNG alternative. Native charts above are not replaced.
	canvas := image.NewRGBA(image.Rect(0, 0, 640, 300))
	draw.Draw(canvas, canvas.Bounds(), &image.Uniform{color.RGBA{250, 252, 255, 255}}, image.Point{}, draw.Src)
	ink := color.RGBA{34, 65, 110, 255}
	blue := color.RGBA{58, 119, 225, 255}
	draw.Draw(canvas, image.Rect(48, 35, 50, 250), &image.Uniform{ink}, image.Point{}, draw.Src)
	draw.Draw(canvas, image.Rect(48, 248, 610, 250), &image.Uniform{ink}, image.Point{}, draw.Src)
	for index, row := range source.Data {
		left := 85 + index*130
		top := 248 - row.Items
		draw.Draw(canvas, image.Rect(left, top, left+70, 248), &image.Uniform{blue}, image.Point{}, draw.Src)
		digits(canvas, left+12, top-24, strconv.Itoa(row.Items), ink)
		digits(canvas, left+30, 267, strconv.Itoa(index+1), ink)
	}
	var imageBytes bytes.Buffer
	if err := png.Encode(&imageBytes, canvas); err != nil {
		return lark.Content{}, lark.Content{}, nil, nil, err
	}
	return card, post, imageBytes.Bytes(), table.Bytes(), nil
}
func digits(canvas *image.RGBA, x, y int, text string, ink color.Color) {
	glyphs := []string{"111101101101111", "010110010010111", "111001111100111", "111001111001111", "101101111001001", "111100111001111", "111100111101111", "111001001001001", "111101111101111", "111101111001111"}
	for index, digit := range text {
		for pos, pixel := range glyphs[digit-'0'] {
			if pixel == '1' {
				left, top := x+index*12+(pos%3)*3, y+(pos/3)*3
				draw.Draw(canvas, image.Rect(left, top, left+3, top+3), &image.Uniform{ink}, image.Point{}, draw.Src)
			}
		}
	}
}
