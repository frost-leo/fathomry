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

package nacos

import (
	"context"
	native "github.com/frost-leo/fathomry/internal/configsource/nacos/v2"
)

// SearchInput selects one namespace-local native page. DynamicKeys is required;
// Mode is accurate/blur. Page defaults to 1 (1..1000000), PageSize to 10 (1..100).
// Filters remain native strings; they are not SQL or a client-side glob.
type SearchInput struct {
	private
	Mode       string
	DataID     string
	Group      string
	ConfigTags string
	AppName    string
	Page       int
	PageSize   int
}

// SearchPage is an immutable bounded result, not transactional pagination.
type SearchPage struct {
	private
	native *native.SearchPage
}

// SearchItem preserves content presence separately from its native metadata.
type SearchItem struct {
	private
	native native.SearchItem
}

// Search preserves the native v1 route and v3 fallback only for an unsupported
// endpoint, never for authentication refusal. Metadata-only is not empty content.
func (client *Client) Search(ctx context.Context, input SearchInput) (*SearchPage, error) {
	var result *SearchPage
	err := client.run(ctx, "search", func(ctx context.Context, selected *native.Client) (Evidence, error) {
		page, err := selected.Search(ctx, native.SearchInputV1{Mode: input.Mode, DataID: input.DataID, Group: input.Group, ConfigTags: input.ConfigTags, AppName: input.AppName, Page: input.Page, PageSize: input.PageSize})
		evidence := Evidence{FailedIndex: -1}
		if err == nil {
			result = &SearchPage{native: page}
			evidence.Documents = len(page.ItemsCopy())
		}
		return evidence, err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
func (page *SearchPage) Total() int {
	if page == nil {
		return 0
	}
	return page.native.Total()
}
func (page *SearchPage) Number() int {
	if page == nil {
		return 0
	}
	return page.native.Number()
}
func (page *SearchPage) Pages() int {
	if page == nil {
		return 0
	}
	return page.native.Pages()
}

// ItemsCopy detaches the container; each item retains immutable native content.
func (page *SearchPage) ItemsCopy() []SearchItem {
	if page == nil {
		return nil
	}
	values := page.native.ItemsCopy()
	result := make([]SearchItem, len(values))
	for index, value := range values {
		result[index] = SearchItem{native: value}
	}
	return result
}
func (value SearchItem) ID() string           { return value.native.ID() }
func (value SearchItem) Key() Key             { return publicKey(value.native.Key()) }
func (value SearchItem) Namespace() string    { return value.native.Namespace() }
func (value SearchItem) ContentPresent() bool { return value.native.ContentPresent() }
func (value SearchItem) RawCopy() []byte      { return value.native.RawCopy() }
func (value SearchItem) MD5() string          { return value.native.MD5() }
func (value SearchItem) AppName() string      { return value.native.AppName() }
