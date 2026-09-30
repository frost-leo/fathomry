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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/invocation"
)

// SearchInputV1 selects one bounded namespace-local native page. Mode is accurate
// or blur; Page defaults to 1 and PageSize to 10 (maximum 100). Filters are native
// strings, not SQL or a client-side glob. DynamicKeys must be explicitly enabled.
// ConfigTags uses config_tags on v1 and configTags on v3; empty omits that filter.
type SearchInputV1 struct {
	private
	Mode       string
	DataID     string
	Group      string
	ConfigTags string
	AppName    string
	Page       int
	PageSize   int
}

// SearchItem is an owned sensitive native search result, not a fresh Read proof.
type SearchItem struct {
	private
	id                               string
	selected                         key
	namespace, content, md5, appName string
	contentPresent                   bool
}

func (item SearchItem) ID() string { return item.id }
func (item SearchItem) Key() KeyV1 {
	return KeyV1{Group: item.selected.Group, DataID: item.selected.DataID}
}
func (item SearchItem) Namespace() string { return item.namespace }

// ContentPresent distinguishes a content-bearing v1 result from metadata-only
// v3 listings. Missing content is not a successful empty configuration.
func (item SearchItem) ContentPresent() bool { return item.contentPresent }
func (item SearchItem) RawCopy() []byte {
	if !item.contentPresent {
		return nil
	}
	return []byte(item.content)
}
func (item SearchItem) MD5() string     { return item.md5 }
func (item SearchItem) AppName() string { return item.appName }

// SearchPage owns one returned page. Pagination does not establish a transactional
// snapshot across requests. ItemsCopy returns independent container storage.
type SearchPage struct {
	private
	total, page, pages int
	items              []SearchItem
}

func (page *SearchPage) Total() int {
	if page == nil {
		return 0
	}
	return page.total
}
func (page *SearchPage) Number() int {
	if page == nil {
		return 0
	}
	return page.page
}
func (page *SearchPage) Pages() int {
	if page == nil {
		return 0
	}
	return page.pages
}
func (page *SearchPage) ItemsCopy() []SearchItem {
	if page == nil {
		return nil
	}
	return append([]SearchItem(nil), page.items...)
}

// Search executes native v1 configuration search, with v3 admin-list fallback only
// when the endpoint is absent/unsupported. No auth failure is masked by fallback.
func (client *Client) Search(ctx context.Context, input SearchInputV1) (*SearchPage, error) {
	if client == nil || client.cancel == nil || !client.settings.Dynamic || input.Mode != "accurate" && input.Mode != "blur" {
		return nil, fail(ErrInput, "search")
	}
	if input.Page == 0 {
		input.Page = 1
	}
	if input.PageSize == 0 {
		input.PageSize = 10
	}
	if input.Page < 1 || input.Page > 1000000 || input.PageSize < 1 || input.PageSize > 100 {
		return nil, fail(ErrInput, "search")
	}
	for _, value := range []string{input.DataID, input.Group, input.ConfigTags, input.AppName} {
		if !identifier(value, 128, true) {
			return nil, fail(ErrInput, "search")
		}
	}
	work, end, err := client.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer end()
	budget, stop, err := (invocation.Budget{Limit: client.settings.Timeout}).Context(work, invocation.Execute)
	if err != nil {
		return nil, err
	}
	defer stop()
	lease, err := client.access.Acquire(budget, reservationBytes)
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	var causes []error
	start := int(client.preferred.Load() % uint64(len(client.settings.Servers)))
	for offset := range client.settings.Servers {
		index := (start + offset) % len(client.settings.Servers)
		page, err := client.searchPage(budget, index, input)
		if err == nil {
			client.preferred.Store(uint64(index))
			return page, nil
		}
		causes = append(causes, err)
		if !errors.Is(err, ErrUnavailable) || budget.Err() != nil {
			return nil, err
		}
	}
	return nil, fail(ErrRead, "search", causes...)
}
func (client *Client) searchPage(ctx context.Context, index int, input SearchInputV1) (*SearchPage, error) {
	token, err := client.token(ctx, index)
	if err != nil {
		return nil, err
	}
	values := url.Values{"search": {input.Mode}, "dataId": {input.DataID}, "group": {input.Group}, "config_tags": {input.ConfigTags}, "appName": {input.AppName},
		"pageNo": {strconv.Itoa(input.Page)}, "pageSize": {strconv.Itoa(input.PageSize)}}
	if client.settings.Namespace != "" {
		values.Set("tenant", client.settings.Namespace)
	}
	if token != "" {
		values.Set("accessToken", token)
	}
	raw, status, err := client.searchRequest(ctx, index, "/v1/cs/configs", values)
	if err != nil {
		return nil, err
	}
	version3 := status == http.StatusNotFound || status == http.StatusMethodNotAllowed || status == http.StatusNotImplemented
	if version3 {
		values.Set("namespaceId", client.settings.Namespace)
		values.Set("groupName", input.Group)
		values.Set("configTags", input.ConfigTags)
		raw, status, err = client.searchRequest(ctx, index, "/v3/admin/cs/config/list", values)
		if err != nil {
			return nil, err
		}
	}
	if status == 401 || status == 403 {
		client.forgetToken(index, token)
		return nil, fail(ErrDenied, "search", HTTPStatus(status))
	}
	if status != 200 {
		return nil, fail(ErrUnavailable, "search", HTTPStatus(status))
	}
	page, err := decodeSearchPage(raw, input, client.settings.Namespace, version3)
	if errors.Is(err, ErrDenied) {
		client.forgetToken(index, token)
	}
	if ctx.Err() != nil {
		return nil, fail(ErrRead, "search", err, ctx.Err(), context.Cause(ctx))
	}
	return page, err
}

func decodeSearchPage(raw []byte, input SearchInputV1, namespace string, version3 bool) (*SearchPage, error) {
	if err := validateJSON(raw); err != nil {
		return nil, fail(ErrDecode, "search", err)
	}
	pageRaw := raw
	if version3 {
		var result struct {
			Code    *int            `json:"code"`
			Message string          `json:"message"`
			Data    json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(raw, &result); err != nil {
			return nil, fail(ErrDecode, "search", err)
		}
		if result.Code == nil {
			return nil, fail(ErrDecode, "search-code")
		}
		if *result.Code != 0 && *result.Code != 200 {
			identity := ErrUnavailable
			if *result.Code == 401 || *result.Code == 403 {
				identity = ErrDenied
			}
			return nil, fail(identity, "search", &RemoteError{resultCode: 200, errorCode: *result.Code, message: strings.Clone(result.Message)})
		}
		pageRaw = result.Data
	}
	var fields struct {
		Total *int              `json:"totalCount"`
		Page  *int              `json:"pageNumber"`
		Pages *int              `json:"pagesAvailable"`
		Items []json.RawMessage `json:"pageItems"`
	}
	if err := json.Unmarshal(pageRaw, &fields); err != nil {
		return nil, fail(ErrDecode, "search", err)
	}
	if fields.Total == nil || fields.Page == nil || fields.Pages == nil || *fields.Total < 0 || *fields.Page != input.Page || *fields.Pages < 0 || len(fields.Items) > input.PageSize || *fields.Total < len(fields.Items) {
		return nil, fail(ErrDecode, "search-page")
	}
	page := &SearchPage{total: *fields.Total, page: *fields.Page, pages: *fields.Pages}
	bytes := 0
	for _, rawItem := range fields.Items {
		var item struct {
			ID        json.Number `json:"id"`
			DataID    string      `json:"dataId"`
			Group     string      `json:"group"`
			GroupName string      `json:"groupName"`
			Tenant    string      `json:"tenant"`
			Namespace string      `json:"namespaceId"`
			Content   *string     `json:"content"`
			MD5       string      `json:"md5"`
			AppName   string      `json:"appName"`
		}
		if err := json.Unmarshal(rawItem, &item); err != nil {
			return nil, fail(ErrDecode, "search-item", err)
		}
		if version3 {
			item.Group, item.Tenant = item.GroupName, item.Namespace
		}
		content := ""
		if item.Content != nil {
			content = *item.Content
		}
		if !identifier(item.DataID, 128, false) || !identifier(item.Group, 128, false) || item.Tenant != namespace || !utf8.ValidString(content) || len(content) > MaxDocumentBytes || len(item.AppName) > 128 {
			return nil, fail(ErrDecode, "search-item")
		}
		if input.Mode == "accurate" && (input.DataID != "" && item.DataID != input.DataID || input.Group != "" && item.Group != input.Group) {
			return nil, fail(ErrDecode, "search-scope")
		}
		if item.MD5 != "" {
			digest, err := hex.DecodeString(item.MD5)
			if err != nil || len(digest) != 16 || item.Content != nil && !strings.EqualFold(item.MD5, checksum(content)) {
				return nil, fail(ErrDecode, "search-md5")
			}
		}
		bytes += len(content)
		if bytes > MaxTotalBytes {
			return nil, fail(ErrLimit, "search-page")
		}
		page.items = append(page.items, SearchItem{id: strings.Clone(item.ID.String()), selected: key{strings.Clone(item.Group), strings.Clone(item.DataID)}, namespace: strings.Clone(item.Tenant), content: strings.Clone(content), contentPresent: item.Content != nil, md5: strings.Clone(item.MD5), appName: strings.Clone(item.AppName)})
	}
	return page, nil
}
func (client *Client) searchRequest(ctx context.Context, index int, path string, values url.Values) ([]byte, int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, client.settings.Servers[index].HTTPURL+path+"?"+values.Encode(), nil)
	if err != nil {
		return nil, 0, fail(ErrInput, "search", err)
	}
	response, err := client.http.Do(request)
	if err != nil {
		return nil, 0, fail(ErrUnavailable, "search", err, ctx.Err(), context.Cause(ctx))
	}
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, MaxWireBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		return nil, response.StatusCode, fail(ErrRead, "search", readErr, closeErr)
	}
	if len(raw) > MaxWireBytes {
		return nil, response.StatusCode, fail(ErrLimit, "search")
	}
	return raw, response.StatusCode, nil
}
func (*SearchInputV1) Format(state fmt.State, _ rune) { restricted(state) }
func (*SearchInputV1) LogValue() slog.Value           { return slog.StringValue("nacos[restricted]") }
func (*SearchItem) Format(state fmt.State, _ rune)    { restricted(state) }
func (*SearchItem) LogValue() slog.Value              { return slog.StringValue("nacos[restricted]") }
func (*SearchPage) Format(state fmt.State, _ rune)    { restricted(state) }
func (*SearchPage) LogValue() slog.Value              { return slog.StringValue("nacos[restricted]") }
