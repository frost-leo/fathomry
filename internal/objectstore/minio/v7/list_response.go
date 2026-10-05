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

package minio

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// MaxEntries bounds projected results. The SDK may retain an entire native page;
// its independent bound must not shrink with a smaller output chunk.
const maxNativeListEntries = 1000
const nativeListWorkBytes = maxNativeListEntries * 4096

type listingPage struct {
	fields    map[string]string
	truncated bool
}
type listingFrame struct {
	name      string
	container bool
	fields    map[string]int
	text      strings.Builder
}

// listPage validates shape before the SDK can allocate entry slices or extension
// maps. It is not an alternate decoder: the SDK still owns object data/iteration.
func (response *controlResponse) listPage(expectedRoot string) (listingPage, error) {
	page := listingPage{fields: make(map[string]string)}
	if response.statusCode != http.StatusOK {
		return page, nil
	}
	if response.readErr != io.EOF {
		return page, failure(ErrProtocol, "list-response", response.readErr)
	}
	decoder := xml.NewDecoder(bytes.NewReader(response.body.Bytes()))
	stack := make([]listingFrame, 0, 4)
	nodes, entries := 0, 0
	ended := false
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return page, failure(ErrProtocol, "list-response", err)
		}
		switch token := token.(type) {
		case xml.StartElement:
			nodes++
			if nodes > 32+64*maxNativeListEntries || len(stack) >= 4 || len(token.Attr) > 8 {
				return page, failure(ErrLimit, "list-shape")
			}
			name := token.Name.Local
			if len(stack) == 0 {
				if ended {
					return page, failure(ErrProtocol, "list-document")
				}
				if name == "Error" {
					return page, response.listFailure(expectedRoot)
				}
				if name != expectedRoot {
					return page, failure(ErrProtocol, "list-root")
				}
				stack = append(stack, listingFrame{name: name, container: true, fields: make(map[string]int)})
				continue
			}
			switch name {
			case "CommonPrefixes", "Grant", "UserMetadata", "UserTags":
				return page, failure(ErrProtocol, "list-extension")
			}
			parent := &stack[len(stack)-1]
			if !parent.container {
				return page, failure(ErrProtocol, "list-scalar")
			}
			entry := listEntry(expectedRoot, name)
			if isListEntry(name) && (!entry || len(stack) != 1) {
				return page, failure(ErrProtocol, "list-entry")
			}
			if entry {
				entries++
				if entries > maxNativeListEntries {
					return page, failure(ErrLimit, "native-list-entries")
				}
			} else {
				if _, exists := parent.fields[name]; !exists && len(parent.fields) >= 32 {
					return page, failure(ErrLimit, "list-fields")
				}
				parent.fields[name]++
				maximum := 1
				if name == "ChecksumAlgorithm" {
					maximum = 16
				}
				if parent.fields[name] > maximum {
					return page, failure(ErrProtocol, "list-duplicate-field")
				}
			}
			container := entry || name == "Owner" || name == "Initiator" || name == "Restore" || name == "RestoreStatus" || name == "Internal"
			stack = append(stack, listingFrame{name: name, container: container, fields: make(map[string]int)})
		case xml.EndElement:
			if len(stack) == 0 {
				return page, failure(ErrProtocol, "list-document")
			}
			frame := &stack[len(stack)-1]
			if listEntry(expectedRoot, frame.name) {
				required := []string{"Key", "Size"}
				switch frame.name {
				case "Version":
					required = append(required, "VersionId")
				case "DeleteMarker":
					required = []string{"Key", "VersionId"}
				case "Upload":
					required = []string{"Key", "UploadId"}
				case "Part":
					required = []string{"PartNumber", "Size"}
				}
				for _, field := range required {
					if frame.fields[field] != 1 {
						return page, failure(ErrProtocol, "list-entry-field")
					}
				}
			}
			if len(stack) == 2 && !frame.container {
				page.fields[frame.name] = frame.text.String()
			}
			stack = stack[:len(stack)-1]
			if len(stack) == 0 {
				ended = true
			}
		case xml.CharData:
			if len(stack) == 0 || stack[len(stack)-1].container {
				if len(bytes.TrimSpace(token)) != 0 {
					return page, failure(ErrProtocol, "list-text")
				}
			} else if len(stack) == 2 {
				frame := &stack[len(stack)-1]
				if frame.text.Len()+len(token) > 8192 {
					return page, failure(ErrLimit, "list-control-field")
				}
				_, _ = frame.text.Write(token)
			}
		case xml.Comment:
		case xml.ProcInst:
			if len(stack) != 0 || ended {
				return page, failure(ErrProtocol, "list-document")
			}
		default:
			return page, failure(ErrProtocol, "list-document")
		}
	}
	if !ended || len(stack) != 0 {
		return page, failure(ErrProtocol, "list-document")
	}
	terminal, present := page.fields["IsTruncated"]
	if !present {
		return page, failure(ErrProtocol, "list-terminal")
	}
	terminal = strings.TrimSpace(terminal)
	switch terminal {
	case "true", "false", "1", "0":
	default:
		return page, failure(ErrProtocol, "list-terminal")
	}
	truncated, _ := strconv.ParseBool(terminal)
	page.truncated = truncated
	identity := []string{"Name"}
	switch expectedRoot {
	case "ListMultipartUploadsResult":
		identity = []string{"Bucket"}
	case "ListPartsResult":
		identity = []string{"Bucket", "Key", "UploadId"}
	}
	for _, field := range identity {
		if _, present := page.fields[field]; !present {
			return page, failure(ErrProtocol, "list-identity")
		}
	}
	return page, nil
}

func isListEntry(name string) bool {
	switch name {
	case "Contents", "Version", "DeleteMarker", "Upload", "Part":
		return true
	}
	return false
}
func listEntry(root, name string) bool {
	switch root {
	case "ListBucketResult":
		return name == "Contents"
	case "ListVersionsResult":
		return name == "Version" || name == "DeleteMarker"
	case "ListMultipartUploadsResult":
		return name == "Upload"
	case "ListPartsResult":
		return name == "Part"
	}
	return false
}

type objectListing struct {
	bucket  string
	request ListRequest
	seen    map[string]bool
}

func (state *objectListing) checkPage(response *controlResponse) error {
	root := "ListBucketResult"
	if state.request.Versions {
		root = "ListVersionsResult"
	}
	page, err := response.listPage(root)
	if err != nil || response.statusCode != http.StatusOK {
		return err
	}
	if page.fields["Name"] != state.bucket {
		return failure(ErrProtocol, "list-bucket")
	}
	if !page.truncated {
		return nil
	}
	var marker string
	if state.request.Versions {
		key, err := decodeKey(page.fields["NextKeyMarker"], page.fields["EncodingType"])
		if err != nil || !validPath(key, false) || !strings.HasPrefix(key, state.request.Prefix) || !validText(page.fields["NextVersionIdMarker"], 1024, false) {
			return failure(ErrProtocol, "version-cursor", err)
		}
		marker = key + "\x00" + page.fields["NextVersionIdMarker"]
	} else {
		marker = page.fields["NextContinuationToken"]
		if !validText(marker, 4096, false) {
			return failure(ErrProtocol, "object-cursor")
		}
	}
	if state.seen[marker] {
		return failure(ErrProtocol, "cursor-no-progress")
	}
	state.seen[marker] = true
	return nil
}
