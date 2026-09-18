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

package i18n

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/failure"
	native "github.com/nicksnyder/go-i18n/v2/i18n"
	"golang.org/x/text/language"
)

// Profile identifies the JSON schema, restricted Go-template syntax, hashing
// rules and composition policy. It is not a module or workflow version.
const Profile = "fathomry.i18n/v1"

// Fixed preparation and rendering limits are bytes unless named otherwise.
// All files/messages, including stale translations, count toward these limits.
const (
	MaxFiles         = 64
	MaxFileBytes     = 1 << 20
	MaxResourceBytes = 8 << 20
	MaxMessages      = 2048
	MaxLocales       = 32
	MaxParameters    = 16
	MaxIDBytes       = 128
	MaxNameBytes     = 64
	MaxLocaleBytes   = 64
	MaxTemplateBytes = 8192
	MaxTemplateNodes = 128
	MaxArgumentBytes = 4096
	MaxOutputBytes   = 65536
	MaxJSONDepth     = 16
)

// Public errors expose stable machine identities, never resource/argument values
// or native parser diagnostics. Match them with errors.Is.
const (
	InvalidResource    failure.Code = "fathomry.i18n.invalid_resource"
	UnsupportedProfile failure.Code = "fathomry.i18n.unsupported_profile"
	Duplicate          failure.Code = "fathomry.i18n.duplicate"
	InvalidTemplate    failure.Code = "fathomry.i18n.invalid_template"
	LimitExceeded      failure.Code = "fathomry.i18n.limit_exceeded"
	InvalidLocale      failure.Code = "fathomry.i18n.invalid_locale"
	InvalidArguments   failure.Code = "fathomry.i18n.invalid_arguments"
	MessageNotFound    failure.Code = "fathomry.i18n.message_not_found"
	RenderFailed       failure.Code = "fathomry.i18n.render_failed"
	InvalidCatalog     failure.Code = "fathomry.i18n.invalid_catalog"
)

// Resource is one caller-owned UTF-8 JSON file. Name is a unique public relative
// slash path (at most 256 bytes), used only for provenance, never file access.
// Data is borrowed only during Prepare; do not mutate it concurrently.
type Resource struct {
	Name string
	Data []byte
}

// ResourceDigest identifies the exact bytes of one named resource, including
// whitespace and license metadata. SHA256 uses the sha256: lowercase-hex form.
type ResourceDigest struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
}

// SourceRevision binds a message to its source contract and all source variants.
type SourceRevision struct {
	ID     string
	SHA256 string
}

// StaleEntry identifies an excluded translation. No wording is returned.
type StaleEntry struct {
	ID     string
	Locale string
}

// Snapshot is an owned metadata copy. ID hashes Profile and sorted Resources,
// not the actual renderer build. Sources and Stale are sorted by ID/locale.
// A digest establishes content identity, not authenticity or semantic correctness.
type Snapshot struct {
	ID        string
	Profile   string
	Resources []ResourceDigest
	Sources   []SourceRevision
	Stale     []StaleEntry
}

// Catalog owns private prepared resources. A zero or nil Catalog cannot render.
// Prepared instances support concurrent Render and Snapshot calls; there are no
// setters. There is no Close: catalogs own no I/O handles or background work.
type Catalog struct {
	sources    map[string]*sourceContract
	localizers map[string]*native.Localizer
	entries    map[string]map[string]bool
	stale      map[string]map[string]bool
	locales    []string
	matcher    language.Matcher
	parser     preparedParser
	snapshot   Snapshot
}

// Snapshot returns independent metadata slices. Nil/zero catalogs return zero.
func (catalog *Catalog) Snapshot() Snapshot {
	if catalog == nil {
		return Snapshot{}
	}
	result := catalog.snapshot
	result.Resources = slices.Clone(result.Resources)
	result.Sources = slices.Clone(result.Sources)
	result.Stale = slices.Clone(result.Stale)
	return result
}

// Prepare atomically validates the complete composition; failure returns nil.
// It does not read files, environment or the network. Input order cannot override
// collisions or change locale tie-breaking. Source en is required for every ID;
// missing translations are allowed, stale ones are validated then excluded.
// Limits apply before copying/decoding and before templates enter native state.
// Resource authors are trusted; this is a restricted profile, not a code sandbox.
func Prepare(resources []Resource) (*Catalog, error) {
	if len(resources) == 0 {
		return nil, problem(InvalidResource)
	}
	if len(resources) > MaxFiles {
		return nil, problem(LimitExceeded)
	}
	total := 0
	names := make(map[string]bool, len(resources))
	for _, resource := range resources {
		if len(resource.Data) > MaxFileBytes || len(resource.Data) > MaxResourceBytes-total {
			return nil, problem(LimitExceeded)
		}
		total += len(resource.Data)
		if resource.Name == "." || len(resource.Name) > 256 || !fs.ValidPath(resource.Name) ||
			!utf8.ValidString(resource.Name) || strings.ContainsFunc(resource.Name, unicode.IsControl) {
			return nil, problem(InvalidResource)
		}
		if names[resource.Name] {
			return nil, problem(Duplicate)
		}
		names[resource.Name] = true
	}
	catalog := &Catalog{
		sources:    make(map[string]*sourceContract),
		localizers: make(map[string]*native.Localizer),
		entries:    make(map[string]map[string]bool),
		stale:      make(map[string]map[string]bool),
		parser:     preparedParser{templates: make(map[string]*compiledTemplate)},
		snapshot:   Snapshot{Profile: Profile},
	}
	files := make([]resourceFile, 0, len(resources))
	seen := make(map[string]map[string]bool)
	messages := 0
	for _, resource := range resources {
		file, err := decodeResource(resource.Data)
		if err != nil {
			return nil, err
		}
		messages += len(file.Messages)
		if messages > MaxMessages {
			return nil, problem(LimitExceeded)
		}
		if seen[file.Locale] == nil {
			if len(seen) == MaxLocales {
				return nil, problem(LimitExceeded)
			}
			seen[file.Locale] = make(map[string]bool)
		}
		for index := range file.Messages {
			message := &file.Messages[index]
			if seen[file.Locale][message.ID] {
				return nil, problem(Duplicate)
			}
			seen[file.Locale][message.ID] = true
			if file.Locale == "en" {
				if err := validateSource(message); err != nil {
					return nil, err
				}
				catalog.sources[message.ID] = &message.sourceContract
			} else if message.Source == "" || message.Description != "" ||
				len(message.Parameters) != 0 || message.Count != "" {
				return nil, problem(InvalidResource)
			}
		}
		catalog.snapshot.Resources = append(catalog.snapshot.Resources, ResourceDigest{
			Name: strings.Clone(resource.Name), SHA256: digest(resource.Data),
		})
		files = append(files, file)
	}
	if len(catalog.sources) == 0 {
		return nil, problem(InvalidResource)
	}
	revisions := make(map[string]string, len(catalog.sources))
	for id, source := range catalog.sources {
		encoded, _ := json.Marshal(source)
		revisions[id] = digest(append([]byte(Profile+"\n"), encoded...))
		catalog.snapshot.Sources = append(catalog.snapshot.Sources, SourceRevision{id, revisions[id]})
	}
	bundles := make(map[string]*native.Bundle)
	for _, file := range files {
		if bundles[file.Locale] == nil {
			tag, _ := parseLocale(file.Locale)
			bundles[file.Locale] = native.NewBundle(tag)
			if err := bundles[file.Locale].AddMessages(tag); err != nil {
				return nil, problem(InvalidLocale)
			}
			catalog.entries[file.Locale] = make(map[string]bool)
			catalog.stale[file.Locale] = make(map[string]bool)
		}
		for _, message := range file.Messages {
			source := catalog.sources[message.ID]
			if source == nil {
				return nil, problem(InvalidResource)
			}
			stale := file.Locale != "en" && message.Source != revisions[message.ID]
			if file.Locale != "en" && !validDigest(message.Source) {
				return nil, problem(InvalidResource)
			}
			if !stale && source.Count == "" && (len(message.Forms) != 1 || message.Forms["other"] == "") {
				return nil, problem(InvalidResource)
			}
			for _, content := range message.Forms {
				compiled, err := compileTemplate(content)
				if err != nil {
					return nil, err
				}
				if !stale {
					for _, name := range compiled.arguments {
						if _, ok := source.Parameters[name]; !ok {
							return nil, problem(InvalidTemplate)
						}
					}
				}
				catalog.parser.templates[content] = compiled
			}
			if stale {
				catalog.stale[file.Locale][message.ID] = true
				catalog.snapshot.Stale = append(catalog.snapshot.Stale, StaleEntry{message.ID, file.Locale})
				continue
			}
			tag, _ := parseLocale(file.Locale)
			if err := bundles[file.Locale].AddMessages(tag, &native.Message{
				ID: message.ID, Zero: message.Forms["zero"], One: message.Forms["one"],
				Two: message.Forms["two"], Few: message.Forms["few"], Many: message.Forms["many"],
				Other: message.Forms["other"],
			}); err != nil {
				return nil, problem(InvalidLocale)
			}
			catalog.entries[file.Locale][message.ID] = true
		}
	}
	catalog.locales = append(catalog.locales, "en")
	for locale := range bundles {
		if locale != "en" {
			catalog.locales = append(catalog.locales, locale)
		}
		catalog.localizers[locale] = native.NewLocalizer(bundles[locale], locale)
	}
	slices.Sort(catalog.locales[1:])
	tags := make([]language.Tag, len(catalog.locales))
	for index, locale := range catalog.locales {
		tags[index], _ = parseLocale(locale)
	}
	catalog.matcher = language.NewMatcher(tags, language.PreferSameScript(false))
	slices.SortFunc(catalog.snapshot.Resources, func(left, right ResourceDigest) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(catalog.snapshot.Sources, func(left, right SourceRevision) int {
		return strings.Compare(left.ID, right.ID)
	})
	slices.SortFunc(catalog.snapshot.Stale, func(left, right StaleEntry) int {
		if order := strings.Compare(left.ID, right.ID); order != 0 {
			return order
		}
		return strings.Compare(left.Locale, right.Locale)
	})
	encoded, _ := json.Marshal(catalog.snapshot.Resources)
	catalog.snapshot.ID = digest(append([]byte(Profile+"\n"), encoded...))
	return catalog, nil
}

func digest(data []byte) string {
	value := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(value[:])
}

func validDigest(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, char := range value[7:] {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

func problem(code failure.Code) error { return failure.New(code, nil) }
