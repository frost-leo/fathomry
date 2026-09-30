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
	"io/fs"

	"github.com/frost-leo/fathomry/failure/v1"
	"golang.org/x/text/feature/plural"
	"golang.org/x/text/language"
)

// Schema and Profile identify this resource grammar; Engine records its qualified
// dependency baseline, not a report of a downstream consuming binary's modules.
const (
	Schema       = "fathomry.messages/v1"
	Profile      = "scalar-cardinal/v1"
	Engine       = "golang.org/x/text v0.41.0"
	LanguageData = language.CLDRVersion
	PluralData   = plural.CLDRVersion
)

// Limits bound admission/render work, not custom callbacks or process RSS.
const (
	MaxComponents      = 128
	MaxSources         = 512
	MaxLocales         = 64
	MaxDocumentBytes   = 65536
	MaxInputBytes      = 8 << 20
	MaxDepth           = 8
	MaxNodes           = 1 << 20
	MaxTokenBytes      = 8192
	MaxKeyBytes        = 256
	MaxMessages        = 4096
	MaxEntries         = 16384
	MaxArguments       = 16
	MaxForms           = 6
	MaxPatternBytes    = 8192
	MaxReferences      = 64
	MaxSegments        = 129
	MaxTotalSegments   = 1 << 18
	MaxStringBytes     = 4096
	MaxArgumentBytes   = 16384
	MaxOutputBytes     = 65536
	MaxInspectionBytes = 16 << 20
)

// Component declares one logical resource owner, not a source/client instance.
// Resources/Directory identify an explicit flat directory of <locale>.json files.
// Every message needs BaseLocale; translations pin the baseline source digest.
// Inputs/files must not change during Prepare. Files are closed by Prepare; custom
// FS implementations own blocking/concurrency behavior. No FS is retained afterward.
type Component struct {
	Module      string
	Name        string
	BaseLocale  string
	Resources   fs.FS
	Directory   string
	Definitions []failure.Definition
	Bindings    []Binding
}

// Binding selects a runtime message for one declared error. Details must exactly
// match the error's detail contract, including an empty contract; MessageContract
// must match the selected baseline message contract. Project receives
// only the direct occurrence, not a descendant found by errors.As. The component
// owns truthful same-occurrence extraction and non-sensitive, bounded,
// synchronous, concurrency-safe projection. Error/panic becomes a secondary issue;
// the original error is never replaced. Closures remain retained with the catalog.
type Binding struct {
	Code            failure.Code
	Message         string
	MessageContract string
	Details         failure.Contract
	Project         func(error) (Input, error)
}

// Input is an ephemeral render projection. Arguments/count are borrowed only for
// rendering and must not mutate concurrently. It is not a schema for error details.
type Input struct {
	Arguments []Argument
	Count     *uint64
}

// Parameter declares a named builtin scalar and its non-sensitive meaning.
type Parameter struct{ Name, Kind, Meaning string }

// Form identifies an integer cardinal category, not a numeric severity.
type Form string

const (
	Zero  Form = "zero"
	One   Form = "one"
	Two   Form = "two"
	Few   Form = "few"
	Many  Form = "many"
	Other Form = "other"
)

// Variant is one authored whole-message pattern.
type Variant struct {
	Category Form
	Pattern  string
}

// Definition is a detached resource inspection record. Digests identify content,
// not author authenticity. A translation inherits baseline argument semantics.
type Definition struct {
	Module, Component, ID, Locale, BaseLocale, Contract, Context string
	SourceDigest, DocumentDigest, SourceName                     string
	Cardinal                                                     bool
	Arguments                                                    []Parameter
	Forms                                                        []Variant
}

// ComponentInfo reports declarations/coverage, not running resource instances.
type ComponentInfo struct {
	Module, Name, BaseLocale string
	Locales, Messages        []string
	Codes                    []failure.Code
}

// Coverage reports exact-resource absence, never credits fallback as translation.
type Coverage struct {
	Module, Component, Locale string
	Missing                   []string
}
