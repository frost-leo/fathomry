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

package configuration

import (
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"time"

	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
	adapters "github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/internal/resource"
)

// Core is plain sensitive configuration, not process state or runtime identity.
type Core struct {
	I18n     LocaleSettings   `json:"i18n"`
	Time     TimeSettings     `json:"time"`
	Instance InstanceSettings `json:"instance"`
}
type LocaleSettings struct {
	DefaultLocale string `json:"default_locale"`
}
type TimeSettings struct {
	DisplayZone string `json:"display_zone"`
}
type InstanceSettings struct {
	Name string `json:"name"`
}

// Settings combines Framework-owned Core and project-owned, strongly typed data.
// Like other DTOs it deliberately permits ordinary sensitive serialization.
type Settings[T any] struct {
	Framework Core `json:"framework"`
	Project   T    `json:"project"`
}

// DefaultSettings sets English/UTC conveniences. It is an ordinary shallow Go
// value constructor; Load/Watch admission, not this helper, isolates nested data.
func DefaultSettings[T any](project T) Settings[T] {
	return Settings[T]{Framework: Core{I18n: LocaleSettings{DefaultLocale: "en"}, Time: TimeSettings{DisplayZone: "UTC"}}, Project: project}
}

// Schema declares the complete document format and typed defaults. FormatVersion
// avoids colliding with fmt.Formatter's Format method on this guarded runtime
// wrapper. Validate must be pure/deterministic and concurrency-safe; it receives
// an isolated complete value after mandatory Core validation. Its mutations vanish.
type Schema[T any] struct {
	private
	FormatVersion uint32
	Defaults      Settings[T]
	Validate      func(Settings[T]) error
	_             typeIdentity[T]
}
type typeIdentity[T any] [0]func() T

// Layer names the three possible external slots, in increasing precedence.
type Layer uint8

const (
	Base Layer = iota + 1
	Environment
	Local
)

// LayerDocument binds one selected slot exactly once. Optional applies only to
// confirmed Missing, never unreadable, empty, malformed or invalid content.
type LayerDocument struct {
	private
	Document string
	Layer    Layer
	Optional bool
}
type Input struct {
	private
	Source    source.Selection
	Documents []LayerDocument
}

// VariableEncoding selects literal Text (including empty and "null") or exact
// JSON tokens. Field uses a slash-delimited path, e.g. /project/batch_size.
type VariableEncoding uint8

const (
	Text VariableEncoding = iota
	JSON
)

type Variable struct {
	private
	Name     string
	Field    string
	Encoding VariableEncoding
	Required bool
}

// Plan is a runtime declaration, not a deployment DTO. Select implementation
// Modules once; multiple unique source instances may reference each module.
// At most three total external slots and 64 explicit Variables are admitted.
type Plan struct {
	private
	Modules   []adapters.Module
	Inputs    []Input
	Variables []Variable
}

type envelope[T any] struct {
	Format    uint32 `json:"format"`
	Framework Core   `json:"framework"`
	Project   T      `json:"project"`
}
type snapshotState[T any] struct {
	prepared    resource.PreparedData[envelope[T]]
	description Description
}

// Snapshot is immutable and safe to share; zero is invalid. Plain value access
// is deliberately sensitive and always returns a detached copy.
type Snapshot[T any] struct {
	private
	state *snapshotState[T]
	_     typeIdentity[T]
}

// SuppliedLayer records logical selection/presence, not sensitive native selectors.
type SuppliedLayer struct {
	Source   string
	Document string
	Layer    Layer
	Presence source.Presence
}

// Provenance records supplied schema paths, not dynamic map keys or winning origins.
type Provenance struct {
	Layer  string
	Fields []string
}

// Description is safe owned metadata. Revision is random preparation identity,
// not equality, ordering, authentication, source revision or a durable Run ID.
type Description struct {
	FormatVersion uint32
	Revision      string
	Sources       []SuppliedLayer
	Provenance    []Provenance
}

func (snapshot Snapshot[T]) ValueCopy() (Settings[T], error) {
	if snapshot.state == nil {
		return Settings[T]{}, fail(ErrValue)
	}
	value, err := snapshot.state.prepared.ValueCopy()
	if err != nil {
		return Settings[T]{}, fail(ErrValue)
	}
	return Settings[T]{Framework: value.Framework, Project: value.Project}, nil
}
func (snapshot Snapshot[T]) Description() (Description, error) {
	if snapshot.state == nil {
		return Description{}, fail(ErrValue)
	}
	description := snapshot.state.description
	description.Sources = append([]SuppliedLayer(nil), description.Sources...)
	description.Provenance = append([]Provenance(nil), description.Provenance...)
	for index := range description.Provenance {
		description.Provenance[index].Fields = append([]string(nil), description.Provenance[index].Fields...)
	}
	return description, nil
}

// Presentation binds a snapshot's requested locale/zone explicitly after data
// acceptance. Zone availability is a platform concern, not a pure validator.
func (snapshot Snapshot[T]) Presentation() (Presentation, error) {
	value, err := snapshot.ValueCopy()
	if err != nil {
		return Presentation{}, err
	}
	var zone *time.Location
	if value.Framework.Time.DisplayZone == "UTC" {
		zone = time.FixedZone("UTC", 0)
	} else {
		zone, err = time.LoadLocation(value.Framework.Time.DisplayZone)
		if err != nil {
			return Presentation{}, fail(ErrPresentation)
		}
	}
	return Presentation{locale: value.Framework.I18n.DefaultLocale, zone: zone}, nil
}

// Presentation is immutable. Its private zone never changes TZ/time.Local;
// FormatTime changes display only, not the instant or previously frozen facts.
type Presentation struct {
	private
	locale string
	zone   *time.Location
}

func (view *Presentation) Locale() (string, error) {
	if view == nil || view.zone == nil {
		return "", fail(ErrValue)
	}
	return view.locale, nil
}
func (view *Presentation) FormatTime(instant time.Time, layout string) (string, error) {
	if view == nil || view.zone == nil {
		return "", fail(ErrValue)
	}
	return instant.In(view.zone).Format(layout), nil
}

type private struct{}

func (private) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, "configuration[restricted]")
}
func (private) LogValue() slog.Value         { return slog.StringValue("configuration[restricted]") }
func (private) MarshalJSON() ([]byte, error) { return nil, fail(ErrValue) }
func (*private) UnmarshalJSON([]byte) error  { return fail(ErrValue) }
func fail(condition failure.Condition, causes ...error) error {
	value, err := failure.New(condition, causes...)
	if err != nil {
		panic(err)
	}
	return value
}
func nilValue(value any) bool {
	if value == nil {
		return true
	}
	kind := reflect.ValueOf(value)
	switch kind.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
		return kind.IsNil()
	}
	return false
}
func label(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '.' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}
