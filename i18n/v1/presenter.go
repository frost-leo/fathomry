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
	"fmt"
	"log/slog"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

// Preferences is component-owned project data. Locale is one BCP 47 tag, not an
// Accept-Language list. Empty is invalid, not an implicit English preference.
type Preferences struct {
	Locale string `json:"locale" yaml:"locale" mapstructure:"locale"`
}

// PreferencesPath is the component's explicit section in a project-owned root.
const PreferencesPath = "/application/i18n"

// Validate checks the declared language without reading settings or resource files.
func (preferences Preferences) Validate() error {
	_, err := parseLocale(preferences.Locale, false)
	return err
}

// Presenter holds an immutable catalog and a language-selection route. Default
// mode captures application settings once per call; WithLocale and WithSettings
// return isolated overrides. Copies share catalog/callback state and may be used
// concurrently under the component projection contract. Zero is unprepared.
type Presenter struct {
	catalog     *catalogState
	locale      string
	reader      settings.Reader
	application bool
}

// NewPresenter captures resources without requiring settings to be configured.
// Early bootstrap errors can therefore keep their original static explanation.
func NewPresenter(catalog *Catalog) (Presenter, error) {
	if !catalog.valid() {
		return Presenter{}, reject(ErrCatalog)
	}
	return Presenter{catalog: catalog.state, application: true}, nil
}

// WithLocale binds an explicit language independently of settings. It does not
// prove an exact translation exists; resolve/render reports actual provenance.
func (presenter Presenter) WithLocale(locale string) (Presenter, error) {
	if presenter.catalog == nil {
		return Presenter{}, reject(ErrCatalog)
	}
	tag, err := parseLocale(locale, false)
	if err != nil {
		return Presenter{}, err
	}
	presenter.locale = tag.String()
	presenter.application = false
	presenter.reader = settings.Reader{}
	return presenter, nil
}

// WithSettings binds an independent populated reader, validating its current
// typed preference. Later calls capture subsequent values without selecting the
// process default. Failure leaves the original presenter unchanged.
func (presenter Presenter) WithSettings(reader settings.Reader) (Presenter, error) {
	if presenter.catalog == nil {
		return Presenter{}, reject(ErrCatalog)
	}
	presenter.locale = ""
	presenter.application = false
	presenter.reader = reader
	if _, err := presenter.preference(); err != nil {
		return Presenter{}, err
	}
	return presenter, nil
}
func (presenter Presenter) preference() (string, error) {
	if presenter.locale != "" {
		return presenter.locale, nil
	}
	var view settings.View
	var err error
	if presenter.application {
		view, err = settings.Default()
	} else {
		view, err = presenter.reader.Capture()
	}
	if err != nil {
		return "", reject(ErrPreferences, err)
	}
	preferences, found, err := settings.Read(view, PreferencesPath, func(value Preferences) Preferences { return value })
	if err != nil {
		return "", reject(ErrPreferences, err)
	}
	if !found {
		return "", reject(ErrPreferences)
	}
	tag, err := parseLocale(preferences.Locale, false)
	if err != nil {
		return "", reject(ErrPreferences, err)
	}
	return tag.String(), nil
}

// Render uses the selected preference for an ordinary resource. It returns no
// partial text on failure; unlike Present, it has no original error to fall back to.
func (presenter Presenter) Render(id string, arguments []Argument, count *uint64) (Rendered, error) {
	if presenter.catalog == nil {
		return Rendered{}, reject(ErrCatalog)
	}
	locale, err := presenter.preference()
	if err != nil {
		return Rendered{}, err
	}
	selection, err := (&Catalog{state: presenter.catalog}).Resolve(id, locale)
	if err != nil {
		return Rendered{}, err
	}
	return selection.Render(arguments, count)
}

// Presentation is a detached record of the frozen human text and its provenance.
// Failure diagnostics remain available from the original common error core.
type Presentation struct {
	Message   Rendered
	Selection SelectionInfo
}
type presentedState struct {
	original error
	core     *failure.Error
	report   Presentation
	issue    error
}

// Presented wraps one original occurrence without rewriting its identity/causes.
// Formatting uses captured text, never raw causes or the component's Error method.
// A later preference update cannot change this occurrence's recorded presentation.
type Presented struct{ state *presentedState }

// Present preserves nil and returns foreign errors untouched, without formatting
// them or guessing a primary inside joins/wrappers. Direct failure occurrences get
// a frozen presentation. Configuration/binding/projection/render failures retain
// the original developer message and are exposed separately through Issue.
//
// Only this package's own existing presentation wrapper is removed on re-presentation.
// The same original occurrence is passed to an admitted owner projector. Panics in
// that projector become ErrProjection without retaining/formatting the panic value.
func (presenter Presenter) Present(original error) error {
	if original == nil {
		return nil
	}
	if previous, ok := original.(*Presented); ok && previous != nil && previous.state != nil {
		original = previous.state.original
	}
	core, ok := failure.Inspect(original)
	if !ok {
		return original
	}
	definition := core.Diagnostic().Definition
	state := &presentedState{original: original, core: core, report: Presentation{
		Message:   Rendered{Text: definition.Message, Category: Other, Variant: Other},
		Selection: SelectionInfo{Fallback: PresentationFailure},
	}}
	result := &Presented{state: state}
	if presenter.catalog == nil {
		state.issue = reject(ErrCatalog)
		return result
	}
	registered, found, err := presenter.catalog.failures.Lookup(definition.Code)
	if err != nil || !found || registered != definition {
		state.issue = reject(ErrBinding, err)
		return result
	}
	for _, item := range presenter.catalog.entries[string(definition.Identifier)] {
		state.report.Message.Locale = item.definition.BaseLocale
		state.report.Selection.Locale = item.definition.BaseLocale
		break
	}
	locale, err := presenter.preference()
	if err != nil {
		state.issue = err
		return result
	}
	state.report.Selection.Requested = locale
	id := string(definition.Identifier)
	input := Input{}
	if binding, exists := presenter.catalog.bindings[definition.Code]; exists {
		id = binding.Message
		input, err = project(binding.Project, original)
		if err != nil {
			state.issue = err
			return result
		}
	}
	selection, err := (&Catalog{state: presenter.catalog}).Resolve(id, locale)
	if err != nil {
		state.issue = err
		return result
	}
	rendered, err := selection.Render(input.Arguments, input.Count)
	if err != nil {
		state.issue = err
		return result
	}
	state.report = Presentation{Message: rendered, Selection: selection.info}
	return result
}
func project(callback func(error) (Input, error), original error) (input Input, err error) {
	completed := false
	defer func() {
		_ = recover()
		if !completed {
			input = Input{}
			err = reject(ErrProjection)
		}
	}()
	input, err = callback(original)
	completed = true
	if err != nil {
		return Input{}, reject(ErrProjection, err)
	}
	return input, nil
}

// Failure returns this same occurrence's core, not a cause selected by traversal.
func (err *Presented) Failure() *failure.Error {
	if err == nil || err.state == nil {
		return nil
	}
	return err.state.core
}

// Unwrap returns the exact original error, retaining all intentional Is/As behavior.
func (err *Presented) Unwrap() error {
	if err == nil || err.state == nil {
		return nil
	}
	return err.state.original
}

// Info returns captured text/provenance; nil/zero receivers return an empty record.
func (err *Presented) Info() Presentation {
	if err == nil || err.state == nil {
		return Presentation{}
	}
	return err.state.report
}

// Issue is a secondary presentation failure, never an operation/retry result.
// Explicit access may expose a native projection/settings/I/O cause.
func (err *Presented) Issue() error {
	if err == nil || err.state == nil {
		return nil
	}
	return err.state.issue
}
func (err *Presented) Error() string {
	if err == nil {
		return "<nil>"
	}
	if err.state == nil {
		return "i18n: invalid presentation"
	}
	definition := err.state.core.Diagnostic().Definition
	return definition.Code.String() + " (" + string(definition.Identifier) + "): " + err.state.report.Message.Text
}
func (err Presented) Format(state fmt.State, verb rune) {
	if verb == 'q' {
		_, _ = fmt.Fprintf(state, "%q", (&err).Error())
	} else {
		_, _ = state.Write([]byte((&err).Error()))
	}
}
func (err *Presented) LogValue() slog.Value {
	if err == nil || err.state == nil {
		return slog.StringValue(err.Error())
	}
	diagnostic := err.state.core.Diagnostic()
	report := err.state.report
	issueCode := ""
	if issue, ok := failure.Inspect(err.state.issue); ok {
		issueCode = issue.Diagnostic().Definition.Code.String()
	}
	return slog.GroupValue(
		slog.String("code", diagnostic.Definition.Code.String()),
		slog.String("identifier", string(diagnostic.Definition.Identifier)),
		slog.String("module", diagnostic.Definition.Module), slog.String("component", diagnostic.Definition.Component),
		slog.String("operation", diagnostic.Location.Operation), slog.String("instance", diagnostic.Location.Instance),
		slog.String("message", report.Message.Text), slog.String("locale", report.Message.Locale),
		slog.String("requested_locale", report.Selection.Requested), slog.String("fallback", string(report.Selection.Fallback)),
		slog.String("presentation_issue", issueCode), slog.Int("cause_count", diagnostic.CauseCount),
	)
}
func (Presented) MarshalJSON() ([]byte, error)   { return nil, reject(ErrSerialization) }
func (*Presented) UnmarshalJSON([]byte) error    { return reject(ErrSerialization) }
func (Presenter) Format(state fmt.State, _ rune) { _, _ = state.Write([]byte("i18n.Presenter")) }
func (Presenter) LogValue() slog.Value           { return slog.StringValue("i18n.Presenter") }
func (Presenter) MarshalJSON() ([]byte, error)   { return nil, reject(ErrSerialization) }
func (*Presenter) UnmarshalJSON([]byte) error    { return reject(ErrSerialization) }
