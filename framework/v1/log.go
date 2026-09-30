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

package framework

import (
	"context"
	"errors"
	"log/slog"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/resource/v1"
)

// ErrorLog binds safe error presentation once. The slog sink remains borrowed;
// nil deliberately disables emission without disabling presentation. Emission is
// not required evidence reception or proof of durable logging.
type ErrorLog struct {
	private
	sink     *slog.Logger
	fallback i18n.Presenter
	source   *resource.Ref[i18n.Presenter]
}

// Emission preserves the operational error separately from presentation/borrowing
// problems. Unknown native errors stay deliberately inspectable in Presented,
// but are never passed to slog's default formatter.
type Emission struct {
	private
	Presented  error
	Issue      error
	Recognized bool
}

// NewErrorLog installs no global locale, sink or settings reader.
func NewErrorLog(sink *slog.Logger, presenter i18n.Presenter) ErrorLog {
	return ErrorLog{sink: sink, fallback: presenter}
}

// WithResource selects a Presenter generation per emission. The fallback remains
// available on failed borrowing. Fixed resource policy does not freeze a
// deliberately supplied live settings.Reader inside that Presenter.
func (logger ErrorLog) WithResource(source resource.Ref[i18n.Presenter]) (ErrorLog, error) {
	if _, err := source.Inspect(); err != nil {
		return ErrorLog{}, err
	}
	logger.source = &source
	return logger, nil
}

// Emit performs one synchronous safe presentation/log attempt. It does not choose
// retry policy or replace native causes with translated strings. User projectors
// and slog handlers must be bounded; no hidden forwarding goroutine is created.
func (logger ErrorLog) Emit(ctx context.Context, original error) Emission {
	result := Emission{Presented: original}
	if original == nil {
		return result
	}
	if ctx == nil {
		result.Issue = fail(ErrOptions, "emit")
		return result
	}
	presenter := logger.fallback
	var lease resource.Lease[i18n.Presenter]
	acquired := false
	if logger.source != nil {
		var err error
		lease, err = logger.source.Acquire(ctx)
		if err != nil {
			result.Issue = err
		} else {
			acquired = true
			value, err := lease.Value()
			if err != nil {
				result.Issue = err
			} else {
				presenter = value
			}
		}
	}
	result.Presented = presenter.Present(original)
	_, result.Recognized = failure.Inspect(result.Presented)
	if rendered, ok := result.Presented.(*i18n.Presented); ok && rendered != nil {
		result.Issue = errors.Join(result.Issue, rendered.Issue())
	}
	if acquired {
		result.Issue = errors.Join(result.Issue, lease.Release())
	}
	if logger.sink != nil {
		attributes := []slog.Attr{slog.Bool("recognized", result.Recognized)}
		if result.Recognized {
			attributes = append(attributes, slog.Any("error", result.Presented))
		} else {
			attributes = append(attributes, slog.String("error", "Unclassified error; native details withheld."))
		}
		if result.Issue != nil {
			attributes = append(attributes, slog.Any("presentation_issue", result.Issue))
		}
		logger.sink.LogAttrs(ctx, slog.LevelError, "fathomry.error", attributes...)
	}
	return result
}
