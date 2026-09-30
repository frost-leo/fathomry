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

package command

import (
	"io"
	"time"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

const (
	MaxArguments     = 256
	MaxArgumentBytes = 128 << 10
	MaxOutputBytes   = 16 << 20
	Schema           = "fathomry.cli/v1"
)

// Options borrows streams for one invocation. Writers must obey io.Writer;
// sharing them with other invocations requires caller-provided synchronization.
type Options struct {
	Input          io.Reader
	Output         io.Writer
	ErrorOutput    io.Writer
	Language       string
	CleanupTimeout time.Duration
}

// Catalogs contains immutable explicit declarations, never a process registry.
type Catalogs struct {
	Errors   *failure.Catalog
	Messages *i18n.Catalog
}
