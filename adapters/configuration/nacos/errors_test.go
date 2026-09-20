/*
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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/framework/configuration"
	native "github.com/frost-leo/fathomry/internal/configsource/nacos/v2"
	"github.com/frost-leo/fathomry/internal/fault"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNativeDeadlineBeforeParentNotification(t *testing.T) {
	privateCause := errors.New("private-native-deadline-detail")
	for name, observed := range map[string]error{
		"direct":  context.DeadlineExceeded,
		"wrapped": fmt.Errorf("private-native-deadline-detail: %w", context.DeadlineExceeded),
		"native": native.ErrRead.New(fault.Context{},
			native.ErrUnavailable.New(fault.Context{}, context.DeadlineExceeded, privateCause)),
		"grpc": status.Error(codes.DeadlineExceeded, "private-native-deadline-detail"),
		"native-grpc": native.ErrRead.New(fault.Context{},
			native.ErrUnavailable.New(fault.Context{}, status.Error(codes.DeadlineExceeded, "private-native-deadline-detail"), privateCause)),
	} {
		t.Run(name, func(t *testing.T) {
			parent := context.Background()
			mapped := classify(parent, "base", observed)
			if !errors.Is(mapped, configuration.Cancelled) || !errors.Is(mapped, context.DeadlineExceeded) {
				t.Fatal("observed native deadline was lost before parent notification")
			}
			if parent.Err() != nil || errors.Is(mapped, privateCause) || errors.Is(mapped, native.ErrRead) ||
				strings.Contains(fmt.Sprintf("%+v", mapped), "private-native") {
				t.Fatal("classification altered parent state or exposed native diagnostics")
			}
		})
	}
}

func TestClassificationUsesDeadlineEvidenceNotNativeText(t *testing.T) {
	for _, observed := range []error{
		errors.New("context deadline exceeded"),
		native.ErrUnavailable.New(fault.Context{}, context.Canceled),
		status.Error(codes.Unavailable, "context deadline exceeded"),
		status.Error(codes.Canceled, "private-native-session-retirement"),
	} {
		mapped := classify(context.Background(), "base", observed)
		if !errors.Is(mapped, configuration.Unavailable) || errors.Is(mapped, configuration.Cancelled) ||
			errors.Is(mapped, context.DeadlineExceeded) || errors.Is(mapped, context.Canceled) {
			t.Fatal("native text or session retirement was misreported as caller cancellation")
		}
	}
	parent, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("intentional caller stop")
	cancel(cause)
	mapped := classify(parent, "base", context.DeadlineExceeded)
	if !errors.Is(mapped, configuration.Cancelled) || !errors.Is(mapped, context.Canceled) ||
		!errors.Is(mapped, cause) || errors.Is(mapped, context.DeadlineExceeded) {
		t.Fatal("observed caller cancellation lost precedence or its intentional cause")
	}
}
