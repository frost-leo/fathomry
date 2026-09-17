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

package internal

func FathomryScopeActivityDescriptionV1(value *ClientActivityExecutionDescription, owner *FathomryScopeOwnerV1, guard FathomryDecodeGuardV1) *ClientActivityExecutionDescription {
	if value == nil || owner == nil || guard == nil {
		return value
	}
	copy := *value
	copy.fathomryScope = fathomryScope(value.fathomryScope, owner, guard)
	if copy.fathomryDecodeMu == nil {
		copy.fathomryDecodeMu = &fathomryDecodeMutex{}
	}
	return &copy
}

// GetHeartbeatDetails preserves the native getter under its optional owner scope.
func (value *ClientActivityExecutionDescription) GetHeartbeatDetails(valuePtrs ...any) error {
	if value.fathomryScope == nil {
		return value.fathomryGetHeartbeatDetails(valuePtrs...)
	}
	return value.fathomryScope.run(func() error {
		value.fathomryDecodeMu.Lock()
		defer value.fathomryDecodeMu.Unlock()
		return value.fathomryGetHeartbeatDetails(valuePtrs...)
	})
}

// GetInput preserves the native getter under its optional owner scope.
func (value *ClientActivityExecutionDescription) GetInput(valuePtrs ...any) error {
	if value.fathomryScope == nil {
		return value.fathomryGetInput(valuePtrs...)
	}
	return value.fathomryScope.run(func() error {
		value.fathomryDecodeMu.Lock()
		defer value.fathomryDecodeMu.Unlock()
		return value.fathomryGetInput(valuePtrs...)
	})
}

// GetResult preserves the native getter under its optional owner scope.
func (value *ClientActivityExecutionDescription) GetResult(valuePtr any) error {
	if value.fathomryScope == nil {
		return value.fathomryGetResult(valuePtr)
	}
	return value.fathomryScope.run(func() error {
		value.fathomryDecodeMu.Lock()
		defer value.fathomryDecodeMu.Unlock()
		return value.fathomryGetResult(valuePtr)
	})
}

// GetOutcomeFailure preserves the native getter under its optional owner scope.
func (value *ClientActivityExecutionDescription) GetOutcomeFailure() error {
	if value.fathomryScope == nil {
		return value.fathomryGetOutcomeFailure()
	}
	return value.fathomryScope.run(func() error {
		value.fathomryDecodeMu.Lock()
		defer value.fathomryDecodeMu.Unlock()
		return value.fathomryGetOutcomeFailure()
	})
}

// GetLastFailure preserves the native getter under its optional owner scope.
func (value *ClientActivityExecutionDescription) GetLastFailure() error {
	if value.fathomryScope == nil {
		return value.fathomryGetLastFailure()
	}
	return value.fathomryScope.run(func() error {
		value.fathomryDecodeMu.Lock()
		defer value.fathomryDecodeMu.Unlock()
		return value.fathomryGetLastFailure()
	})
}

// GetSummary preserves the native getter under its optional owner scope.
func (value *ClientActivityExecutionDescription) GetSummary() (string, error) {
	if value.fathomryScope == nil {
		return value.fathomryGetSummary()
	}
	var output string
	err := value.fathomryScope.run(func() (err error) {
		value.fathomryDecodeMu.Lock()
		defer value.fathomryDecodeMu.Unlock()
		output, err = value.fathomryGetSummary()
		return err
	})
	return output, err
}

// GetStaticDetails preserves the native getter under its optional owner scope.
func (value *ClientActivityExecutionDescription) GetStaticDetails() (string, error) {
	if value.fathomryScope == nil {
		return value.fathomryGetStaticDetails()
	}
	var output string
	err := value.fathomryScope.run(func() (err error) {
		value.fathomryDecodeMu.Lock()
		defer value.fathomryDecodeMu.Unlock()
		output, err = value.fathomryGetStaticDetails()
		return err
	})
	return output, err
}

func FathomryScopeWorkflowDescriptionV1(value *WorkflowExecutionDescription, owner *FathomryScopeOwnerV1, guard FathomryDecodeGuardV1) *WorkflowExecutionDescription {
	if value == nil || owner == nil || guard == nil {
		return value
	}
	copy := *value
	copy.fathomryScope = fathomryScope(value.fathomryScope, owner, guard)
	if copy.fathomryDecodeMu == nil {
		copy.fathomryDecodeMu = &fathomryDecodeMutex{}
	}
	return &copy
}

// GetStaticSummary preserves the native getter under its optional owner scope.
func (value *WorkflowExecutionDescription) GetStaticSummary() (string, error) {
	if value.fathomryScope == nil {
		return value.fathomryGetStaticSummary()
	}
	var output string
	err := value.fathomryScope.run(func() (err error) {
		value.fathomryDecodeMu.Lock()
		defer value.fathomryDecodeMu.Unlock()
		output, err = value.fathomryGetStaticSummary()
		return err
	})
	return output, err
}

// GetStaticDetails preserves the native getter under its optional owner scope.
func (value *WorkflowExecutionDescription) GetStaticDetails() (string, error) {
	if value.fathomryScope == nil {
		return value.fathomryGetStaticDetails()
	}
	var output string
	err := value.fathomryScope.run(func() (err error) {
		value.fathomryDecodeMu.Lock()
		defer value.fathomryDecodeMu.Unlock()
		output, err = value.fathomryGetStaticDetails()
		return err
	})
	return output, err
}

// GetMemoValue preserves the native getter under its optional owner scope.
func (value *WorkflowExecutionDescription) GetMemoValue(key string, valuePtr any) error {
	if value.fathomryScope == nil {
		return value.fathomryGetMemoValue(key, valuePtr)
	}
	return value.fathomryScope.run(func() error {
		value.fathomryDecodeMu.Lock()
		defer value.fathomryDecodeMu.Unlock()
		return value.fathomryGetMemoValue(key, valuePtr)
	})
}

func FathomryScopeNexusDescriptionV1(value *ClientNexusOperationExecutionDescription, owner *FathomryScopeOwnerV1, guard FathomryDecodeGuardV1) *ClientNexusOperationExecutionDescription {
	if value == nil || owner == nil || guard == nil {
		return value
	}
	copy := *value
	copy.fathomryScope = fathomryScope(value.fathomryScope, owner, guard)
	if copy.fathomryDecodeMu == nil {
		copy.fathomryDecodeMu = &fathomryDecodeMutex{}
	}
	copy.CancellationInfo = FathomryScopeNexusCancellationV1(value.CancellationInfo, owner, guard)
	return &copy
}

// GetSummary preserves the native getter under its optional owner scope.
func (value *ClientNexusOperationExecutionDescription) GetSummary() (string, error) {
	if value.fathomryScope == nil {
		return value.fathomryGetSummary()
	}
	var output string
	err := value.fathomryScope.run(func() (err error) {
		value.fathomryDecodeMu.Lock()
		defer value.fathomryDecodeMu.Unlock()
		output, err = value.fathomryGetSummary()
		return err
	})
	return output, err
}

// GetLastAttemptFailure preserves the native getter under its optional owner scope.
func (value *ClientNexusOperationExecutionDescription) GetLastAttemptFailure() error {
	if value.fathomryScope == nil {
		return value.fathomryGetLastAttemptFailure()
	}
	return value.fathomryScope.run(func() error {
		value.fathomryDecodeMu.Lock()
		defer value.fathomryDecodeMu.Unlock()
		return value.fathomryGetLastAttemptFailure()
	})
}

func FathomryScopeNexusCancellationV1(value *ClientNexusOperationCancellationInfo, owner *FathomryScopeOwnerV1, guard FathomryDecodeGuardV1) *ClientNexusOperationCancellationInfo {
	if value == nil || owner == nil || guard == nil {
		return value
	}
	copy := *value
	copy.fathomryScope = fathomryScope(value.fathomryScope, owner, guard)
	if copy.fathomryDecodeMu == nil {
		copy.fathomryDecodeMu = &fathomryDecodeMutex{}
	}
	return &copy
}

// GetLastAttemptFailure preserves the native getter under its optional owner scope.
func (value *ClientNexusOperationCancellationInfo) GetLastAttemptFailure() error {
	if value.fathomryScope == nil {
		return value.fathomryGetLastAttemptFailure()
	}
	return value.fathomryScope.run(func() error {
		value.fathomryDecodeMu.Lock()
		defer value.fathomryDecodeMu.Unlock()
		return value.fathomryGetLastAttemptFailure()
	})
}
