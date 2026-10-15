/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package nativeprobe

import (
	"errors"
	"go.yaml.in/yaml/v3"
	"io"
	"os"
	"syscall"
)

type nativeProbeSettings struct{ AppID, AppSecret string }

func readSettings(path string) (nativeProbeSettings, error) {
	var root struct {
		AppID     string `yaml:"feishuid"`
		AppSecret string `yaml:"feishusecret"`
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nativeProbeSettings{}, errors.New("private configuration unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 1<<20 {
		return nativeProbeSettings{}, errors.New("configuration must be an owner-only regular file at most 1 MiB")
	}
	data, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	if err != nil || len(data) > 1<<20 {
		return nativeProbeSettings{}, errors.New("configuration read failed")
	}
	if yaml.Unmarshal(data, &root) != nil || root.AppID == "" || root.AppSecret == "" {
		return nativeProbeSettings{}, errors.New("missing Feishu credentials")
	}
	return nativeProbeSettings{AppID: root.AppID, AppSecret: root.AppSecret}, nil
}
