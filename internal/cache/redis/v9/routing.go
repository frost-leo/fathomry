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

package redis

import (
	"strconv"
	"strings"
)

// routingPosition supplements generic Cmd routing for native command forms
// whose first argument is a subcommand, count, function name or option.
func (command Command) routingPosition() (int8, error) {
	if len(command.args) == 0 {
		return 0, failure(ErrInput, "arguments")
	}
	args := command.args
	name := strings.ToUpper(args[0])
	if name == "CLUSTER" {
		if len(args) < 2 {
			return 0, failure(ErrInput, "arguments")
		}
		sub := strings.ToUpper(args[1])
		if sub == "COUNTKEYSINSLOT" || sub == "GETKEYSINSLOT" {
			minimum := 3
			if sub == "GETKEYSINSLOT" {
				minimum = 4
			}
			if len(args) != minimum {
				return 0, failure(ErrInput, "arguments")
			}
			slot, err := strconv.Atoi(args[2])
			if err != nil || slot < 0 || slot >= 16384 {
				return 0, failure(ErrInput, "slot")
			}
		}
	}
	if command.keyPosition != 0 {
		return command.keyPosition, nil
	}
	position := 0
	counted := func(index int) (int, error) {
		if len(args) <= index {
			return 0, failure(ErrInput, "arguments")
		}
		count, err := strconv.Atoi(args[index])
		if err != nil || count < 1 || count > len(args)-index-1 {
			return 0, failure(ErrInput, "key-count")
		}
		return index + 1, nil
	}
	var err error
	switch name {
	case "FCALL", "FCALL_RO":
		if len(args) < 3 {
			return 0, failure(ErrInput, "arguments")
		}
		if args[2] == "0" {
			return 0, nil
		}
		position, err = counted(2)
	case "ZINTER", "ZUNION", "ZDIFF", "ZINTERCARD", "SINTERCARD", "SDIFFCARD", "SUNIONCARD", "LMPOP", "ZMPOP", "MSETEX":
		position, err = counted(1)
	case "BLMPOP", "BZMPOP":
		position, err = counted(2)
	case "BITOP":
		position = 2
	case "XGROUP", "XINFO":
		if len(args) < 2 {
			return 0, failure(ErrInput, "arguments")
		}
		if !strings.EqualFold(args[1], "HELP") {
			position = 2
		}
	case "MEMORY":
		if len(args) > 1 && strings.EqualFold(args[1], "USAGE") {
			position = 2
		}
	case "XREAD", "XREADGROUP":
		index := 1
		group := false
		for index < len(args) {
			token := strings.ToUpper(args[index])
			if token == "STREAMS" {
				if (len(args)-index-1) < 2 || (len(args)-index-1)%2 != 0 {
					return 0, failure(ErrInput, "streams")
				}
				position = index + 1
				break
			}
			switch token {
			case "COUNT", "BLOCK", "MAXCOUNT", "MAXSIZE":
				index += 2
			case "GROUP":
				if name != "XREADGROUP" || index+2 >= len(args) {
					return 0, failure(ErrInput, "streams")
				}
				group = true
				index += 3
			case "CLAIM":
				if name != "XREADGROUP" {
					return 0, failure(ErrInput, "streams")
				}
				index += 2
			case "NOACK":
				if name != "XREADGROUP" {
					return 0, failure(ErrInput, "streams")
				}
				index++
			default:
				return 0, failure(ErrInput, "streams")
			}
		}
		if position == 0 || name == "XREADGROUP" && !group {
			return 0, failure(ErrInput, "streams")
		}
	}
	if err != nil {
		return 0, err
	}
	if position >= len(args) || position > 127 {
		return 0, failure(ErrInput, "key-position")
	}
	return int8(position), nil
}

func (command Command) nativeArguments() []any {
	args := make([]any, len(command.args))
	for index, arg := range command.args {
		args[index] = arg
	}
	if len(args) == 0 {
		return args
	}
	name := strings.ToLower(command.args[0])
	args[0] = name
	if name == "cluster" && len(args) > 1 {
		sub := strings.ToLower(command.args[1])
		args[1] = sub
		if (sub == "getkeysinslot" || sub == "countkeysinslot") && len(args) > 2 {
			slot, _ := strconv.Atoi(command.args[2])
			args[2] = slot
		}
	}
	return args
}
