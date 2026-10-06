//go:build redis_service

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
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestPublicRedisLostAcknowledgement(t *testing.T) {
	node := startServer(t, "", false)
	oracle := serviceOracle(t, topologySettings(node.address))
	target := namespace(t) + "lost"
	address := peer(t, func(_ net.Conn, args []string) string {
		backend, err := net.DialTimeout("tcp", node.address, time.Second)
		if err != nil {
			return ""
		}
		defer backend.Close()
		_ = backend.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err := fmt.Fprintf(backend, "*%d\r\n", len(args)); err != nil {
			return ""
		}
		for _, arg := range args {
			if _, err := fmt.Fprintf(backend, "$%d\r\n%s\r\n", len(arg), arg); err != nil {
				return ""
			}
		}
		reply, err := bufio.NewReader(backend).ReadString('\n')
		if err != nil {
			return ""
		}
		if strings.EqualFold(args[0], "INCR") {
			return ""
		}
		return reply
	})
	owner, deps := openTest(t, topologySettings(address))
	output, err := owner.Client().Cache().Execute(testContext(t), command(t, Cache, "INCR", target))
	if !errors.Is(err, io.EOF) || output.Replies()[0].State() != Unknown || output.Replies()[0].HasValue() {
		t.Fatal("lost real acknowledgement invented value or certainty")
	}
	effect, err := oracle.Get(testContext(t), target).Int()
	serviceOK(t, err)
	if effect != 1 {
		t.Fatal("actual mutation retried or lost")
	}
	ack(t, deps.Evidence, 1)
	serviceOK(t, oracle.Del(testContext(t), target).Err())
	count, err := oracle.Exists(testContext(t), target).Result()
	serviceOK(t, err)
	if count != 0 {
		t.Fatal("owned effect cleanup absent")
	}
}
