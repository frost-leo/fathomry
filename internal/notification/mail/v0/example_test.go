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

package mail_test

import (
	"context"
	"fmt"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	mail "github.com/frost-leo/fathomry/internal/notification/mail/v0"
	"github.com/frost-leo/fathomry/internal/resource"
)

func ExampleNewMessage() {
	_, err := mail.NewMessage(mail.Content{
		ID: "notice@example.test", From: "Sender <sender@example.test>", To: []string{"recipient@example.test"},
		Subject: "A bounded notification", Text: "The plain-text alternative.",
		HTML: "<p>The HTML alternative.</p>",
	})
	fmt.Println(err == nil)
	// Output: true
}

func ExampleSelect() {
	options := mail.OptionsV1{Name: "notifications", Host: "smtp.example.test", Port: 465,
		Hello: "worker.example.test", TLSMode: "implicit", Auth: "none", MaxMessages: 1, MaxRecipients: 1}
	selected, err := mail.Select(options)
	if err != nil {
		panic(err)
	}
	selected = resource.WithLimits(selected, mail.LimitsV1(options))
	initialization, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cleanup, endCleanup := context.WithTimeout(context.Background(), time.Second)
	defer endCleanup()
	assembly, err := resource.Assemble(initialization, cleanup, "example", selected)
	if assembly != nil {
		defer assembly.Close(cleanup)
	}
	if err != nil {
		panic(err)
	}
	inbox, err := invocation.NewInbox[mail.Result](1, 8<<20)
	if err != nil {
		panic(err)
	}
	client, err := mail.Bind(assembly, selected, inbox, nil)
	if err != nil {
		panic(err)
	}
	// Construction and binding perform no SMTP I/O. Only Send/SendBatch submit.
	fmt.Println(client.Profile().SDKMode)
	// Output: mail-smtp
}
