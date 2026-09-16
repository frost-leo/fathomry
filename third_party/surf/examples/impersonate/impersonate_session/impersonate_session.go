package main

import "github.com/enetx/surf"

func main() {
	const url = "https://tls.peet.ws/api/clean"

	cli := surf.NewClient().
		Builder().
		Session(). // Enables TLS session cache: 1st request = full handshake, 2nd = resumed with PSK (ext 41)
		Impersonate().
		Chrome().
		// // Disable TLS session.
		// With(func(cli *surf.Client) error {
		// 	cli.GetTLSConfig().ClientSessionCache = nil
		// 	return nil
		// }).
		Build().
		Unwrap()

	r := cli.Get(url).Do()
	r.Ok().Body.String().Unwrap().Println()

	// "peetprint": "GREASE-772-771|2-1.1|GREASE-4588-29-23-24|2308-2309-2310-1027-2052-1025-1283-2053-1281-2054-1537|1|2|GREASE-4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53|0-10-11-13-16-17613-18-23-27-35-43-45-5-51-65037-65281-GREASE-GREASE"
	// "peetprint_hash": "67c3e9111bed9e7f03d2f21d6d88994b"
	// No extension 41 (pre_shared_key) — this is a full initial handshake

	r = cli.Get(url).Do()
	r.Ok().Body.String().Unwrap().Println()

	// "peetprint": "GREASE-772-771|2-1.1|GREASE-4588-29-23-24|2308-2309-2310-1027-2052-1025-1283-2053-1281-2054-1537|1|2|GREASE-4865-4866-4867-49195-49199-49196-49200-52393-52392-49171-49172-156-157-47-53|0-10-11-13-16-17613-18-23-27-35-41-43-45-5-51-65037-65281-GREASE-GREASE"
	// "peetprint_hash": "35fc5e864929e3b01e9ba9eb41bc1360"
	// Includes extension 41 (pre_shared_key) — resumed from saved session
}
