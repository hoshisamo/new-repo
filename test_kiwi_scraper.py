"""Offline checks: python -m unittest test_kiwi_scraper.py"""

import json
import unittest
from datetime import datetime, timezone
from unittest.mock import call, patch

from kiwi_scraper import (
    discover_routes,
    bruno_ticket_payload,
    get_uuid,
    parse_offers,
    prepare_tickets,
    run_update_cycle,
    update_ticket_pool,
)

URL = "https://www.kiwi.com/en/cheap-flights/prague-czechia/kosice-slovakia/"


def fixture(destination="KSC", return_trip=False):
    offer = {
        "@type": "Offer", "url": URL.replace("cheap-flights", "search/results") +
        ("2026-10-14/2026-10-20/" if return_trip else "2026-10-14/no-return/"),
        "price": 53, "priceCurrency": "EUR",
        "itemOffered": {"itinerary": {"itemListElement": [{
            "@type": "Flight", "departureAirport": {"iataCode": "PRG"},
            "arrivalAirport": {"iataCode": destination},
            "departureTime": "2026-10-14T18:50:00", "description": "Nonstop (direct)",
        }]}},
    }
    records = [
        {"__id": "from", "code": "PRG"}, {"__id": "to", "code": destination},
        {"__id": "money", "amount": "53"}, {"__id": "carrier", "name": "Ryanair"},
        {"__id": "offer", "id": "offer", "__typename": "OnewaySearchResult",
         "source": {"__ref": "from"}, "destination": {"__ref": "to"},
         "price": {"__ref": "money"}, "carriers": {"__refs": ["carrier"]},
         "outboundStopovers": {"__refs": []},
         "outboundDepartureTime": "2026-10-14T18:50:00",
         "outboundArrivalTime": "2026-10-14T20:05:00"},
        {"__id": "results", "__typename": "LandingPageSearchResults",
         "onewaySearchResults": {"__refs": ["offer"]}, "updatedAt": "2026-09-25T02:01:04"},
    ]
    stream = "a:" + json.dumps(records) + "\n"
    # Flight streams may be split in the middle of a JSON record.
    packets = [stream[:61], stream[61:]]
    html = '<script type="application/ld+json">' + json.dumps([offer, offer]) + '</script>'
    for part in packets:
        html += '<script>self.__next_f.push(' + json.dumps([1, part]) + ')</script>'
    return html


class KiwiTests(unittest.TestCase):
    def test_enrichment_duplicates_and_unknowns(self):
        rows = parse_offers(fixture(), URL)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual((row['origin'], row['destination'], row['price'], row['currency']), ('PRG', 'KSC', 53, 'EUR'))
        self.assertEqual(row['arrival_time'], '20:05')
        self.assertEqual(row['airlines'], ['Ryanair'])
        self.assertEqual(row['stops'], 0)
        self.assertEqual(row['source_quote_at'], '2026-09-25T02:01:04')
        self.assertIsNone(row['duration_minutes'])
        self.assertIsNone(row['checked_baggage_included'])

    def test_wrong_destination_and_returns_excluded(self):
        for html in [fixture(destination='BCN'), fixture(return_trip=True)]:
            with self.assertRaises(ValueError):
                parse_offers(html, URL)

    def test_empty_response_fails(self):
        with self.assertRaises(ValueError):
            parse_offers('<html>Access unavailable</html>', URL)

    def test_only_real_inbound_kiwi_route_links(self):
        html = f'<a href="{URL}">route</a><a href="{URL}">duplicate</a>'
        html += '<a href="https://example.com/en/cheap-flights/a/kosice-slovakia/">wrong host</a>'
        html += '<a href="/en/cheap-flights/kosice-slovakia/prague-czechia/">outbound</a>'
        self.assertEqual(discover_routes(html, URL), [URL])

    def test_prepare_tickets_adds_synthetic_capacity_and_expiry(self):
        offers = [
            {"source_offer_id": "future", "departure_date": "2030-01-02"},
            {"source_offer_id": "past", "departure_date": "2029-12-31"},
        ]
        prepared = prepare_tickets(
            offers, datetime(2030, 1, 1, tzinfo=timezone.utc)
        )
        self.assertEqual([ticket["status"] for ticket in prepared], ["active", "expired"])
        for ticket in prepared:
            self.assertGreaterEqual(ticket["total_capacity"], 100)
            self.assertLessEqual(ticket["total_capacity"], 500)
            self.assertGreaterEqual(ticket["seats_remaining"], 1)
            self.assertLessEqual(
                ticket["seats_remaining"], ticket["total_capacity"] // 2
            )
            self.assertRegex(ticket["ticket_id"], r"^[0-9a-f]{16}$")
            self.assertIn(
                ticket["travel_class"],
                {"ECONOMY", "PREMIUM_ECONOMY", "BUSINESS", "FIRST"},
            )
            self.assertIsInstance(ticket["baggage_allowance"], bool)

    def test_get_uuid_uses_bruno_time_and_hex_padding(self):
        with (
            patch("kiwi_scraper.time.time", return_value=1700000000),
            patch("kiwi_scraper.random.choices", return_value=["a"] * 8),
        ):
            self.assertEqual(get_uuid(), "6553f100aaaaaaaa")

    def test_new_ticket_mqtt_payload_matches_bruno_schema(self):
        ticket = {
            "ticket_id": "ticket-1",
            "origin": "PRG",
            "destination": "KSC",
            "departure_date": "2030-01-02",
            "departure_time": "08:15",
            "arrival_date": "2030-01-02",
            "arrival_time": "09:40",
            "airlines": ["Ryanair", "Partner"],
            "price": 53,
            "currency": "EUR",
            "stops": 0,
            "travel_class": None,
            "cabin_baggage_included": None,
            "total_capacity": 300,
            "seats_remaining": 100,
            "status": "active",
            "source_offer_id": "not-in-mqtt-payload",
        }
        payload = bruno_ticket_payload(ticket)
        self.assertEqual(set(payload), {
            "ticket_id", "scrape_timestamp", "source_website",
            "departure_airport", "destination_airport", "departure_date",
            "destination_date", "airline_name", "price", "currency",
            "number_of_stops", "cabin_class", "baggage_allowance",
            "total_capacity", "seats_remaining", "status",
        })
        self.assertEqual(payload["source_website"], "Kiwi.com")
        self.assertEqual(payload["departure_airport"], "PRG")
        self.assertEqual(payload["destination_airport"], "KSC")
        self.assertEqual(payload["departure_date"], "2030-01-02T08:15:00Z")
        self.assertEqual(payload["destination_date"], "2030-01-02T09:40:00Z")
        self.assertEqual(payload["airline_name"], "Ryanair")
        self.assertEqual(payload["number_of_stops"], 0)
        self.assertIn(
            payload["cabin_class"],
            {"ECONOMY", "PREMIUM_ECONOMY", "BUSINESS", "FIRST"},
        )
        self.assertIsInstance(payload["baggage_allowance"], bool)
        self.assertNotIn("origin", payload)
        self.assertNotIn("source_offer_id", payload)

    def test_update_ticket_pool_emits_updates_and_reflects_sold_out(self):
        pool = [{
            "ticket_id": "ticket-1",
            "departure_date": "2030-01-02",
            "price": 53,
            "seats_remaining": 5,
            "status": "active",
        }]
        with patch("kiwi_scraper.random.randint", side_effect=[1, 5, 0]):
            updates, remaining = update_ticket_pool(
                pool, datetime(2030, 1, 1, tzinfo=timezone.utc)
            )
        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0]["ticket_id"], "ticket-1")
        self.assertEqual(updates[0]["seats_remaining"], 0)
        self.assertEqual(updates[0]["status"], "sold_out")
        self.assertEqual(updates[0]["price"], 200)
        self.assertEqual(remaining, [])

    def test_cycle_persists_scraped_tickets_when_mqtt_publish_fails(self):
        saved_pool = []
        with (
            patch("kiwi_scraper.load_ticket_pool", return_value=[]),
            patch("kiwi_scraper.update_ticket_pool", return_value=([], [])),
            patch(
                "kiwi_scraper.scrape",
                return_value=                {"offers": [{
                    "source_offer_id": "offer-1",
                    "departure_date": "2030-01-02",
                    "departure_time": "08:15",
                    "price": 53,
                }]},
            ),
            patch("kiwi_scraper.publish_tickets", side_effect=[True, None]),
            patch(
                "kiwi_scraper.save_ticket_pool",
                side_effect=lambda tickets: saved_pool.extend(tickets) or True,
            ),
        ):
            self.assertFalse(run_update_cycle())
        self.assertEqual(len(saved_pool), 1)
        self.assertEqual(saved_pool[0]["source_offer_id"], "offer-1")


if __name__ == '__main__':
    unittest.main()
