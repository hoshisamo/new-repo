"""Offline checks for MQTT-to-SQLite ticket handling."""

import json
import sqlite3
import unittest

from dbmanigger import (
    THEATRE_NEW_TOPIC,
    THEATRE_UPDATE_TOPIC,
    TICKET_NEW_TOPIC,
    TICKET_UPDATE_TOPIC,
    handle_message,
    initialize_database,
)


class DatabaseManagerTests(unittest.TestCase):
    def setUp(self):
        self.connection = sqlite3.connect(":memory:")
        initialize_database(self.connection)

    def tearDown(self):
        self.connection.close()

    def publish(self, topic, records):
        return handle_message(
            self.connection,
            topic,
            json.dumps(records).encode("utf-8"),
        )

    def test_new_ticket_is_saved_with_bruno_fields(self):
        ticket = {
            "ticket_id": "ticket-1",
            "scrape_timestamp": "2030-01-01T00:00:00Z",
            "source_website": "Kiwi.com",
            "departure_airport": "PRG",
            "destination_airport": "KSC",
            "departure_date": "2030-01-02T08:15:00Z",
            "destination_date": "2030-01-02T09:40:00Z",
            "airline_name": "Ryanair",
            "price": 53,
            "currency": "EUR",
            "number_of_stops": 0,
            "cabin_class": "ECONOMY",
            "baggage_allowance": True,
            "total_capacity": 300,
            "seats_remaining": 100,
            "status": "active",
        }

        self.assertEqual(self.publish(TICKET_NEW_TOPIC, [ticket]), 1)
        row = self.connection.execute(
            "SELECT departure_airport, baggage_allowance, status "
            "FROM ticket_new WHERE ticket_id = ?",
            ("ticket-1",),
        ).fetchone()
        self.assertEqual(row, ("PRG", 1, "active"))

    def test_updates_without_a_previously_saved_new_ticket_are_ignored(self):
        update = {
            "ticket_id": "unknown-ticket",
            "update_timestamp": "2030-01-01T00:15:00Z",
            "price": 100,
            "seats_remaining": 90,
            "status": "active",
        }

        self.assertEqual(self.publish(TICKET_UPDATE_TOPIC, [update]), 0)
        count = self.connection.execute(
            "SELECT COUNT(*) FROM ticket_update"
        ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_update_is_saved_separately_for_existing_ticket(self):
        self.publish(TICKET_NEW_TOPIC, [{
            "ticket_id": "ticket-1",
            "price": 53,
            "seats_remaining": 100,
            "status": "active",
        }])
        update = {
            "ticket_id": "ticket-1",
            "update_timestamp": "2030-01-01T00:15:00Z",
            "price": 75,
            "seats_remaining": 80,
            "status": "active",
        }

        self.assertEqual(self.publish(TICKET_UPDATE_TOPIC, [update]), 1)
        row = self.connection.execute(
            "SELECT ticket_id, price, seats_remaining, status, update_timestamp "
            "FROM ticket_update WHERE ticket_id = ?",
            ("ticket-1",),
        ).fetchone()
        self.assertEqual(
            row, ("ticket-1", 75.0, 80, "active", "2030-01-01T00:15:00Z")
        )
        original = self.connection.execute(
            "SELECT price, seats_remaining FROM ticket_new WHERE ticket_id = ?",
            ("ticket-1",),
        ).fetchone()
        self.assertEqual(original, (53.0, 100))

    def test_multiple_updates_are_kept_as_separate_rows(self):
        self.publish(TICKET_NEW_TOPIC, [{"ticket_id": "ticket-1"}])
        first = {
            "ticket_id": "ticket-1",
            "update_timestamp": "2030-01-01T00:15:00Z",
            "price": 75,
        }
        second = {
            "ticket_id": "ticket-1",
            "update_timestamp": "2030-01-01T00:30:00Z",
            "price": 80,
        }

        self.assertEqual(self.publish(TICKET_UPDATE_TOPIC, [first]), 1)
        self.assertEqual(self.publish(TICKET_UPDATE_TOPIC, [second]), 1)
        rows = self.connection.execute(
            "SELECT price, update_timestamp FROM ticket_update "
            "WHERE ticket_id = ? ORDER BY update_id",
            ("ticket-1",),
        ).fetchall()
        self.assertEqual(rows, [
            (75.0, "2030-01-01T00:15:00Z"),
            (80.0, "2030-01-01T00:30:00Z"),
        ])

    def test_database_has_separate_new_and_update_tables(self):
        tables = {
            row[0] for row in self.connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        self.assertIn("ticket_new", tables)
        self.assertIn("ticket_update", tables)
        self.assertNotIn("tickets", tables)

    def test_date_columns_use_timestamp_type(self):
        declared_types = {
            table: {
                row[1]: row[2].upper()
                for row in self.connection.execute(
                    f"PRAGMA table_info({table})"
                )
            }
            for table in ("ticket_new", "ticket_update")
        }
        self.assertEqual(declared_types["ticket_new"]["scrape_timestamp"], "TIMESTAMP")
        self.assertEqual(declared_types["ticket_new"]["departure_date"], "TIMESTAMP")
        self.assertEqual(declared_types["ticket_new"]["destination_date"], "TIMESTAMP")
        self.assertEqual(declared_types["ticket_update"]["update_timestamp"], "TIMESTAMP")

    def test_previous_split_schema_migrates_text_dates_to_timestamps(self):
        old_connection = sqlite3.connect(":memory:")
        old_connection.execute("""
            CREATE TABLE ticket_new (
                ticket_id TEXT PRIMARY KEY,
                scrape_timestamp TEXT,
                source_website TEXT,
                departure_airport TEXT,
                destination_airport TEXT,
                departure_date TEXT,
                destination_date TEXT,
                airline_name TEXT,
                price REAL,
                currency TEXT,
                number_of_stops INTEGER,
                cabin_class TEXT,
                baggage_allowance BOOLEAN,
                total_capacity INTEGER,
                seats_remaining INTEGER,
                status TEXT
            )
        """)
        old_connection.execute("""
            CREATE TABLE ticket_update (
                update_id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id TEXT NOT NULL,
                update_timestamp TEXT,
                price REAL,
                seats_remaining INTEGER,
                status TEXT
            )
        """)
        old_connection.execute("""
            INSERT INTO ticket_new (
                ticket_id, scrape_timestamp, departure_date, destination_date, price
            ) VALUES (
                'ticket-1', '2030-01-01T00:00:00Z',
                '2030-01-02T08:15:00Z', '2030-01-02T09:40:00Z', 53
            )
        """)
        old_connection.execute("""
            INSERT INTO ticket_update (ticket_id, update_timestamp, price)
            VALUES ('ticket-1', '2030-01-01T00:15:00Z', 75)
        """)

        initialize_database(old_connection)

        date_values = old_connection.execute("""
            SELECT scrape_timestamp, departure_date, destination_date
            FROM ticket_new WHERE ticket_id = 'ticket-1'
        """).fetchone()
        update_timestamp = old_connection.execute(
            "SELECT update_timestamp FROM ticket_update"
        ).fetchone()[0]
        self.assertEqual(date_values, (
            "2030-01-01T00:00:00Z",
            "2030-01-02T08:15:00Z",
            "2030-01-02T09:40:00Z",
        ))
        self.assertEqual(update_timestamp, "2030-01-01T00:15:00Z")
        self.assertEqual(
            old_connection.execute(
                "PRAGMA table_info(ticket_new)"
            ).fetchall()[1][2].upper(),
            "TIMESTAMP",
        )
        self.assertEqual(
            old_connection.execute(
                "PRAGMA table_info(ticket_update)"
            ).fetchall()[2][2].upper(),
            "TIMESTAMP",
        )
        old_connection.close()

    def test_legacy_tickets_table_migrates_into_both_tables(self):
        legacy_connection = sqlite3.connect(":memory:")
        legacy_connection.execute("""
            CREATE TABLE tickets (
                ticket_id TEXT PRIMARY KEY,
                scrape_timestamp TEXT,
                source_website TEXT,
                departure_airport TEXT,
                destination_airport TEXT,
                departure_date TEXT,
                destination_date TEXT,
                airline_name TEXT,
                price REAL,
                currency TEXT,
                number_of_stops INTEGER,
                cabin_class TEXT,
                baggage_allowance BOOLEAN,
                total_capacity INTEGER,
                seats_remaining INTEGER,
                status TEXT,
                update_timestamp TEXT
            )
        """)
        legacy_connection.execute("""
            INSERT INTO tickets (ticket_id, price, seats_remaining, status, update_timestamp)
            VALUES ('ticket-1', 75, 80, 'active', '2030-01-01T00:15:00Z')
        """)

        initialize_database(legacy_connection)

        migrated_ticket = legacy_connection.execute(
            "SELECT ticket_id, price FROM ticket_new"
        ).fetchone()
        migrated_update = legacy_connection.execute(
            "SELECT ticket_id, price, update_timestamp FROM ticket_update"
        ).fetchone()
        legacy_exists = legacy_connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'tickets'"
        ).fetchone()
        self.assertEqual(migrated_ticket, ("ticket-1", 75.0))
        self.assertEqual(
            migrated_update,
            ("ticket-1", 75.0, "2030-01-01T00:15:00Z"),
        )
        self.assertIsNone(legacy_exists)
        legacy_connection.close()

    def test_duplicate_new_ticket_id_does_not_overwrite_existing_record(self):
        self.publish(TICKET_NEW_TOPIC, [{
            "ticket_id": "ticket-1",
            "price": 53,
        }])
        self.assertEqual(self.publish(TICKET_NEW_TOPIC, [{
            "ticket_id": "ticket-1",
            "price": 99,
        }]), 0)
        price = self.connection.execute(
            "SELECT price FROM ticket_new WHERE ticket_id = ?",
            ("ticket-1",),
        ).fetchone()[0]
        self.assertEqual(price, 53.0)

    def test_theatre_new_payload_stores_nested_fields_as_json(self):
        theatre_ticket = {
            "ticket_id": "ndk-colosseum-131285",
            "ticket_type": "theatre",
            "performance_id": "colosseum-131285",
            "production_id": None,
            "scrape_timestamp": "2026-10-03T09:53:41+00:00",
            "source_website": "NdK / Colosseum",
            "theatre_name": "Národné divadlo Košice",
            "title": "Sándor Márai",
            "performance_date": "2026-10-03T19:00:00+02:00",
            "timezone": "Europe/Bratislava",
            "venue_id": "venue-historicka-budova",
            "venue_name": "HISTORICKÁ BUDOVA NDKE",
            "categories": None,
            "duration_minutes": None,
            "price": 11.9,
            "currency": "EUR",
            "price_basis": "minimum_published_category",
            "price_categories": [{
                "amount": "17.00",
                "currency": "EUR",
                "category_id": "213_0",
                "category_name": "Plná cena",
            }],
            "price_scrape_timestamp": "2026-10-03T09:53:55+00:00",
            "total_capacity": 105,
            "capacity_basis": "published_event_sale_capacity",
            "observed_available_seats": 40,
            "observed_status": None,
            "seats_remaining": 40,
            "status": "active",
            "simulated": True,
            "inventory_basis": "published_availability_seed",
            "booking_url": None,
            "source_urls": ["https://example.test/booking"],
            "ticket_fetch_status": "collected",
        }

        self.assertEqual(self.publish(THEATRE_NEW_TOPIC, [theatre_ticket]), 1)
        row = self.connection.execute(
            """
            SELECT title, performance_date, price_categories, source_urls, simulated
            FROM theatre_new WHERE ticket_id = ?
            """,
            ("ndk-colosseum-131285",),
        ).fetchone()
        self.assertEqual(row[0], "Sándor Márai")
        self.assertEqual(row[1], "2026-10-03T19:00:00+02:00")
        self.assertEqual(json.loads(row[2]), theatre_ticket["price_categories"])
        self.assertEqual(json.loads(row[3]), theatre_ticket["source_urls"])
        self.assertEqual(row[4], 1)

    def test_theatre_update_persists_nullable_price_for_known_id(self):
        self.publish(THEATRE_NEW_TOPIC, [{
            "ticket_id": "ndk-colosseum-131285",
            "ticket_type": "theatre",
        }])
        update = {
            "ticket_id": "ndk-colosseum-131285",
            "update_timestamp": "2026-10-03T09:56:29Z",
            "price": None,
            "seats_remaining": 258,
            "status": "active",
            "simulated": True,
        }

        self.assertEqual(self.publish(THEATRE_UPDATE_TOPIC, [update]), 1)
        row = self.connection.execute(
            """
            SELECT ticket_id, update_timestamp, price, seats_remaining, status, simulated
            FROM theatre_update
            """
        ).fetchone()
        self.assertEqual(row, (
            "ndk-colosseum-131285",
            "2026-10-03T09:56:29Z",
            None,
            258,
            "active",
            1,
        ))

    def test_theatre_update_for_unknown_ticket_id_is_ignored(self):
        update = {
            "ticket_id": "ndk-colosseum-unknown",
            "update_timestamp": "2026-10-03T09:56:29Z",
            "price": 25.0,
            "seats_remaining": 21,
            "status": "active",
            "simulated": True,
        }

        self.assertEqual(self.publish(THEATRE_UPDATE_TOPIC, [update]), 0)
        count = self.connection.execute(
            "SELECT COUNT(*) FROM theatre_update"
        ).fetchone()[0]
        self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()
