"""Subscribe to scraper MQTT topics and persist tickets in SQLite."""

import json
import logging
import os
import sqlite3
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

MQTT_IP = os.environ.get("MQTT_IP", "178.143.18.120")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USERNAME = os.environ.get("MQTT_USERNAME")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
# Use a separate client ID so this subscriber does not disconnect the scraper.
MQTT_CLIENT_ID = os.environ.get("DB_MQTT_ID", "di-ticket-db-manager")
DATABASE_FILE = Path(os.environ.get(
    "TICKET_DATABASE_FILE",
    str(Path(__file__).with_name("tickets.db")),
))

TICKET_NEW_TOPIC = "TUKE/DI/04/ticket_new"
TICKET_UPDATE_TOPIC = "TUKE/DI/04/ticket_update"
THEATRE_NEW_TOPIC = "theatre_new"
THEATRE_UPDATE_TOPIC = "theatre_update"

TICKET_COLUMNS = (
    "ticket_id",
    "scrape_timestamp",
    "source_website",
    "departure_airport",
    "destination_airport",
    "departure_date",
    "destination_date",
    "airline_name",
    "price",
    "currency",
    "number_of_stops",
    "cabin_class",
    "baggage_allowance",
    "total_capacity",
    "seats_remaining",
    "status",
)
UPDATE_COLUMNS = {
    "price": "price",
    "seats_remaining": "seats_remaining",
    "status": "status",
    "update_timestamp": "update_timestamp",
}
THEATRE_NEW_COLUMNS = (
    "ticket_id",
    "ticket_type",
    "performance_id",
    "production_id",
    "scrape_timestamp",
    "source_website",
    "theatre_name",
    "title",
    "performance_date",
    "timezone",
    "venue_id",
    "venue_name",
    "categories",
    "duration_minutes",
    "price",
    "currency",
    "price_basis",
    "price_categories",
    "price_scrape_timestamp",
    "total_capacity",
    "capacity_basis",
    "observed_available_seats",
    "observed_status",
    "seats_remaining",
    "status",
    "simulated",
    "inventory_basis",
    "booking_url",
    "source_urls",
    "ticket_fetch_status",
)
THEATRE_JSON_COLUMNS = {"categories", "price_categories", "source_urls"}

LOGGER = logging.getLogger("dbmanigger")

TICKET_NEW_SCHEMA = """
    CREATE TABLE IF NOT EXISTS {table_name} (
        ticket_id TEXT PRIMARY KEY,
        scrape_timestamp TIMESTAMP,
        source_website TEXT,
        departure_airport TEXT,
        destination_airport TEXT,
        departure_date TIMESTAMP,
        destination_date TIMESTAMP,
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
"""
TICKET_UPDATE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS {table_name} (
        update_id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticket_id TEXT NOT NULL REFERENCES {parent_table}(ticket_id),
        update_timestamp TIMESTAMP,
        price REAL,
        seats_remaining INTEGER,
        status TEXT
    )
"""
THEATRE_NEW_SCHEMA = """
    CREATE TABLE IF NOT EXISTS theatre_new (
        ticket_id TEXT PRIMARY KEY,
        ticket_type TEXT,
        performance_id TEXT,
        production_id TEXT,
        scrape_timestamp TIMESTAMP,
        source_website TEXT,
        theatre_name TEXT,
        title TEXT,
        performance_date TIMESTAMP,
        timezone TEXT,
        venue_id TEXT,
        venue_name TEXT,
        categories TEXT,
        duration_minutes INTEGER,
        price REAL,
        currency TEXT,
        price_basis TEXT,
        price_categories TEXT,
        price_scrape_timestamp TIMESTAMP,
        total_capacity INTEGER,
        capacity_basis TEXT,
        observed_available_seats INTEGER,
        observed_status TEXT,
        seats_remaining INTEGER,
        status TEXT,
        simulated BOOLEAN,
        inventory_basis TEXT,
        booking_url TEXT,
        source_urls TEXT,
        ticket_fetch_status TEXT
    )
"""
THEATRE_UPDATE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS theatre_update (
        update_id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticket_id TEXT NOT NULL REFERENCES theatre_new(ticket_id),
        update_timestamp TIMESTAMP,
        price REAL,
        seats_remaining INTEGER,
        status TEXT,
        simulated BOOLEAN
    )
"""


def initialize_database(connection):
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute(
        TICKET_NEW_SCHEMA.format(table_name="ticket_new")
    )
    connection.execute(
        TICKET_UPDATE_SCHEMA.format(
            table_name="ticket_update", parent_table="ticket_new"
        )
    )
    connection.execute(THEATRE_NEW_SCHEMA)
    connection.execute(THEATRE_UPDATE_SCHEMA)
    legacy_table = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'tickets'"
    ).fetchone()
    if legacy_table:
        connection.execute(f"""
            INSERT OR IGNORE INTO ticket_new ({", ".join(TICKET_COLUMNS)})
            SELECT {", ".join(TICKET_COLUMNS)} FROM tickets
        """)
        legacy_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(tickets)")
        }
        if "update_timestamp" in legacy_columns:
            connection.execute("""
                INSERT INTO ticket_update (
                    ticket_id, update_timestamp, price, seats_remaining, status
                )
                SELECT ticket_id, update_timestamp, price, seats_remaining, status
                FROM tickets
                WHERE update_timestamp IS NOT NULL
            """)
        connection.execute("DROP TABLE tickets")
    _migrate_timestamp_columns(connection)
    connection.commit()


def _migrate_timestamp_columns(connection):
    expected_types = {
        "ticket_new": {
            "scrape_timestamp": "TIMESTAMP",
            "departure_date": "TIMESTAMP",
            "destination_date": "TIMESTAMP",
        },
        "ticket_update": {"update_timestamp": "TIMESTAMP"},
    }
    needs_migration = False
    for table, columns in expected_types.items():
        actual_types = {
            row[1]: row[2].upper()
            for row in connection.execute(f"PRAGMA table_info({table})")
        }
        if any(actual_types.get(name) != kind for name, kind in columns.items()):
            needs_migration = True
            break
    if not needs_migration:
        return

    connection.commit()
    connection.execute("PRAGMA foreign_keys = OFF")
    try:
        connection.execute("BEGIN")
        connection.execute(
            TICKET_NEW_SCHEMA.format(table_name="ticket_new_timestamp")
        )
        ticket_columns = ", ".join(TICKET_COLUMNS)
        connection.execute(f"""
            INSERT INTO ticket_new_timestamp ({ticket_columns})
            SELECT {ticket_columns} FROM ticket_new
        """)
        connection.execute(
            TICKET_UPDATE_SCHEMA.format(
                table_name="ticket_update_timestamp",
                parent_table="ticket_new_timestamp",
            )
        )
        connection.execute("""
            INSERT INTO ticket_update_timestamp (
                update_id, ticket_id, update_timestamp, price, seats_remaining, status
            )
            SELECT update_id, ticket_id, update_timestamp, price, seats_remaining, status
            FROM ticket_update
        """)
        connection.execute("DROP TABLE ticket_update")
        connection.execute("DROP TABLE ticket_new")
        connection.execute(
            "ALTER TABLE ticket_new_timestamp RENAME TO ticket_new"
        )
        connection.execute(
            "ALTER TABLE ticket_update_timestamp RENAME TO ticket_update"
        )
        connection.commit()
    except sqlite3.Error:
        connection.rollback()
        raise
    finally:
        connection.execute("PRAGMA foreign_keys = ON")


def _message_records(payload):
    try:
        data = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        LOGGER.warning("Ignoring MQTT message with invalid JSON")
        return []
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        records = [record for record in data if isinstance(record, dict)]
        if len(records) != len(data):
            LOGGER.warning("Ignoring non-object entries in MQTT message")
        return records
    LOGGER.warning("Ignoring MQTT message that is not an object or array")
    return []


def _scalar_fields(record, allowed_fields):
    fields = {}
    for field in allowed_fields:
        if field not in record:
            continue
        value = record[field]
        if value is None or isinstance(value, (str, int, float, bool)):
            fields[field] = value
        else:
            LOGGER.warning("Ignoring ticket with non-scalar field %s", field)
            return None
    return fields


def save_new_tickets(connection, records):
    saved = 0
    columns = ", ".join(TICKET_COLUMNS)
    placeholders = ", ".join("?" for _ in TICKET_COLUMNS)
    query = f"INSERT OR IGNORE INTO ticket_new ({columns}) VALUES ({placeholders})"
    for record in records:
        fields = _scalar_fields(record, TICKET_COLUMNS)
        ticket_id = fields.get("ticket_id") if fields is not None else None
        if not isinstance(ticket_id, str) or not ticket_id.strip():
            LOGGER.warning("Ignoring new ticket without a valid ticket_id")
            continue
        values = [fields.get(column) for column in TICKET_COLUMNS]
        cursor = connection.execute(query, values)
        saved += cursor.rowcount
    return saved


def save_new_theatre_tickets(connection, records):
    saved = 0
    columns = ", ".join(THEATRE_NEW_COLUMNS)
    placeholders = ", ".join("?" for _ in THEATRE_NEW_COLUMNS)
    query = (
        f"INSERT OR IGNORE INTO theatre_new ({columns}) "
        f"VALUES ({placeholders})"
    )
    for record in records:
        fields = dict(record)
        for column in THEATRE_JSON_COLUMNS:
            if column not in fields or fields[column] is None:
                continue
            try:
                fields[column] = json.dumps(
                    fields[column], ensure_ascii=False, separators=(",", ":")
                )
            except (TypeError, ValueError):
                LOGGER.warning(
                    "Ignoring theatre ticket with invalid JSON field %s", column
                )
                fields = None
                break
        if fields is None:
            continue
        fields = _scalar_fields(fields, THEATRE_NEW_COLUMNS)
        ticket_id = fields.get("ticket_id") if fields is not None else None
        if not isinstance(ticket_id, str) or not ticket_id.strip():
            LOGGER.warning("Ignoring new theatre ticket without a valid ticket_id")
            continue
        values = [fields.get(column) for column in THEATRE_NEW_COLUMNS]
        cursor = connection.execute(query, values)
        saved += cursor.rowcount
    return saved


def apply_ticket_updates(connection, records):
    updated = 0
    for record in records:
        fields = _scalar_fields(record, ("ticket_id", *UPDATE_COLUMNS))
        ticket_id = fields.get("ticket_id") if fields is not None else None
        if not isinstance(ticket_id, str) or not ticket_id.strip():
            LOGGER.warning("Ignoring ticket update without a valid ticket_id")
            continue
        updates = {
            column: fields[source]
            for source, column in UPDATE_COLUMNS.items()
            if source in fields
        }
        if not updates:
            LOGGER.warning("Ignoring ticket update without update fields")
            continue

        exists = connection.execute(
            "SELECT 1 FROM ticket_new WHERE ticket_id = ?",
            (ticket_id,),
        ).fetchone()
        if exists is None:
            LOGGER.info(
                "Ignoring update for ticket_id not previously received on %s: %s",
                TICKET_NEW_TOPIC,
                ticket_id,
            )
            continue

        connection.execute(
            """
            INSERT INTO ticket_update (
                ticket_id, update_timestamp, price, seats_remaining, status
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                ticket_id,
                updates.get("update_timestamp"),
                updates.get("price"),
                updates.get("seats_remaining"),
                updates.get("status"),
            ),
        )
        updated += 1
    return updated


def apply_theatre_updates(connection, records):
    updated = 0
    update_fields = (
        "ticket_id", "update_timestamp", "price", "seats_remaining",
        "status", "simulated",
    )
    for record in records:
        fields = _scalar_fields(record, update_fields)
        ticket_id = fields.get("ticket_id") if fields is not None else None
        if not isinstance(ticket_id, str) or not ticket_id.strip():
            LOGGER.warning("Ignoring theatre update without a valid ticket_id")
            continue
        exists = connection.execute(
            "SELECT 1 FROM theatre_new WHERE ticket_id = ?",
            (ticket_id,),
        ).fetchone()
        if exists is None:
            LOGGER.info(
                "Ignoring theatre update for unknown ticket_id: %s", ticket_id
            )
            continue
        connection.execute(
            """
            INSERT INTO theatre_update (
                ticket_id, update_timestamp, price, seats_remaining, status, simulated
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                ticket_id,
                fields.get("update_timestamp"),
                fields.get("price"),
                fields.get("seats_remaining"),
                fields.get("status"),
                fields.get("simulated"),
            ),
        )
        updated += 1
    return updated


def handle_message(connection, topic, payload):
    records = _message_records(payload)
    if not records:
        return 0
    try:
        if topic == TICKET_NEW_TOPIC:
            count = save_new_tickets(connection, records)
            connection.commit()
            LOGGER.info("Saved %d new ticket(s)", count)
            return count
        if topic == TICKET_UPDATE_TOPIC:
            count = apply_ticket_updates(connection, records)
            connection.commit()
            LOGGER.info("Applied %d ticket update(s)", count)
            return count
        if topic == THEATRE_NEW_TOPIC:
            count = save_new_theatre_tickets(connection, records)
            connection.commit()
            LOGGER.info("Saved %d new theatre ticket(s)", count)
            return count
        if topic == THEATRE_UPDATE_TOPIC:
            count = apply_theatre_updates(connection, records)
            connection.commit()
            LOGGER.info("Applied %d theatre update(s)", count)
            return count
    except sqlite3.Error:
        connection.rollback()
        LOGGER.exception("Could not save MQTT message from %s", topic)
        return 0

    LOGGER.warning("Ignoring message from unexpected topic %s", topic)
    return 0


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    if not MQTT_USERNAME or not MQTT_PASSWORD:
        LOGGER.error("MQTT_USERNAME and MQTT_PASSWORD must be set in the environment or .env")
        return 1
    try:
        import paho.mqtt.client as mqtt
        from paho.mqtt import MQTTException
    except ImportError:
        LOGGER.error("MQTT subscriber requires paho-mqtt; install it with: python -m pip install paho-mqtt")
        return 1

    try:
        connection = sqlite3.connect(DATABASE_FILE)
    except sqlite3.Error:
        LOGGER.exception("Could not open SQLite database at %s", DATABASE_FILE)
        return 1

    with connection:
        initialize_database(connection)

    client = mqtt.Client(client_id=MQTT_CLIENT_ID)
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    def on_connect(mqtt_client, userdata, flags, reason_code, properties=None):
        failure = getattr(reason_code, "is_failure", False)
        if not failure:
            try:
                failure = int(reason_code) != 0
            except (TypeError, ValueError):
                failure = str(reason_code).lower() not in {"success", "0"}
        if failure:
            LOGGER.error("MQTT connection failed: %s", reason_code)
            return
        result, _ = mqtt_client.subscribe([
            (TICKET_NEW_TOPIC, 1),
            (TICKET_UPDATE_TOPIC, 1),
            (THEATRE_NEW_TOPIC, 1),
            (THEATRE_UPDATE_TOPIC, 1),
        ])
        if result != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.error("MQTT subscribe failed with result %s", result)
        else:
            LOGGER.info(
                "Connected to %s:%s; subscribed to %s, %s, %s and %s",
                MQTT_IP, MQTT_PORT, TICKET_NEW_TOPIC, TICKET_UPDATE_TOPIC,
                THEATRE_NEW_TOPIC, THEATRE_UPDATE_TOPIC,
            )

    def on_message(mqtt_client, userdata, message):
        handle_message(connection, message.topic, message.payload)

    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(MQTT_IP, MQTT_PORT, keepalive=60)
        LOGGER.info("Listening for ticket messages; SQLite database: %s", DATABASE_FILE)
        client.loop_forever()
    except KeyboardInterrupt:
        LOGGER.info("Stopping MQTT subscriber")
    except (OSError, MQTTException):
        LOGGER.exception("MQTT subscriber stopped after a connection error")
        return 1
    finally:
        client.disconnect()
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())