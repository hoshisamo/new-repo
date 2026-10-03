import json
import math
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen
from paho.mqtt import MQTTException
import paho.mqtt.publish as publish


API_BASE_URL = "https://office.letenky.sk/api/pelijee-letenky-sk/"
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Accept-Language": "en;q=0.9",
    "appType": "mobile",
    "X-App-Language": "sk",
    "Referer": "https://www.letenky.sk/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        " (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}
MQTT_IP = os.environ.get("MQTT_IP", "178.143.18.120")
MQTT_PORT = os.environ.get("MQTT_PORT", "1883")
MQTT_USERNAME = os.environ.get("MQTT_USERNAME")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
MQTT_ID = os.environ.get("MQTT_ID", "di-ticket-scraper")
POOL_FILE = Path(os.environ.get(
    "TICKET_POOL_FILE",
    str(Path(__file__).with_name("ticket_pool.json")),
))
POOL_TARGET_SIZE = 50
UPDATE_INTERVAL_SECONDS = 15 * 60
MIN_TICKETS_PER_UPDATE = 5
MAX_TICKETS_PER_UPDATE = 15
MIN_PRICE_CHANGE = -50
MAX_PRICE_CHANGE = 50
ORIGIN_AIRPORT_CODES = (
    "ADD AGA AKL AMM ANC AUA AUH AYT BJV BKK BOG BOM BON BOS CAI CAN "
    "CEB CMB CNX COK CPT CUN CUR DAC DAD DEL DLM DPS DXB EBB ESB EVN "
    "EWR EZE FDF FUK GCM GIG GYD HAN HIJ HKG HKT HND HNL HRG IAD ICN "
    "IST JED JFK JKT JNB KBV KIX KTI KUL KUT LAS LAX LIM MEX MIA MLE "
    "MNL MRU NAS NBO NGO NRT OKA ORD PEK PEN PHL PTP PUJ RAK ROR RTB "
    "RUN SAO SCL SDQ SEA SEZ SFO SGN SIN SJO SKB SSH SXM SYD TLV TNR "
    "TPE UIO USM XIY YYZ ZNZ"
).split()


def fetch_session_token():
    try:
        req = Request(API_BASE_URL + "admin/getsession", headers=DEFAULT_HEADERS)
        with urlopen(req, timeout=60) as response:
            session_data = json.load(response)
        if not isinstance(session_data, dict):
            return None
        token = session_data.get("sessionToken")
        return token if isinstance(token, str) and token else None
    except (URLError, OSError, ValueError, TypeError):
        return None


def fetch_data(token=""):
    if not isinstance(token, str) or not token:
        return None
    try:
        current_date = datetime.now().date()
        payload = {
            "passengerCount": {
                "adt": 1,
                "yth": 0,
                "ythYears": [],
                "chd": 0,
                "chdYears": [],
                "inf": 0,
            },
            "travelClass": "ECONOMY",
            "rangeDates": True,
            "there": {
                "date": [current_date.year, current_date.month, current_date.day],
                "formattedDate": f"{current_date.day}.{current_date.month}.",
                "from": {
                    "code": " ".join(random.sample(ORIGIN_AIRPORT_CODES, 3)),
                    "type": "MULTI_CITY"
                },
                "to": {
                    "code": "KSC",
                    "type": "CITY"
                }
            },
            "filterQuery": {"sortBy": "RECOMMENDED"},
        }

        headers = {**DEFAULT_HEADERS, "Content-Type": "application/json"}
        data_bytes = json.dumps(payload).encode("utf-8")
        req = Request(
            API_BASE_URL + f"mobile/search?limit=10&offset=0&tabIdentifier=" + token,
            data=data_bytes, headers=headers, method="POST"
            )
        with urlopen(req, timeout=60) as response:
            return json.load(response)
    except (URLError, OSError, ValueError, TypeError):
        return None


def list_to_date(date_array):
    if date_array == None:
        return None
    try:
        dt_obj = datetime(*date_array)
        return dt_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, TypeError, OverflowError):
        return None


def resolve_airport_code(airports, code):
    for airport, details in airports.items():
        if details.get('cityCode') == code:
            return airport
    return code


def get_uuid():
    time_str = f"{int(time.time()):x}"
    padding = "".join(random.choices("0123456789abcdef", k=16-len(time_str)))
    return time_str + padding


def get_tickets(json_data):
    if not isinstance(json_data, dict):
        return None
    airports = json_data.get("airportCodes", {})
    flights = json_data.get("flights", [])
    if not isinstance(airports, dict) or not isinstance(flights, list):
        return None

    try:
        extracted_tickets = []
        timestamp_now = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        for trip in flights:
            if not isinstance(trip, dict):
                continue
            there_trips = trip.get("thereTrips", [])
            if not isinstance(there_trips, list):
                continue
            for flight in there_trips:
                if not isinstance(flight, dict):
                    continue
                carrier_codes = flight.get("carrierCodes", [])
                segments = flight.get("segments", [])
                if not isinstance(carrier_codes, list) or not isinstance(segments, list):
                    continue
                total_capacity = random.randint(2, 10) * 50
                extracted_tickets.append({
                    "ticket_id": get_uuid(),
                    "scrape_timestamp": timestamp_now,
                    "source_website": "Letenky.sk",
                    "departure_airport": resolve_airport_code(
                        airports, flight.get("from")
                    ),
                    "destination_airport": resolve_airport_code(
                        airports, flight.get("to")
                    ),
                    "departure_date": list_to_date(flight.get("departureDateTime")),
                    "destination_date": list_to_date(flight.get("arrivalDateTime")),
                    "airline_name": carrier_codes[0] if carrier_codes else None,
                    "price": trip.get("price"),
                    "currency": "EUR",
                    "number_of_stops": max(len(segments) - 1, 0),
                    "cabin_class": flight.get("travelClass"),
                    "baggage_allowance": trip.get("hasBaggage"),
                    "total_capacity": total_capacity,
                    "seats_remaining": random.randint(1, total_capacity // 2),
                    "status": "active" if flight.get("available") else "expired",
                })
        return extracted_tickets
    except (AttributeError, IndexError, KeyError, TypeError, ValueError, OverflowError):
        return None


def publish_tickets(topic, tickets):
    if not isinstance(tickets, list):
        return None
    if not tickets:
        return True
    if not MQTT_USERNAME or not MQTT_PASSWORD:
        return None
    try:
        publish.single(
            topic=topic,
            payload=json.dumps(tickets),
            hostname=MQTT_IP,
            port=int(MQTT_PORT),
            client_id=MQTT_ID,
            auth={
                "username": MQTT_USERNAME,
                "password": MQTT_PASSWORD,
            },
        )
        return True
    except (OSError, MQTTException, TypeError, ValueError):
        return None


def load_ticket_pool():
    try:
        with POOL_FILE.open(encoding="utf-8") as pool_file:
            tickets = json.load(pool_file)
    except FileNotFoundError:
        return []
    except (OSError, ValueError, TypeError):
        return None

    if isinstance(tickets, list) and all(
        isinstance(ticket, dict) for ticket in tickets
    ):
        return tickets
    return None


def save_ticket_pool(tickets):
    if not isinstance(tickets, list) or any(
        not isinstance(ticket, dict) for ticket in tickets
    ):
        return None

    temporary_file = POOL_FILE.with_suffix(POOL_FILE.suffix + ".tmp")
    try:
        with temporary_file.open("w", encoding="utf-8") as pool_file:
            json.dump(tickets, pool_file, indent=2)
            pool_file.write("\n")
        os.replace(temporary_file, POOL_FILE)
        return True
    except (OSError, TypeError, ValueError):
        try:
            temporary_file.unlink(missing_ok=True)
        except FileNotFoundError:
            pass
        except OSError:
            return None
        return None


def update_ticket_pool(tickets, current_time=None):
    try:
        now = current_time or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        active_tickets = [
            ticket for ticket in tickets if ticket.get("status") == "active"
        ]
        if not active_tickets:
            return [], []

        maximum = min(MAX_TICKETS_PER_UPDATE, len(active_tickets))
        minimum = min(MIN_TICKETS_PER_UPDATE, maximum)
        update_count = random.randint(minimum, maximum)
        selected_tickets = random.sample(active_tickets, update_count)
        updates = []

        for ticket in selected_tickets:
            departure_date = ticket.get("departure_date")
            if not isinstance(departure_date, str):
                return None
            departure_time = datetime.fromisoformat(
                departure_date.replace("Z", "+00:00")
            )
            if departure_time.tzinfo is None:
                departure_time = departure_time.replace(tzinfo=timezone.utc)

            seats_remaining = ticket.get("seats_remaining")
            if not isinstance(seats_remaining, int) or seats_remaining < 0:
                return None
            if seats_remaining:
                seats_remaining = max(0, seats_remaining - random.randint(5, 50))

            price = ticket.get("price")
            if not isinstance(price, (int, float)) or not math.isfinite(price):
                return None
            price = round(
                max(price, 200)
                + random.randint(
                    MIN_PRICE_CHANGE * 100, MAX_PRICE_CHANGE * 100
                ) / 100,
                2,
            )

            if departure_time < now:
                ticket["status"] = "expired"
            if seats_remaining == 0:
                ticket["status"] = "sold_out"

            ticket["price"] = price
            ticket["seats_remaining"] = seats_remaining
            updates.append({
                "ticket_id": ticket["ticket_id"],
                "update_timestamp": now.astimezone(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "price": price,
                "seats_remaining": seats_remaining,
                "status": ticket["status"],
            })

        remaining_tickets = [
            ticket for ticket in active_tickets if ticket["status"] == "active"
        ]
        return updates, remaining_tickets
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError):
        return None


def run_update_cycle():
    print("Starting ticket update cycle.")

    ticket_pool = load_ticket_pool()
    if ticket_pool is None:
        print("Load ticket pool: FAILED")
        print("Ticket update cycle: FAILED")
        return False
    print(f"Load ticket pool: OK ({len(ticket_pool)} tickets)")

    result = update_ticket_pool(ticket_pool)
    if result is None:
        print("Update ticket pool: FAILED")
        print("Ticket update cycle: FAILED")
        return False
    updates, ticket_pool = result
    print(f"Update ticket pool: OK ({len(updates)} tickets updated)")

    publish_result = publish_tickets("TUKE/DI/04/ticket_update", updates)
    print(
        "Publish ticket updates: "
        + ("OK" if publish_result is not None else "FAILED")
    )
    cycle_succeeded = publish_result is not None

    if len(ticket_pool) < POOL_TARGET_SIZE:
        print(
            f"Check ticket pool size: REFILL NEEDED "
            f"({len(ticket_pool)} below {POOL_TARGET_SIZE})"
        )
        token = fetch_session_token()
        if token is None:
            print("Fetch session token: FAILED")
            cycle_succeeded = False
        else:
            print("Fetch session token: OK")

        if token is not None:
            data = fetch_data(token)
            if data is None:
                print("Fetch tickets: FAILED")
                cycle_succeeded = False
            else:
                print("Fetch tickets: OK")
                new_tickets = get_tickets(data)
                if new_tickets is None:
                    print("Format fetched tickets: FAILED")
                    cycle_succeeded = False
                else:
                    print(f"Format fetched tickets: OK ({len(new_tickets)} tickets)")
                    publish_result = publish_tickets(
                        "TUKE/DI/04/ticket_new", new_tickets
                    )
                    print(
                        "Publish new tickets: "
                        + ("OK" if publish_result is not None else "FAILED")
                    )
                    if publish_result is None:
                        cycle_succeeded = False
                    else:
                        ticket_pool.extend(
                            ticket for ticket in new_tickets
                            if ticket.get("status") == "active"
                        )
    else:
        print(f"Check ticket pool size: OK ({len(ticket_pool)} meets target)")

    save_result = save_ticket_pool(ticket_pool)
    print("Save ticket pool: " + ("OK" if save_result else "FAILED"))
    if save_result is None:
        cycle_succeeded = False

    print(
        "Ticket update cycle: "
        + ("COMPLETE" if cycle_succeeded else "COMPLETED WITH FAILURES")
    )
    return cycle_succeeded


if __name__ == "__main__":
    while True:
        run_update_cycle()
        time.sleep(UPDATE_INTERVAL_SECONDS)

    