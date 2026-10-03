#!/usr/bin/env python3
"""Collect dated one-way Kiwi offers to KSC. Python 3.10+."""

import dotenv
dotenv.load_dotenv()  # Load .env file if present, before importing paho.m
import argparse
import json
import math
import os
import random
import re
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit
from urllib.request import Request, urlopen

DEFAULT_URL = "https://www.kiwi.com/en/city/kosice-slovakia/"
MQTT_IP = os.environ.get("MQTT_IP", "178.143.18.120")
MQTT_PORT = os.environ.get("MQTT_PORT", "1883")
MQTT_ID = os.environ.get("MQTT_ID", "di-kiwi-scraper")
POOL_FILE = Path(os.environ.get(
    "KIWI_TICKET_POOL_FILE",
    str(Path(__file__).with_name("kiwi_ticket_pool.json")),
))
POOL_TARGET_SIZE = 50
UPDATE_INTERVAL_SECONDS = 15 * 60
MIN_TICKETS_PER_UPDATE = 5
MAX_TICKETS_PER_UPDATE = 15
MIN_PRICE_CHANGE = -50
MAX_PRICE_CHANGE = 50


def get_uuid():
    time_str = f"{int(time.time()):x}"
    padding = "".join(
        random.choices("0123456789abcdef", k=16 - len(time_str))
    )
    return time_str + padding


def generate_cabin_class():
    return random.choices(
        ["ECONOMY", "PREMIUM_ECONOMY", "BUSINESS", "FIRST"],
        weights=[85, 7, 7, 1],
        k=1,
    )[0]


def generate_baggage_allowance():
    return random.choice([True, False])


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def fetch_html(url):
    if urlsplit(url).hostname != "www.kiwi.com" or urlsplit(url).scheme != "https":
        raise ValueError("Expected an https://www.kiwi.com/ URL")
    request = Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
    })
    with urlopen(request, timeout=30) as response:
        return response.read().decode("utf-8", errors="replace")


class Page(HTMLParser):
    """Collect embedded JSON and real links without executing page JavaScript."""

    def __init__(self, html):
        super().__init__(convert_charrefs=True)
        self.scripts = []
        self.links = []
        self.script = None
        self.feed(html)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "script":
            self.script = [attrs, ""]
        elif tag == "a" and attrs.get("href"):
            self.links.append(attrs["href"])

    def handle_data(self, data):
        if self.script is not None:
            self.script[1] += data

    def handle_endtag(self, tag):
        if tag == "script" and self.script is not None:
            self.scripts.append(self.script)
            self.script = None


def walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def relay_records(page):
    """Read JSON records from Next.js text packets; never eval site code."""
    decoder = json.JSONDecoder()
    chunks = []
    for _, script in page.scripts:
        for marker in re.finditer(r"self\.__next_f\.push\(", script):
            try:
                packet, _ = decoder.raw_decode(script[marker.end():])
            except ValueError:
                continue
            if isinstance(packet, list) and len(packet) > 1 and packet[0] == 1 and isinstance(packet[1], str):
                chunks.append(packet[1])
    stream = "".join(chunks)
    records = {}
    for marker in re.finditer(r"(?:^|\n)[0-9a-f]+:", stream):
        try:
            value, _ = decoder.raw_decode(stream[marker.end():])
        except ValueError:
            continue
        for item in walk(value):
            if isinstance(item.get("__id"), str):
                records[item["__id"]] = item
    return records


def reference(records, value):
    return records.get((value or {}).get("__ref"), {})


def parse_offers(html, source_url):
    page = Page(html)
    records = relay_records(page)
    indexed = {}
    quote_times = {}
    for record in records.values():
        if record.get("__typename") == "LandingPageSearchResults":
            for key in record.get("onewaySearchResults", {}).get("__refs", []):
                quote_times[key] = record.get("updatedAt")
        if record.get("__typename") != "OnewaySearchResult":
            continue
        origin = reference(records, record.get("source")).get("code")
        destination = reference(records, record.get("destination")).get("code")
        amount = reference(records, record.get("price")).get("amount")
        if amount is not None:
            key = (origin, destination, record.get("outboundDepartureTime"), Decimal(str(amount)))
            indexed.setdefault(key, []).append(record)

    offers = {}
    for attrs, script in page.scripts:
        if attrs.get("type") != "application/ld+json":
            continue
        try:
            data = json.loads(script)
        except ValueError as error:
            raise ValueError("Invalid JSON-LD; page structure may have changed") from error
        for item in walk(data):
            if item.get("@type") != "Offer":
                continue
            link = urljoin(source_url, item.get("url", ""))
            if "/no-return/" not in urlsplit(link).path:
                continue
            trip = item.get("itemOffered", {})
            flights = trip.get("itinerary", {}).get("itemListElement", [])
            if not flights or any(f.get("@type") != "Flight" for f in flights):
                continue
            first, last = flights[0], flights[-1]
            origin = first.get("departureAirport", {}).get("iataCode")
            destination = last.get("arrivalAirport", {}).get("iataCode")
            if destination != "KSC":
                continue
            departure = first.get("departureTime")
            departure_dt = datetime.fromisoformat(departure)
            price = Decimal(str(item["price"]))
            currency = item.get("priceCurrency")
            if not origin or not price.is_finite() or price < 0 or not re.fullmatch(r"[A-Z]{3}", currency or ""):
                raise ValueError("Offer is missing valid airports, price or currency")
            matches = indexed.get((origin, destination, departure, price), [])
            # Do not merge unrelated itineraries sharing a route/date/price.
            record = matches[0] if len(matches) == 1 else {}
            arrival = record.get("outboundArrivalTime") or last.get("arrivalTime")
            arrival_dt = datetime.fromisoformat(arrival) if arrival else None
            carriers = [records.get(k, {}) for k in record.get("carriers", {}).get("__refs", [])]
            stop_refs = record.get("outboundStopovers", {}).get("__refs")
            stops = len(stop_refs) if stop_refs is not None else (0 if first.get("description") == "Nonstop (direct)" else None)
            station_codes = [records.get(k, {}).get("code") for k in (stop_refs or [])]
            offer_id = record.get("id") or item.get("@id") or link
            offers[offer_id] = {
                "origin": origin, "destination": destination,
                "departure_date": departure_dt.date().isoformat(),
                "departure_time": departure_dt.strftime("%H:%M"),
                "arrival_date": arrival_dt.date().isoformat() if arrival_dt else None,
                "arrival_time": arrival_dt.strftime("%H:%M") if arrival_dt else None,
                # These timestamps are local, without offsets. Subtraction could be wrong.
                "duration_minutes": None,
                "stops": stops, "is_direct": stops == 0 if stops is not None else None,
                "connection_airports": [code for code in station_codes if code],
                "flight_numbers": [],
                "airlines": [c["name"] for c in carriers if c.get("name")],
                "operating_airlines": [], "travel_class": None, "passengers": None,
                "seller": "Kiwi.com", "cabin_baggage_included": None,
                "checked_baggage_included": None, "checked_baggage_extra_price": None,
                "checked_baggage_max_weight_kg": None,
                "price": int(price) if price == price.to_integral_value() else float(price),
                "currency": currency, "source": "Kiwi.com", "source_url": link,
                "source_offer_id": offer_id,
                # Source has no timezone here; preserve verbatim, never append Z.
                "source_quote_at": quote_times.get(record.get("__id")),
                "scraped_at": now(),
            }
    if not offers:
        raise ValueError("No dated one-way KSC offers found; route page may be empty or changed")
    return list(offers.values())


def discover_routes(html, base_url):
    urls = []
    for link in Page(html).links:
        url = urljoin(base_url, link)
        parts = urlsplit(url)
        if parts.scheme == "https" and parts.hostname == "www.kiwi.com" and re.fullmatch(r"/[a-z-]+/cheap-flights/[^/]+/kosice-slovakia/", parts.path):
            if url not in urls:
                urls.append(url)
    return urls


def scrape(url=DEFAULT_URL, max_routes=3):
    html = fetch_html(url)
    is_city = "/city/" in urlsplit(url).path
    routes = discover_routes(html, url)[:max_routes] if is_city else [url]
    if not routes:
        raise ValueError("No inbound Košice route links found")
    offers, errors = {}, []
    for route in routes:
        try:
            if is_city:
                time.sleep(1)
            rows = parse_offers(fetch_html(route) if is_city else html, route)
            for row in rows:
                offers[row["source_offer_id"]] = row
            print(f"{len(rows)} offers: {route}", file=sys.stderr)
        except (OSError, ValueError, KeyError, TypeError, InvalidOperation) as error:
            errors.append({"url": route, "error": str(error)})
    if not offers:
        raise ValueError(f"No offers collected. Errors: {errors}")
    return {"source": "Kiwi.com", "source_url": url, "scraped_at": now(),
            "status": "partial" if errors else "success", "route_pages": routes,
            "offer_count": len(offers), "offers": list(offers.values()), "errors": errors}


def prepare_tickets(offers, current_time=None):
    if not isinstance(offers, list):
        return None
    now_value = current_time or datetime.now(timezone.utc)
    if now_value.tzinfo is None:
        now_value = now_value.replace(tzinfo=timezone.utc)

    tickets = []
    try:
        for offer in offers:
            if not isinstance(offer, dict):
                return None
            departure_date = datetime.fromisoformat(
                offer["departure_date"]
            ).date()
            capacity = random.randint(2, 10) * 50
            ticket = dict(offer)
            ticket.update({
                "ticket_id": get_uuid(),
                "total_capacity": capacity,
                "seats_remaining": random.randint(1, capacity // 2),
                "status": "active" if departure_date >= now_value.date() else "expired",
                "travel_class": (
                    offer.get("travel_class") or generate_cabin_class()
                ),
                "baggage_allowance": generate_baggage_allowance(),
            })
            tickets.append(ticket)
        return tickets
    except (KeyError, TypeError, ValueError, OverflowError):
        return None


def bruno_ticket_payload(ticket):
    try:
        departure = datetime.fromisoformat(
            f"{ticket['departure_date']}T{ticket['departure_time']}:00"
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        arrival_date = ticket.get("arrival_date")
        arrival_time = ticket.get("arrival_time")
        arrival = (
            datetime.fromisoformat(
                f"{arrival_date}T{arrival_time}:00"
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
            if arrival_date and arrival_time else None
        )
        airlines = ticket.get("airlines")
        return {
            "ticket_id": ticket["ticket_id"],
            "scrape_timestamp": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "source_website": "Kiwi.com",
            "departure_airport": ticket.get("origin"),
            "destination_airport": ticket.get("destination"),
            "departure_date": departure,
            "destination_date": arrival,
            "airline_name": airlines[0] if airlines else None,
            "price": ticket.get("price"),
            "currency": ticket.get("currency"),
            "number_of_stops": ticket.get("stops"),
            "cabin_class": ticket.get("travel_class") or generate_cabin_class(),
            "baggage_allowance": ticket.get("baggage_allowance")
            if ticket.get("baggage_allowance") is not None
            else (
                ticket.get("checked_baggage_included")
                if ticket.get("checked_baggage_included") is not None
                else (
                    ticket.get("cabin_baggage_included")
                    if ticket.get("cabin_baggage_included") is not None
                    else generate_baggage_allowance()
                )
            ),
            "total_capacity": ticket["total_capacity"],
            "seats_remaining": ticket["seats_remaining"],
            "status": ticket["status"],
        }
    except (KeyError, TypeError, ValueError):
        return None


def publish_tickets(topic, tickets):
    if not isinstance(tickets, list):
        return None
    if not tickets:
        return True
    mqtt_username = os.environ.get("MQTT_USERNAME")
    mqtt_password = os.environ.get("MQTT_PASSWORD")
    if not mqtt_username or not mqtt_password:
        print(
            "MQTT_USERNAME and MQTT_PASSWORD must be set in this process environment.",
            file=sys.stderr,
        )
        return None
    try:
        import paho.mqtt.publish as publish
        from paho.mqtt import MQTTException
    except ImportError:
        print("MQTT publishing requires the optional paho-mqtt package.", file=sys.stderr)
        return None

    try:
        publish.single(
            topic=topic,
            payload=json.dumps(tickets),
            hostname=MQTT_IP,
            port=int(MQTT_PORT),
            client_id=MQTT_ID,
            auth={"username": mqtt_username, "password": mqtt_password},
        )
        return True
    except (OSError, MQTTException, TypeError, ValueError) as error:
        print(f"MQTT publish failed for {topic}: {error}", file=sys.stderr)
        return None


def load_ticket_pool():
    try:
        with POOL_FILE.open(encoding="utf-8") as pool_file:
            tickets = json.load(pool_file)
    except FileNotFoundError:
        return []
    except (OSError, ValueError, TypeError):
        return None
    if isinstance(tickets, list) and all(isinstance(ticket, dict) for ticket in tickets):
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
            json.dump(tickets, pool_file, ensure_ascii=False, indent=2)
            pool_file.write("\n")
        os.replace(temporary_file, POOL_FILE)
        return True
    except (OSError, TypeError, ValueError):
        try:
            temporary_file.unlink(missing_ok=True)
        except OSError:
            pass
        return None


def update_ticket_pool(tickets, current_time=None):
    try:
        now_value = current_time or datetime.now(timezone.utc)
        if now_value.tzinfo is None:
            now_value = now_value.replace(tzinfo=timezone.utc)
        active_tickets = [
            ticket for ticket in tickets if ticket.get("status") == "active"
        ]
        if not active_tickets:
            return [], []

        maximum = min(MAX_TICKETS_PER_UPDATE, len(active_tickets))
        minimum = min(MIN_TICKETS_PER_UPDATE, maximum)
        selected = random.sample(
            active_tickets, random.randint(minimum, maximum)
        )
        updates = []
        for ticket in selected:
            departure_date = datetime.fromisoformat(
                ticket["departure_date"]
            ).date()
            seats = ticket.get("seats_remaining")
            price = ticket.get("price")
            if not isinstance(seats, int) or isinstance(seats, bool) or seats < 0:
                return None
            if (
                not isinstance(price, (int, float))
                or isinstance(price, bool)
                or not math.isfinite(price)
            ):
                return None

            if seats:
                seats = max(0, seats - random.randint(5, 50))
            price = round(
                max(price, 200)
                + random.randint(MIN_PRICE_CHANGE * 100, MAX_PRICE_CHANGE * 100) / 100,
                2,
            )
            if departure_date < now_value.date():
                ticket["status"] = "expired"
            if seats == 0:
                ticket["status"] = "sold_out"
            ticket["price"] = price
            ticket["seats_remaining"] = seats
            updates.append({
                "ticket_id": ticket["ticket_id"],
                "update_timestamp": now_value.astimezone(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "price": price,
                "seats_remaining": seats,
                "status": ticket["status"],
            })
        remaining = [
            ticket for ticket in active_tickets if ticket["status"] == "active"
        ]
        return updates, remaining
    except (AttributeError, KeyError, TypeError, ValueError, OverflowError):
        return None


def run_update_cycle(url=DEFAULT_URL, max_routes=3):
    print("Starting Kiwi ticket update cycle.")
    ticket_pool = load_ticket_pool()
    if ticket_pool is None:
        print("Load ticket pool: FAILED")
        return False
    print(f"Load ticket pool: OK ({len(ticket_pool)} tickets)")

    result = update_ticket_pool(ticket_pool)
    if result is None:
        print("Update ticket pool: FAILED")
        return False
    updates, ticket_pool = result
    publish_result = publish_tickets("TUKE/DI/04/ticket_update", updates)
    print(f"Publish ticket updates: {'OK' if publish_result is not None else 'FAILED'}")
    cycle_succeeded = publish_result is not None

    if len(ticket_pool) < POOL_TARGET_SIZE:
        print(f"Check ticket pool size: REFILL NEEDED ({len(ticket_pool)} below {POOL_TARGET_SIZE})")
        try:
            result = scrape(url, max_routes)
            new_tickets = prepare_tickets(result["offers"])
        except (OSError, ValueError, KeyError, TypeError, InvalidOperation) as error:
            print(f"Fetch Kiwi offers: FAILED ({error})", file=sys.stderr)
            new_tickets = None
        if new_tickets is None:
            cycle_succeeded = False
        else:
            existing_ids = {
                ticket.get("source_offer_id") for ticket in ticket_pool
            }
            new_tickets = [
                ticket for ticket in new_tickets
                if ticket.get("source_offer_id") not in existing_ids
                and ticket["status"] == "active"
            ]
            print(f"Fetch Kiwi offers: OK ({len(new_tickets)} new tickets)")
            # Keep scraped tickets locally even if the broker is unavailable.
            ticket_pool.extend(new_tickets)
            mqtt_tickets = [bruno_ticket_payload(ticket) for ticket in new_tickets]
            if any(ticket is None for ticket in mqtt_tickets):
                print("Format new tickets for MQTT: FAILED", file=sys.stderr)
                cycle_succeeded = False
                mqtt_tickets = []
                publish_result = None
            else:
                print("Format new tickets for MQTT: OK")
                publish_result = publish_tickets(
                    "TUKE/DI/04/ticket_new", mqtt_tickets
                )
            print(f"Publish new tickets: {'OK' if publish_result is not None else 'FAILED'}")
            if publish_result is None:
                cycle_succeeded = False
    else:
        print(f"Check ticket pool size: OK ({len(ticket_pool)} meets target)")

    save_result = save_ticket_pool(ticket_pool)
    print(f"Save ticket pool: {'OK' if save_result else 'FAILED'}")
    if save_result is None:
        cycle_succeeded = False
    print(
        "Kiwi ticket update cycle: "
        + ("COMPLETE" if cycle_succeeded else "COMPLETED WITH FAILURES")
    )
    return cycle_succeeded


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_URL, help="Kiwi Košice city page or an inbound route page")
    parser.add_argument("--max-routes", type=int, default=3, help="Maximum discovered route pages (default 3)")
    parser.add_argument("--output", type=Path, default=Path("kiwi-offers.json"))
    parser.add_argument(
        "--mqtt", action="store_true",
        help="Run periodic MQTT ticket updates instead of the one-shot JSON export",
    )
    args = parser.parse_args()
    if not 1 <= args.max_routes <= 20:
        parser.error("--max-routes must be between 1 and 20")
    if args.mqtt:
        while True:
            run_update_cycle(args.url, args.max_routes)
            time.sleep(UPDATE_INTERVAL_SECONDS)
    try:
        result = scrape(args.url, args.max_routes)
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except (OSError, ValueError) as error:
        print(f"Scrape failed: {error}", file=sys.stderr)
        return 1
    print(f"Saved {result['offer_count']} offers to {args.output} ({result['status']})")
    return 2 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
