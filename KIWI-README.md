# Kiwi flight scraper

Requires Python 3.10+ and internet. The one-shot scraper needs no additional packages; MQTT mode optionally requires `paho-mqtt`.

```sh
python kiwi_scraper.py --output kiwi-offers.json
```

By default it reads the Košice city page, discovers real inbound route links, and reads up to three route pages at one-second intervals. It extracts dated one-way flight offers, not the city's generic minimum-price cards. It excludes return offers and destinations other than KSC.

To collect from more discovered routes:

```sh
python kiwi_scraper.py --max-routes 9 --output kiwi-offers.json
```

To test only a particular route:

```sh
python kiwi_scraper.py --url "https://www.kiwi.com/en/cheap-flights/prague-czechia/kosice-slovakia/" --output kiwi-prague.json
```

## MQTT updates

Run the Bruno-style periodic ticket pool and MQTT publishing cycle with:

```sh
python -m pip install paho-mqtt
python kiwi_scraper.py --mqtt
```

Set `MQTT_USERNAME` and `MQTT_PASSWORD` in the same process environment used to start the scraper. In PowerShell:

```powershell
$env:MQTT_USERNAME = "your-broker-username"
$env:MQTT_PASSWORD = "your-broker-password"
python kiwi_scraper.py --mqtt
```

Optional settings are `MQTT_IP` (default `178.143.18.120`), `MQTT_PORT` (default `1883`), `MQTT_ID` (default `di-kiwi-scraper`), and `KIWI_TICKET_POOL_FILE` (default `kiwi_ticket_pool.json` next to the script). The cycle publishes price/seat updates to `TUKE/DI/04/ticket_update` and newly discovered tickets to `TUKE/DI/04/ticket_new`, then waits 15 minutes before repeating. New-ticket MQTT messages use Bruno's field names and shape (`ticket_id`, `scrape_timestamp`, `source_website`, airport/date fields, airline, price, stops, cabin/baggage, capacity, seats and status); unavailable Kiwi details are `null`. Newly scraped active tickets are saved locally even if MQTT publishing fails, so a broker issue will not leave the pool empty.

MQTT mode attaches generated capacity and remaining-seat values to offers, chooses a synthetic cabin class (mostly Economy) and baggage-allowance flag when Kiwi does not supply them, and simulates periodic price/seat changes. These are generated values, not inventory or fare conditions reported by Kiwi.com. Ticket IDs use Bruno's 16-character time-plus-hex format. The regular one-shot command and its JSON schema are unchanged.

## SQLite MQTT subscriber

Install the MQTT client and environment-file support, then start the database manager in a second terminal:

```sh
python -m pip install paho-mqtt python-dotenv
python dbmanigger.py
```

It loads the same broker host, port, and credentials from environment variables or `.env`, subscribes to the flight topics `TUKE/DI/04/ticket_new` and `TUKE/DI/04/ticket_update` and theatre topics `theatre_new` and `theatre_update`, and stores data in `tickets.db` beside the script. Flight tickets and updates are stored separately in `ticket_new` and `ticket_update`; theatre tickets and updates are stored in `theatre_new` and `theatre_update`. Theatre list/object fields (`categories`, `price_categories`, `source_urls`) are JSON-encoded in SQLite text columns. Date/time columns use SQLite's `TIMESTAMP` type and preserve incoming ISO-8601 timestamp strings. Update rows are inserted only for IDs already present in their matching `*_new` table; unknown IDs are ignored. Existing flight databases are migrated to the timestamp schema when the manager starts. Set `TICKET_DATABASE_FILE` to change the database path. `DB_MQTT_ID` optionally changes the subscriber client ID; keep it different from the scraper's ID so the broker does not disconnect either client.

On the current project computer, replace `python` with:

```powershell
& 'C:\Users\burzon\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
```

## Output

The JSON has an `offers` array using the same field names as the Skyscanner scraper. Populated fields include route, departure/arrival dates and local times when available, airline, stop count, advertised price, currency and search link. The two embedded data sources are joined only when airports, departure datetime and price match uniquely.

Duration, flight numbers, operating airlines, cabin class, passenger count and baggage remain unknown where not explicitly provided. Airport-local timestamps have no timezone offset; the scraper does not subtract them to guess duration. Empty arrays indicate unavailable details as well as empty lists.

`source_quote_at` preserves Kiwi's published search-results update timestamp, which has no timezone in the inspected source. `scraped_at` is collection time in UTC. Some quotes can be weeks old. Listings are advertised examples, not exhaustive live inventory or confirmed availability.

`route_pages` lists pages attempted; `errors` records failures. Exit code 0 means success, 1 means no usable result or fatal error, and 2 means partial results were saved. A successful/partial run replaces the selected output file. A total scrape failure leaves any existing output untouched.

## Test

```sh
python -m unittest test_kiwi_scraper.py
```

Tests cover embedded packet decoding, matching, deduplication, excluding return/wrong-destination offers, missing data, and route-link filtering. The actual website may change; parse failures are reported instead of producing an empty successful result.
