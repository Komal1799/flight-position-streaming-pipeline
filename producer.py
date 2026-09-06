import os
import json
import time
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID")
CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET")
TOKEN_REFRESH_MARGIN = 30

KAFKA_TOPIC = "flight-positions"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Field names in the order OpenSky returns them
FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude",
    "squawk", "spi", "position_source"
]


class TokenManager:
    def __init__(self):
        self.token = None
        self.expires_at = None

    def get_token(self):
        if self.token and self.expires_at and datetime.now() < self.expires_at:
            return self.token
        return self._refresh()

    def _refresh(self):
        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
        )
        r.raise_for_status()
        data = r.json()
        self.token = data["access_token"]
        expires_in = data.get("expires_in", 1800)
        self.expires_at = datetime.now() + timedelta(seconds=expires_in - TOKEN_REFRESH_MARGIN)
        return self.token

    def headers(self):
        return {"Authorization": f"Bearer {self.get_token()}"}


def delivery_report(err, msg):
    """Called once for each message to confirm delivery or report failure."""
    if err is not None:
        print(f"Delivery failed: {err}")
    # We deliberately don't print every success — 10,000+ messages would flood the console


def fetch_and_publish(tokens, producer):
    response = requests.get(
        "https://opensky-network.org/api/states/all",
        headers=tokens.headers(),
    )
    response.raise_for_status()
    data = response.json()
    states = data.get("states", [])

    for state in states:
        # Convert the fixed-position array into a labeled dictionary
        record = dict(zip(FIELDS, state))

        # Skip records with no position data — can't do much with these downstream
        if record["longitude"] is None or record["latitude"] is None:
            continue

        producer.produce(
            KAFKA_TOPIC,
            key=record["icao24"],  # partitioning key — same aircraft always maps consistently
            value=json.dumps(record),
            callback=delivery_report,
        )

    producer.poll(0)  # trigger delivery callbacks
    producer.flush()  # block until all messages are sent
    print(f"Published {len(states)} aircraft states at {datetime.now()}")


if __name__ == "__main__":
    tokens = TokenManager()
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    # Poll every 15 seconds — respects OpenSky's rate limits and gives Spark
    # a steady stream instead of one giant burst
    while True:
        try:
            fetch_and_publish(tokens, producer)
        except Exception as e:
            print(f"Error during fetch/publish: {e}")
        time.sleep(15)