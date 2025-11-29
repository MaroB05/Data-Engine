from azure.eventhub import EventHubProducerClient, EventData
import json
import random
import time
from datetime import datetime, timezone

# ----------------- CONFIG -----------------
EVENT_HUB_CONNECTION_STR = "Endpoint=sb://depi-traffic-stream.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=jZ8S8XpijKQNdmZiiJ/2HnniiBbqf8edY+AEhBSOmD8="
EVENT_HUB_NAME = "traffic-input"

LOCATIONS = ["Ring Road", "Nasr City", "Giza Square", "Alex Desert Road", "Cairo Downtown"]
VEHICLE_TYPES = ["Car", "Bus", "Truck", "Motorcycle"]

GENERATE_INTERVAL = 5              # seconds between events
HEAVY_DURATION_REPORTS = 10        # heavy traffic holds one area for 10 reports
ACCIDENT_DURATION_REPORTS = 5      # accident holds one area for 5 reports
HEAVY_VEHICLES_RANGE = (30, 80)
ACCIDENT_VEHICLES_RANGE = (2, 6)
NORMAL_VEHICLES_RANGE = (5, 25)
EMPTY_VEHICLES_RANGE = (1, 6)
EMPTY_PROB = 0.06                  # 6% chance of empty high-speed scenario

# ----------------- STATE TRACKING ------------------
report_counter = 0
heavy_location = None
heavy_remaining = 0
accident_location = None
accident_remaining = 0

# ---------------------------------------------------

def pick_new_location(exclude=None):
    """Pick location different from exclude if provided."""
    choices = [l for l in LOCATIONS if l != exclude] if exclude else LOCATIONS[:]
    return random.choice(choices)

def generate_event():
    global report_counter, heavy_location, heavy_remaining, accident_location, accident_remaining
    report_counter += 1

    # Start heavy cluster
    if heavy_remaining == 0 and random.random() < 0.08:
        heavy_location = pick_new_location(exclude=accident_location)
        heavy_remaining = HEAVY_DURATION_REPORTS

    # Start accident cluster
    if accident_remaining == 0 and random.random() < 0.05:
        accident_location = pick_new_location(exclude=heavy_location)
        accident_remaining = ACCIDENT_DURATION_REPORTS

    # Select where current event will occur
    if heavy_remaining > 0 and accident_remaining > 0:
        location = random.choices([heavy_location, accident_location] + LOCATIONS,
                                  weights=[0.30, 0.15] + [0.55/len(LOCATIONS)]*len(LOCATIONS))[0]
    elif heavy_remaining > 0:
        location = random.choices([heavy_location] + LOCATIONS,
                                  weights=[0.45] + [0.55/len(LOCATIONS)]*len(LOCATIONS))[0]
    elif accident_remaining > 0:
        location = random.choices([accident_location] + LOCATIONS,
                                  weights=[0.35] + [0.65/len(LOCATIONS)]*len(LOCATIONS))[0]
    else:
        location = random.choice(LOCATIONS)

    # ---------------- EVENT GENERATION ----------------

    # Accident → few vehicles + very slow
    if accident_remaining > 0 and location == accident_location:
        average_speed = round(random.uniform(3.0, 8.0), 2)
        vehicle_count = random.randint(*ACCIDENT_VEHICLES_RANGE)
        accident_remaining -= 1

    # Heavy traffic → many vehicles + crawling speed
    elif heavy_remaining > 0 and location == heavy_location:
        average_speed = round(random.uniform(1.0, 10.0), 2)
        vehicle_count = random.randint(*HEAVY_VEHICLES_RANGE)
        heavy_remaining -= 1

    else:
        # Sometimes road is empty → very fast speeds
        if random.random() < EMPTY_PROB:
            average_speed = round(random.uniform(120.0, 160.0), 2)
            vehicle_count = random.randint(*EMPTY_VEHICLES_RANGE)
        else:
            average_speed = round(random.uniform(40.0, 100.0), 2)
            vehicle_count = random.randint(*NORMAL_VEHICLES_RANGE)

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "location": location,
        "vehicle_type": random.choice(VEHICLE_TYPES),
        "vehicle_count": vehicle_count,
        "average_speed": average_speed
    }

# ---------------------------------------------------

def main():
    print(f"🟢 Connecting to Event Hub '{EVENT_HUB_NAME}'...")
    producer = EventHubProducerClient.from_connection_string(EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME)
    print("🚦 Streaming Live Traffic Data — Ctrl+C to stop")

    try:
        while True:
            event = generate_event()
            batch = producer.create_batch()
            batch.add(EventData(json.dumps(event)))
            producer.send_batch(batch)

            print(event)
            time.sleep(GENERATE_INTERVAL)

    except KeyboardInterrupt:
        print("\n🟥 Stream stopped manually")
    finally:
        producer.close()
        print("🔌 Producer connection closed")

if __name__ == "__main__":
    main()
