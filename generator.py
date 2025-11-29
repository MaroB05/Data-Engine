from azure.eventhub import EventHubProducerClient, EventData
import json
import random
import time
from datetime import datetime, timezone
import os 
from dotenv import load_dotenv

load_dotenv()

EVENT_HUB_CONNECTION_STR = os.getenv('EVENT_HUB_CONNECTION_STR')
EVENT_HUB_NAME = os.getenv('EVENT_HUB_NAME')

LOCATIONS = ["Cairo Downtown", "Nasr City", "Alex Desert Road", "Ring Road", "Giza Square"]
VEHICLE_TYPES = ["Car", "Bus", "Truck", "Motorcycle"]

def generate_traffic_data():
    """Generates a dictionary with simulated traffic metrics."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "location": random.choice(LOCATIONS),
        "vehicle_type": random.choice(VEHICLE_TYPES),
        "vehicle_count": random.randint(1, 5),
        "average_speed": round(random.uniform(20.0, 120.0), 2)
    }

def main():
    
    print(f"Connecting to Event Hub: {EVENT_HUB_NAME}...")
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME  
    )
    
    print("🚦 Starting traffic simulation...")

    try:
        while True:
            data = generate_traffic_data()
            event_data = EventData(json.dumps(data))

            
            batch = producer.create_batch()
            batch.add(event_data)

            producer.send_batch(batch)

            print(f"Sent: {data}")
            time.sleep(2)

    except KeyboardInterrupt:
        print("\nStopped by user.")
    except Exception as e:
        print(f"\nAn error occurred: {e}")

    finally:
        print("Closing producer client.")
        producer.close()

if __name__ == "__main__":
    main()
