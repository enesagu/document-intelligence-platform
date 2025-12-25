import time
import sys

def run_pipeline():
    print("Extractor Service Initialized.")
    # In a real worker scenario, this would poll a queue (Redis/RabbitMQ/Kafka)
    # For this demo, it just stays alive.
    while True:
        time.sleep(60)

if __name__ == "__main__":
    run_pipeline()
