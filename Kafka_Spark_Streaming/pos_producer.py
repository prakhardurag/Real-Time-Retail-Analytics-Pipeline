import json
import random
import time
import logging
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

fake = Faker()

# Kafka Config
KAFKA_BROKER = "kafka:9092"
TOPIC = "transactions"

# Product Catalog (15 total)
PRODUCTS = [
    {"product_id": 101, "name": "iPhone 15", "category": "Electronics", "price": 999.99},
    {"product_id": 102, "name": "Samsung QLED TV", "category": "Electronics", "price": 1499.99},
    {"product_id": 103, "name": "PlayStation 5", "category": "Gaming", "price": 499.99},
    {"product_id": 104, "name": "Dell XPS Laptop", "category": "Computers", "price": 1299.99},
    {"product_id": 105, "name": "Sony Headphones", "category": "Audio", "price": 199.99},
    {"product_id": 106, "name": "Amazon Echo", "category": "Smart Home", "price": 99.99},
    {"product_id": 107, "name": "Google Nest Hub", "category": "Smart Home", "price": 129.99},
    {"product_id": 108, "name": "Canon EOS M50", "category": "Cameras", "price": 649.99},
    {"product_id": 109, "name": "Bose SoundLink", "category": "Audio", "price": 179.99},
    {"product_id": 110, "name": "Apple Watch", "category": "Wearables", "price": 399.99},
    {"product_id": 111, "name": "Fitbit Charge 5", "category": "Wearables", "price": 149.99},
    {"product_id": 112, "name": "Nintendo Switch", "category": "Gaming", "price": 299.99},
    {"product_id": 113, "name": "MacBook Air", "category": "Computers", "price": 999.99},
    {"product_id": 114, "name": "Lenovo ThinkPad", "category": "Computers", "price": 849.99},
    {"product_id": 115, "name": "GoPro Hero10", "category": "Cameras", "price": 499.99}
]

# Store list
STORES = [
    {"store_id": 1, "location": "New York"},
    {"store_id": 2, "location": "Los Angeles"},
    {"store_id": 3, "location": "Chicago"},
    {"store_id": 4, "location": "Houston"},
    {"store_id": 5, "location": "Phoenix"}
]

# Payment methods
PAYMENT_METHODS = ["Credit Card", "Debit Card", "Cash", "Mobile Payment", "Gift Card"]

def create_producer(bootstrap_servers, retries=5, delay=5):
    """Create Kafka producer with retry logic."""
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks='all',
                retries=3,
                compression_type='gzip'
            )
            logging.info("Successfully connected to Kafka broker")
            return producer
        except NoBrokersAvailable as e:
            logging.warning(f"Attempt {attempt + 1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                logging.error("Failed to connect to Kafka after retries")
                raise
        except Exception as e:
            logging.error(f"Unexpected error during producer creation: {e}")
            raise

def generate_transaction():
    """Generate a transaction, with 20% chance of corruption."""
    product = random.choice(PRODUCTS)
    store = random.choice(STORES)
    payment_method = random.choice(PAYMENT_METHODS)
    quantity = random.randint(1, 5)

    # Simulate corruption in 20% of records
    if random.random() < 0.02:
        corruption_type = random.choice(["missing_field", "null_field", "invalid_type"])
        if corruption_type == "missing_field":
            # Randomly skip one field (e.g., transaction_id or payment_method)
            fields = ["transaction_id", "payment_method"]
            missing_field = random.choice(fields)
            txn = {
                "product_id": product["product_id"],
                "store_id": store["store_id"],
                "quantity": quantity,
                "price": round(product["price"] * quantity, 2),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            if missing_field != "transaction_id":
                txn["transaction_id"] = fake.uuid4()
            if missing_field != "payment_method":
                txn["payment_method"] = payment_method
            return txn
        elif corruption_type == "null_field":
            return {
                "transaction_id": None,
                "product_id": product["product_id"],
                "store_id": store["store_id"],
                "quantity": None,
                "price": round(product["price"] * quantity, 2),
                "payment_method": None,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        elif corruption_type == "invalid_type":
            return {
                "transaction_id": fake.uuid4(),
                "product_id": str(product["product_id"]),  # String instead of int
                "store_id": store["store_id"],
                "quantity": "invalid",  # Invalid type
                "price": -round(product["price"] * quantity, 2),  # Negative price
                "payment_method": payment_method,
                "timestamp": "2023-13-45T99:99:99"  # Invalid timestamp
            }

    # Normal clean record
    return {
        "transaction_id": fake.uuid4(),
        "product_id": product["product_id"],
        "store_id": store["store_id"],
        "quantity": quantity,
        "price": round(product["price"] * quantity, 2),
        "payment_method": payment_method,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def main():
    """Main loop to generate and send transactions."""
    logging.info("Starting POS event simulator with dirty data...")
    try:
        producer = create_producer(KAFKA_BROKER)
        while True:
            txn = generate_transaction()
            try:
                producer.send(TOPIC, value=txn).get(timeout=10)  # Synchronous send
                logging.info(f"Sent: {txn}")
            except Exception as e:
                logging.error(f"Failed to send: {e}")
            time.sleep(random.uniform(0.5, 1))
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        producer.flush()
        producer.close()
    except Exception as e:
        logging.error(f"Unexpected error in main loop: {e}")
        raise

if __name__ == "__main__":
    main()