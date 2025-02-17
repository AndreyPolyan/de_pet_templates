import redis
import json
import uuid
import random
import logging
from faker import Faker
from datetime import datetime, timedelta, UTC

# Configure logging to stdout for Docker
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# Initialize Faker
fake = Faker("ru_RU")

# Connect to Redis
try:
    redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)
    redis_client.ping()
    log.info("Connected to Redis successfully.")
except redis.ConnectionError as e:
    log.error("Failed to connect to Redis. Ensure Redis is running.")
    raise e

# Check if data is already loaded
if redis_client.exists("data_initialized"):
    log.info("Data is already loaded. Skipping initialization...")
else:
    # Generate 100 users
    users = {}
    for _ in range(100):
        user_id = str(uuid.uuid4().hex[:24])
        users[user_id] = json.dumps({
            "_id": user_id,
            "name": fake.name(),
            "login": fake.user_name(),
            "update_ts_utc": (datetime.now(UTC) - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d %H:%M:%S")
        })

    # Save users to Redis
    for user_id, user_data in users.items():
        try:
            redis_client.set(user_id, user_data)
            log.info(f"User successfully loaded. ID: {user_id}")
        except Exception as e:
            log.error(f"Error while loading user: {user_id}. Exception: {e}")
            raise e

    log.info("SUCCESS: 100 Users Loaded into Redis.")

    # Generate 10 restaurants with menus
    restaurants = {}
    for _ in range(10):
        restaurant_id = str(uuid.uuid4().hex[:24])
        menu = [
            {
                "_id": str(uuid.uuid4().hex[:24]),
                "name": fake.word(),
                "price": random.randint(100, 1000),
                "category": random.choice(["Закуски", "Супы", "Основные блюда", "Гарниры", "Напитки"])
            }
            for _ in range(random.randint(5, 15))  # Each restaurant has 5-15 menu items
        ]
        restaurants[restaurant_id] = json.dumps({
            "_id": restaurant_id,
            "name": fake.company(),
            "menu": menu,
            "update_ts_utc": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
        })

    # Save restaurants to Redis
    for restaurant_id, restaurant_data in restaurants.items():
        try:
            redis_client.set(restaurant_id, restaurant_data)
            log.info(f"Restaurant successfully loaded. ID: {restaurant_id}")
        except Exception as e:
            log.error(f"Error while loading restaurant: {restaurant_id}. Exception: {e}")
            raise e

    log.info("SUCCESS: 10 Restaurants Loaded into Redis.")


    redis_client.set("data_initialized", "true")
    log.info("Data initialization completed successfully.")