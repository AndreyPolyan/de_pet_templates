import random
import datetime
import uuid
from typing import Dict, List
from lib.kafka_connect import KafkaProducer
from lib.redis import RedisClient

class StgMessageGenerator:
    def __init__(self, kafka_producer: KafkaProducer, redis_client: RedisClient, logger) -> None:
        self._producer = kafka_producer
        self._redis = redis_client
        self._logger = logger
        self._users = {}  # Store users
        self._restaurants = {}  # Store restaurants

        self._load_data()

    def _load_data(self):
        """Load users and restaurants from Redis."""
        self._logger.info("Loading data from Redis...")
        self._users.clear()
        self._restaurants.clear()

        keys = self._redis._client.keys("*")  # Get all keys in Redis
        for key in keys:
            value = self._redis.get(key)
            if isinstance(value, dict):
                if "login" in value:  # Identify users
                    self._users[key] = value
                elif "menu" in value:  # Identify restaurants
                    self._restaurants[key] = value

        self._logger.info(f"Loaded {len(self._users)} users and {len(self._restaurants)} restaurants.")
    
    def _generate_order(self) -> Dict:
        """Simulate an order based on available users and restaurants."""
        if not self._users or not self._restaurants:
            self._logger.error("No users or restaurants available for order generation.")
            return {}

        # Select a random user and restaurant
        user_key, user = random.choice(list(self._users.items()))
        restaurant_key, restaurant = random.choice(list(self._restaurants.items()))

        # Select 1-3 random menu items
        if not restaurant.get("menu"):
            self._logger.error(f"Restaurant {restaurant['id']} has no menu.")
            return {}

        order_items = random.sample(restaurant["menu"], k=random.randint(1, 3))
        order_items = [dict(item) for item in order_items] # Guarantee we have new copy of products
        for item in order_items:
            item['id'] = item.pop('_id')
            item["quantity"] = random.randint(1, 5)  # Assign random quantity

        # Calculate order cost
        total_cost = sum(item["price"] * item["quantity"] for item in order_items)

        # Generate order statuses
        order_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=random.randint(10, 120))
        statuses = [
            {"status": "OPEN", "dttm": (order_time + datetime.timedelta(minutes=5)).isoformat()},
            {"status": "COOKING", "dttm": (order_time + datetime.timedelta(minutes=20)).isoformat()},
            {"status": "DELIVERING", "dttm": (order_time + datetime.timedelta(minutes=40)).isoformat()},
            {"status": "CLOSED", "dttm": (order_time + datetime.timedelta(minutes=60)).isoformat()},
        ]

        return {
            "object_id": uuid.uuid4().int % (10**7),  # Random 7-digit ID
            "object_type": "order",
            "sent_dttm": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "payload": {
                "restaurant": {"id": restaurant["_id"]},
                "date": order_time.isoformat(),
                "user": {"id": user["_id"]},
                "order_items": order_items,
                "bonus_payment": 0,
                "cost": total_cost,
                "payment": total_cost,
                "bonus_grant": 0,
                "statuses": statuses,
                "final_status": "CLOSED",
                "update_ts": statuses[-1]["dttm"],
            }
        }
    def push_messages(self, n: int) -> None:
        """Generate and send N messages to Kafka."""
        self._load_data()  # Reload users and restaurants before generating messages
        for _ in range(n):
            order = self._generate_order()
            if order:
                self._producer.produce(order)
                self._logger.info(f"Order {order['object_id']} sent to Kafka.")