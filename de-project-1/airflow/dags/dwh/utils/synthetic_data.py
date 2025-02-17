import datetime
import random
from bson import ObjectId
from typing import List
import numpy as np
from pymongo import MongoClient
from faker import Faker
from lib.pg_connect import PgConnect
from .dict_util import json2str

fake = Faker("ru_RU")

def generate_user():
    now = datetime.datetime.now().replace(microsecond=0)
    return {
        "_id": ObjectId(),
        "name": fake.name(),
        "login": fake.user_name(),
        "registration_ts": now - datetime.timedelta(days = random.randint(1, 100)),
        "update_ts": now
    }

def generate_restaurant():
    now = datetime.datetime.now().replace(microsecond=0)
    menu_items = [
        {
            "_id": ObjectId(),
            "name": f"Dish_{i}",
            "price": round(random.uniform(10, 100), 2),
            "category": random.choice(["Starter", "Main", "Dessert", "Drink"])
        }
        for i in range(random.randint(20,200))
    ]
    return {
        "_id": ObjectId(),
        "name": f"Restaurant_{random.randint(1, 50)}",
        "menu": menu_items,
        "update_ts": now
    }

def generate_random_timestamps_with_gap(init_date, n, min_gap_minutes=10):
    # Define the end of the range
    end_date = init_date + datetime.timedelta(hours=2)
    
    # Total time in seconds available
    total_seconds = (end_date - init_date).total_seconds()
    min_gap_seconds = min_gap_minutes * 60
    
    # Ensure enough space for timestamps with the required gap
    if n * min_gap_seconds > total_seconds:
        raise ValueError("Not enough time range to generate timestamps with the required gap.")
    
    # Generate timestamps
    timestamps = []
    current_time = init_date
    for _ in range(n):
        # Calculate the maximum time for this timestamp
        max_offset = (end_date - current_time - datetime.timedelta(seconds=(n - len(timestamps) - 1) * min_gap_seconds)).total_seconds()
        random_offset = random.uniform(0, max_offset)
        current_time += datetime.timedelta(seconds=random_offset)
        timestamps.append(current_time)
        
        # Add the minimum gap
        current_time += datetime.timedelta(seconds=min_gap_seconds)
    
    return timestamps

def generate_order(existing_restaurants: List, existing_users: List):
    now = datetime.datetime.now().replace(microsecond=0)
    user = random.choice(existing_users)
    restaurant = random.choice(existing_restaurants)

    products = random.sample(restaurant['menu'], random.randint(1,10))

    statuses = ['CREATED', 'ACCEPTED', 'COOKING','DELIVERING','CLOSED', 'CANCELLED']

    final_status = str(np.random.choice(statuses[-2:], 1, p = [0.9,0.1])[0])

    if final_status == 'CANCELLED':
        working_statuses = statuses[:random.randint(1,4)] + [final_status]
    else:
        working_statuses = statuses[:-1]

    order_date = max([now - datetime.timedelta(days = random.randint(1, 100)), user['registration_ts']])
    statuses_ts = generate_random_timestamps_with_gap(order_date, len(working_statuses))

    order_items = [
        {
            "id": product['_id'],
            "name": product['name'],
            "price": product['price'],
            "quantity": random.randint(1, 5)
        }
        for product in products
    ]
    order_system_order = {
        "_id": ObjectId(),
        "restaurant": {"id": restaurant["_id"]},  
        "date": order_date,
        "user": {"id": user["_id"]}, 
        "order_items": order_items,
        "cost": sum(item["price"] * item["quantity"] for item in order_items),
        "statuses": [
            {"status": working_statuses[i], "dttm":statuses_ts[i] } for i in range(len(working_statuses))
        ],
        "final_status": final_status,
        "update_ts": now
    }
    bonussystem_payment = {
        "user_id": random.randint(1, 1000),  
        "order_id": str(order_system_order["_id"]),
        "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
        "product_payments": []
    }

    for item in order_items:
        product_cost = item["price"] * item["quantity"]
        bonus_payment = round(random.uniform(0, product_cost), 2)  
        bonus_grant = round(product_cost * 0.05)  

        bonussystem_payment["product_payments"].append({
            "product_id": str(item["id"]),
            "product_name": item["name"],
            "price": item["price"],
            "quantity": item["quantity"],
            "product_cost": product_cost,
            "bonus_payment": bonus_payment,
            "bonus_grant": bonus_grant
        })
    bonuspayment_event = {'event_ts': order_date.strftime("%Y-%m-%d %H:%M:%S")
                              ,'event_type':'bonus_transaction'
                              ,'event_value':json2str(bonussystem_payment)}

    return  [order_system_order, bonuspayment_event]

def insert_data_to_mongo(mg_client: MongoClient, collection_name, data_generator, check_fields = None, count=0, **kwargs):
    if count == 0:
        return
    if not check_fields:
        check_fields = set(['_id'])
    else:
        check_fields = set(['_id'] + check_fields)
    collection = mg_client[collection_name]
    existing = list(collection.find({}, {x for x in check_fields}))
    for _ in range(count):
        exist = 0
        new_item = data_generator(**kwargs)
        for key in check_fields:
            existing_vals = [x[key] for x in existing]
            if new_item[key] in existing_vals:
                exist += 1
        if exist == 0:
            collection.insert_one(new_item)
            existing.append(new_item)
        
def insert_order(mg_client: MongoClient, pg_client: PgConnect, collection_name, data_generator, check_fields = None, count=0, **kwargs):
    if count == 0:
        return
    if not check_fields:
        check_fields = set(['_id'])
    else:
        check_fields = set(['_id'] + check_fields)
    collection = mg_client[collection_name]
    existing = list(collection.find({}, {x for x in check_fields}))
    for _ in range(count):
        exist = 0
        new_item, bonus_item = data_generator(**kwargs)
        for key in check_fields:
            existing_vals = [x[key] for x in existing]
            if new_item[key] in existing_vals:
                exist += 1
        if exist == 0:
            collection.insert_one(new_item)
            existing.append(new_item)


            with pg_client.connection() as con:
                with con.cursor() as cursor:
                    cursor.execute(
                            """
                            INSERT INTO outbox (event_ts, event_type, event_value)
                            VALUES (%s, %s, %s)
                            """,
                            (bonus_item['event_ts'], bonus_item['event_type'], bonus_item['event_value']))
