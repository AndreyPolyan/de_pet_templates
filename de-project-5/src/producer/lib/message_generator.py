import random
import uuid
from datetime import datetime, timedelta
from itertools import count
from typing import Dict

class CampaignRecordGenerator:
    def __init__(self) -> None:
        # Initialize the counter
        self.key_counter = count(1)
    
    def generate_record(self) -> Dict:
        """
        Generate a single random campaign record with an incrementing key.
        Returns a dictionary with 'key' and 'value', where:
        - 'key' is an incrementing integer.
        - 'value' is the generated campaign record.
        """
        restaurant_ids = [
            "123e4567-e89b-12d3-a456-426614174001",
            "123e4567-e89b-12d3-a456-426614174000",
            str(uuid.uuid4()),
            str(uuid.uuid4()),
            str(uuid.uuid4()),
        ]
        
        campaign_content_variants = [
            "Промо - 1 бургер в подарок",
            "Ланч за наш счет",
            "Скидка 20% на все меню",
            "2 по цене 1 на пиццу",
            "Бесплатная доставка",
            "Счастливый час - скидка 50%",
            "Бесплатный напиток при заказе",
            "Комплимент от шефа",
            "Скидка на завтрак 30%",
            "Праздничное меню со скидкой",
        ]

        # Randomly select a restaurant
        restaurant_id = random.choice(restaurant_ids)
        owner_name = f"Owner {restaurant_id[-4:]}"  # Dummy owner name
        owner_contact = f"owner_{restaurant_id[-4:]}@restaurant.ru"  # Dummy email

        # Generate a single record
        adv_campaign_id = f"{restaurant_id}_{random.randint(1, 100)}"
        adv_campaign_content = random.choice(campaign_content_variants)
        now = datetime.now()
        adv_campaign_datetime_start = int((now - timedelta(days=random.randint(1, 30))).timestamp())
        adv_campaign_datetime_end = int((now + timedelta(days=random.randint(1, 30))).timestamp())
        datetime_created = int(now.timestamp())

        # Construct the record
        record = {
            "restaurant_id": restaurant_id,
            "adv_campaign_id": adv_campaign_id,
            "adv_campaign_content": adv_campaign_content,
            "adv_campaign_owner": owner_name,
            "adv_campaign_owner_contact": owner_contact,
            "adv_campaign_datetime_start": adv_campaign_datetime_start,
            "adv_campaign_datetime_end": adv_campaign_datetime_end,
            "datetime_created": datetime_created,
        }

        return {
            "key": next(self.key_counter),
            "value": record,
        }