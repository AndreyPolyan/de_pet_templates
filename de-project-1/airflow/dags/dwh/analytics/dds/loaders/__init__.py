import os
import sys

from .fct_sales_loader import FctSaleLoader
from .order_loader import OrderLoader
from .product_loader import ProductLoader
from .restaurant_loader import RestaurantLoader
from .time_loader import TimeLoader
from .user_loader import UserLoader

sys.path.append(os.path.dirname(os.path.realpath(__file__)))