import logging
import uuid
import random
from decimal import Decimal, ROUND_HALF_UP
from functools import cmp_to_key

from database_connection import postgresql_db_cursor
from types import MappingProxyType


host_name = 'localhost'
database = 'postgresDB'
username = 'Joban'
password = 'Joban123'
port_id = 5432

TARGET_RECORD_NUM = 300
TARGET_UNIQUE_MERCHANT_NUM = 40
DAYS_IN_MONTH_MAP = MappingProxyType({ 1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31 })

drop_table_sql = "drop table if exists bank_transaction_yearly"
create_table_query = (
    "CREATE TABLE bank_transaction_yearly ("
    "id SERIAL PRIMARY KEY, "
    "month INT NOT NULL, "
    "day INT NOT NULL, "
    "hour INT NOT NULL, "
    "minute INT NOT NULL, "
    "amount NUMERIC(10,2) NOT NULL, "
    "merchant VARCHAR(36) NOT NULL"
    ")"
)

def initialize_empty_database():
    with postgresql_db_cursor(host_name, database, username, password) as cursor:
        cursor.execute(drop_table_sql)
        cursor.execute(create_table_query)
        logging.info("Initialized database")

def insert_bank_transaction(records_to_insert):
    insert_query = """
    INSERT INTO bank_transaction_yearly (month, day, hour, minute, amount, merchant)
    VALUES (%(month)s, %(day)s, %(hour)s, %(minute)s, %(amount)s, %(merchant)s)
    """
    with postgresql_db_cursor(host_name, database, username, password) as cursor:
        cursor.executemany(insert_query, records_to_insert)

def generate_merchants():
    merchant_list = []
    for _ in range(TARGET_UNIQUE_MERCHANT_NUM):
        merchant_list.append(str(uuid.uuid4()))
    return merchant_list

def generate_records(merchants):
    month = random.randint(1,12)
    day = random.randint(1,DAYS_IN_MONTH_MAP.get(month))
    hour = random.randint(0,23)
    minute = random.randint(0,59)
    float_amount = (random.randint(1,100000))/100
    if random.choice([True, False]):
        float_amount*=-1
    random.random()
    amount = Decimal(float_amount).quantize(Decimal('0.01'),rounding=ROUND_HALF_UP)
    merchant = random.choice(merchants)
    return {
        "month": month,
        "day": day,
        "hour": hour,
        "minute": minute,
        "amount": amount,  # Convert Decimal to float if needed
        "merchant": merchant
    }

def compare_records(t1, t2):
    if t1["month"] != t2["month"]:
        return t1["month"] - t2["month"]
    elif t1["day"] != t2["day"]:
        return t1["day"] - t2["day"]
    elif t1["hour"] != t2["hour"]:
        return t1["hour"] - t2["hour"]
    elif t1["minute"] != t2["minute"]:
        return t1["minute"] - t2["minute"]
    else:
        return (t1["amount"] > t2["amount"]) - (t1["amount"] < t2["amount"])



def generate_data():
    initialize_empty_database()
    merchants = generate_merchants()
    records_to_insert = []
    for _ in range(TARGET_RECORD_NUM):
        records_to_insert.append(generate_records(merchants))
    #Python’s sort method accepts a key function. The cmp_to_key utility is used to adapt the comparator-style function for sorting.
    records_to_insert.sort(key=cmp_to_key(compare_records))
    print(records_to_insert)
    insert_bank_transaction(records_to_insert)
generate_data()








