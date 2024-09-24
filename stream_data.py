import uuid
import random
import requests
import json
import time
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

default_args = {
    'owner': '',
    'start_date': datetime(2024, 9, 24, 2, 59)
}

# Sample data for demonstration
payment_methods = ['credit_card', 'paypal', 'bank_transfer', 'applepay']
currencies = ['USD', 'EUR', 'GBP']
transaction_statuses = ['completed', 'failed', 'pending']
product_categories = ['electronics', 'clothing', 'groceries', 'toys', 'furniture']

def get_random_user():
    """Fetch random user data from the Random User API."""
    res = requests.get("https://randomuser.me/api/")
    return res.json()['results'][0]

def generate_transaction_data(user):
    """Generate sample transaction data using random user details."""
    transaction_id = str(uuid.uuid4())
    timestamp = datetime.now().isoformat()
    customer_id = str(uuid.uuid4())
    payment_method = random.choice(payment_methods)
    transaction_amount = round(random.uniform(5.0, 1050.0), 2)  # Random amount between $5 and $500
    currency = random.choice(currencies)
    transaction_status = random.choice(transaction_statuses)
    product_id = str(uuid.uuid4())
    product_category = random.choice(product_categories)
    
    # Extracting relevant user data
    shipping_address = f"{user['location']['street']['number']} {user['location']['street']['name']}, " \
                      f"{user['location']['city']}, {user['location']['state']}, {user['location']['country']}"
    
    geolocation = f"{user['location']['coordinates']['latitude']},{user['location']['coordinates']['longitude']}"  # lat, long
    device_type = random.choice(['mobile', 'desktop'])
    ip_address = user['login']['uuid']  # Using UUID as a placeholder for IP address

    data = {
        'transaction_id': transaction_id,
        'timestamp': timestamp,
        'customer_id': customer_id,
        'payment_method': payment_method,
        'transaction_amount': transaction_amount,
        'currency': currency,
        'transaction_status': transaction_status,
        'product_id': product_id,
        'product_category': product_category,
        'shipping_address': shipping_address,
        'geolocation': geolocation,
        'device_type': device_type,
        'ip_address': ip_address
    }
    return data

def stream_data():
    """Stream transaction data to Kafka."""
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:  # Run for 1 minute
            break
        try:
            user = get_random_user()
            transaction_data = generate_transaction_data(user)
            producer.send('transactions_created', json.dumps(transaction_data).encode('utf-8'))
            time.sleep(1)  # Wait for a second before sending the next transaction
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue

with DAG('transaction_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
