import json
import time
import random
import requests
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def get_exchange_rate():
    try:
        response = requests.get(
            'https://api.exchangerate-api.com/v4/latest/NGN'
        )
        data = response.json()
        return data['rates']['USD']
    except Exception:
        return 0.00063

def generate_transaction(exchange_rate):
    amount = round(random.uniform(500, 500000), 2)
    return {
        'transaction_id': fake.uuid4(),
        'timestamp': datetime.utcnow().isoformat(),
        'amount': amount,
        'currency': 'NGN',
        'amount_usd': round(amount * exchange_rate, 2),
        'exchange_rate': exchange_rate,
        'status': random.choice(['success', 'success', 'success', 'failed', 'pending']),
        'payment_method': random.choice(['card', 'bank_transfer', 'mobile_money', 'ussd']),
        'merchant_id': fake.uuid4(),
        'merchant_name': fake.company(),
        'customer_id': fake.uuid4(),
        'customer_name': fake.name(),
        'country': random.choice(['Nigeria', 'Ghana', 'Kenya', 'South Africa', 'Egypt']),
        'is_flagged': amount > 300000
    }
    
def run_producer():
    print("Starting fintech transaction producer...")
    
    while True:
        try:
            exchange_rate = get_exchange_rate()
            
            batch_size = random.randint(1, 5)
            
            for _ in range(batch_size):
                transaction = generate_transaction(exchange_rate)
                producer.send('transactions', value=transaction)
                print(f"Sent transaction: {transaction['transaction_id']} | Amount: {transaction['amount']} NGN | Status: {transaction['status']}")
            
            producer.flush()
            time.sleep(2)
            
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)
            
if __name__ == "__main__":
    run_producer()
