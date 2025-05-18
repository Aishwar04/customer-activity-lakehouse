import csv
from faker import Faker
import random
from datetime import datetime, timedelta

def generate_transactions(num_records, output_file):
    fake = Faker()
    transactions = []

    # Add header row
    transactions.append([
        'transaction_id', 'customer_id', 'product_id',
        'transaction_timestamp', 'quantity', 'price',
        'store_location', 'payment_method'
    ])

    # Generate records
    for i in range(num_records):
        transaction_id = fake.uuid4()
        customer_id = random.randint(1000, 50000) # Simulate a range of customer IDs
        product_id = f'PROD{random.randint(100, 999)}' # Simulate product IDs

        # Generate a timestamp within the last year
        timestamp = datetime.now() - timedelta(days=random.randint(0, 365),
                                               hours=random.randint(0, 23),
                                               minutes=random.randint(0, 59),
                                               seconds=random.randint(0, 59))

        quantity = random.randint(1, 10)
        price = round(random.uniform(5.0, 500.0), 2) # Simulate price with 2 decimal places
        store_location = random.choice(['online', 'store_A', 'store_B', 'mobile_app'])
        payment_method = random.choice(['credit_card', 'paypal', 'cash', 'bank_transfer'])


        transactions.append([
            transaction_id, customer_id, product_id,
            timestamp.strftime('%Y-%m-%d %H:%M:%S'), # Format timestamp as string
            quantity, price,
            store_location, payment_method
        ])

    # Write to CSV file
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(transactions)

    print(f"Generated {num_records} transaction records to {output_file}")

if __name__ == "__main__":
    # Generate 10,000000 transaction records and save to a CSV file
    generate_transactions(10000000, 'customer_transactions.csv')