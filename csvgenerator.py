import pandas as pd
import numpy as np
import random
from faker import Faker
import json

fake = Faker()
random.seed(42)
np.random.seed(42)

# Dataset sizes
num_accounts = 20000
num_transactions = 200000

# --- ACCOUNTS DATA ---
acc_data = []
account_types = ['savings', 'current', 'salary', 'business']
account_tiers = ['silver', 'gold', 'platinum', 'diamond']
regions = ['East', 'West', 'North', 'South']

for i in range(num_accounts):
    account_id = f"ACC{i:06d}"
    customer_name = fake.name()
    email = fake.email()
    contact_number = fake.phone_number()
    branch_id = random.randint(100, 150)
    account_type = random.choice(account_types)
    account_tier = random.choices(account_tiers, [0.60, 0.25, 0.10, 0.05])[0]
    opened_date = fake.date_between(start_date='-5y', end_date='-1y')
    account_status = random.choices(['active', 'inactive', 'closed'], [0.83, 0.12, 0.05])[0]
    balance = round(random.uniform(0, 200000), 2)
    region = random.choice(regions)

    acc_data.append([
        account_id, customer_name, email, contact_number, branch_id,
        account_type, account_tier, opened_date, account_status, balance, region
    ])

accounts_df = pd.DataFrame(acc_data,
                           columns=[
                               'account_id', 'customer_name', 'email', 'contact_number', 'branch_id',
                               'account_type', 'account_tier', 'opened_date', 'account_status', 'balance', 'region'
                            ])

# Only 10 columns, remove the 11th
accounts_df = accounts_df.drop(columns='region')

# Add missing values
for col in ['email', 'contact_number']:
    accounts_df.loc[accounts_df.sample(frac=0.01, random_state=1).index, col] = np.nan

# Add duplicates (0.5%)
acc_dupes = accounts_df.sample(frac=0.005, random_state=2)
accounts_df = pd.concat([accounts_df, acc_dupes]).sample(frac=1, random_state=42).reset_index(drop=True)
accounts_df.to_csv('accounts.csv', index=False)

# --- TRANSACTIONS DATA ---
txn_data = []
main_locations = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Atlanta', 'San Francisco']
merchants = ['Amazon', 'Walmart', 'Starbucks', 'Uber', 'Apple', 'Target', 'Shell', 'Costco']
currencies = ['USD', 'EUR', 'GBP']
channels = ['ATM', 'POS', 'Online', 'Mobile', 'Branch']

for i in range(num_transactions):
    transaction_id = f"TXN{i:08d}"
    account_id = f"ACC{random.randint(0, num_accounts-1):06d}"
    transaction_type = random.choice(['credit', 'debit'])
    amount = round(random.uniform(10.0, 20000.0), 2)
    merchant = random.choice(merchants)
    # Anomaly: 1% outlier merchants
    if random.random() < 0.01:
        merchant = fake.company()

    currency = random.choice(currencies)
    location = random.choice(main_locations)
    # Anomaly: 2% random city
    if random.random() < 0.02:
        location = fake.city()
    channel = random.choice(channels)
    status = random.choices(['success', 'failed', 'pending'], [0.96, 0.02, 0.02])[0]
    transaction_date = fake.date_time_between(start_date='-1y', end_date='now')

    txn_data.append([
        transaction_id, account_id, transaction_type, amount, merchant,
        currency, location, channel, status, transaction_date
    ])

transactions_df = pd.DataFrame(txn_data, columns=[
    'transaction_id', 'account_id', 'transaction_type', 'amount', 'merchant',
    'currency', 'location', 'channel', 'status', 'transaction_date'
])

# Add missing values
for col in ['amount', 'merchant', 'location']:
    transactions_df.loc[transactions_df.sample(frac=0.01, random_state=3).index, col] = np.nan

# Add duplicates (0.5%)
txn_dupes = transactions_df.sample(frac=0.005, random_state=4)
transactions_df = pd.concat([transactions_df, txn_dupes]).sample(frac=1, random_state=10).reset_index(drop=True)
transactions_df.to_csv('transactions.csv', index=False)

# --- FRAUD PATTERNS JSON (unchanged) ---
fraud_rules = {
    "rules": [
        {"rule_id": 1, "description": "High value transfer", "field": "amount", "threshold": 10000, "type": "amount"},
        {"rule_id": 2, "description": "Location mismatch", "field": "location", "type": "location"},
        {"rule_id": 3, "description": "Multiple failed attempts", "field": "status", "threshold": 3, "type": "velocity"},
        {"rule_id": 4, "description": "Foreign currency", "field": "currency", "type": "currency", "allowed": ["USD"]},
        {"rule_id": 5, "description": "Unusual merchant", "field": "merchant", "type": "anomaly"}
    ]
}
with open('fraud_patterns.json', 'w') as f:
    json.dump(fraud_rules, f, indent=4)

print("Files created: accounts.csv, transactions.csv, fraud_patterns.json")
