
import json
import random
import requests
from time import perf_counter_ns, time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Generate Transactions
transactions = []
price = 100.0
quantity = 5.0
users = ['user1', 'user2']
user_holdings = {'user1': {'btc': 0.0, 'cash': 1000000}, 'user2': {'btc': 0.0, 'cash': 1000000}}

t1 = time()
for i in range(1000):
    user = users[i % 2]  # Alternate users
    can_sell = user_holdings[user]['btc'] >= quantity
    can_buy = user_holdings[user]['cash'] >= price * quantity

    # Decide buy or sell
    if can_buy and not can_sell:
        side = 'buy'
    elif can_sell and not can_buy:
        side = 'sell'
    elif can_sell and can_buy:
        side = random.choice(['buy', 'sell'])
    else:
        # Skip if user can neither buy nor sell
        continue

    # Update simulated holdings
    if side == 'buy':
        user_holdings[user]['cash'] -= price * quantity
        user_holdings[user]['btc'] += quantity
    else:
        user_holdings[user]['cash'] += price * quantity
        user_holdings[user]['btc'] -= quantity

    transaction = {
        "uId": user,
        "stockId": "btc",
        "orderType": "limit",
        "side": side,
        "quantity": quantity,
        "pricePerUnit": round(price, 2)
    }

    transactions.append(transaction)

    # Slight price fluctuation
    price += random.choice([-0.5, 0.0, 0.5])

url = "http://localhost:8000/user/transaction/new"
index = 1

totalTime_ns = 0  # In nanoseconds

def send_transaction(index, transaction):
    stTime = perf_counter_ns()
    print(f"Transaction Number: {index}")
    response = requests.post(url, json=transaction)
    etTime = perf_counter_ns()
    duration_ns = etTime - stTime
    return duration_ns, response.status_code, index

max_threads = 10
with ThreadPoolExecutor(max_workers=max_threads) as executor:
    futures = [executor.submit(send_transaction, i, tx) for i, tx in enumerate(transactions)]

    for future in as_completed(futures):
        duration_ns, status, tx_index = future.result()
        totalTime_ns += duration_ns
        duration_sec = duration_ns / 1_000_000_000  # Convert to seconds
        print(f"Transaction {tx_index} completed in {duration_sec:.6f} s with status {status}")

# Total time (sum of all transaction durations)
total_time_sec = totalTime_ns / 1_000_000_000
print(f"Total cumulative transaction time: {total_time_sec:.6f} seconds")

print(time() - t1)