
from tinydb import TinyDB

with open("database/users.json", 'w') as file:
    pass
with open("database/transactions.json", 'w') as file:
    pass
with open("database/finance.json", 'w') as file:
    pass
with open("database/internalTransactions.json", 'w') as file:
    pass
userDb = TinyDB("database/users.json")
transactionDb = TinyDB("database/transactions.json")
financeDb = TinyDB("database/finance.json")
internalTransactionDb = TinyDB("database/internalTransactions.json")
