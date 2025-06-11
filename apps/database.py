
from tinydb import TinyDB

userDb = TinyDB("database/users.json")
transactionDb = TinyDB("database/transactions.json")
financeDb = TinyDB("database/finance.json")
internalTransactionDb = TinyDB("database/internalTransactions.json")
