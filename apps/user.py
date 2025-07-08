
import time
from uuid import uuid4
from tinydb import Query
from apps.transactionEngine import me
from apps.database import financeDb, transactionDb
from fastapi import APIRouter
from apps.helperFunctions import formatResponse
from models import TransactionIn

router = APIRouter()

def pushTransaction(transactionRequest):    
    def fetchUserData(uId):
        return me.users.get(uId, {})
        
    # transactionRequest -> Syntactically Valid
    try:
        uId = transactionRequest.get("uId")
        userData = fetchUserData(uId)    # This fetches all the data regarding the user
        # print(userData)
        if not userData:
            return 401
        
        transactionRequest["timeStamp"] = time.time()
        side = transactionRequest.get("side")
        tId = uuid4().hex; status = "RECIEVED"

        print(tId)
        
        if side == "buy":
            pricePerUnit = transactionRequest.get("pricePerUnit")
            quantityOfTransaction = transactionRequest.get("quantity")
            walletBalance = userData.get("walletBalance")
            if walletBalance <= pricePerUnit * quantityOfTransaction + 10:  # Transaction Fees
                return 403
            
            # User can buy the stock
            transactionRequest["status"] = status
            transactionRequest["tId"] = tId
            me.dbQueue.put(transactionRequest)
            transactionRequest["action"] = "transaction"
            # print(transactionRequest)
            me.transactionQueue.put(transactionRequest)
            userData["walletBalance"] -= (pricePerUnit * quantityOfTransaction) + 10
            adminData = me.users["admin"]
            adminData["walletBalance"] += 10
            me.users[uId] = userData
            me.users["admin"] = adminData
        
        else:
            quantityOfTransaction = transactionRequest.get("quantity")
            stockId = transactionRequest.get("stockId")    
            userStocks = userData.get("stocks", {})
            if stockId not in userStocks:
                return 404
            elif userStocks.get(stockId, 0) < quantityOfTransaction:
                return 403
            else:
                # User can sell his stock
                transactionRequest["status"] = status
                transactionRequest["tId"] = tId
                me.dbQueue.put(transactionRequest)
                transactionRequest["action"] = "transaction"
                me.transactionQueue.put(transactionRequest)
                userData["stocks"][stockId] -= quantityOfTransaction
                userData["walletBalance"] -= 10 # Transaction Fees
                me.users[uId] = userData
        
        adminData = me.users["admin"]
        adminData["walletBalance"] += 10
        me.users["admin"] = adminData
        return 200

    except Exception as _e:
        print("Error in Push Transaction :", str(_e))
        return 500

@router.get("/new")
async def newUser():
    # Add the user into the database and add into the TransactionEngine
    uId = "user" + str(me.numberOfUsers + 1)
    me.numberOfUsers += 1
    me.users[uId] = {"walletBalance": 0.00, "stocks": {}, "advanced": {}}
    return {
        "statusCode": 200,
        "userId": uId
    }

@router.get("/finance/add")
async def addMoney(amount: float, uId: str):
    if uId not in me.users:
        return formatResponse(statusCode=401)
    
    userData = me.users[uId]
    data = {"uId": uId, "amount": amount, "action": "add", "message": "Amount Credited Successfully"}
    financeDb.insert(data)  # Insert the record in the db
    
    userData["walletBalance"] += amount
    me.users[uId] = userData
    print(me.users[uId])
    return formatResponse(statusCode=200, description="Amount Added to wallet", resource="wallet", state="finance:add")

@router.get("/finance/withdraw")
async def withrawMoney(amount: float, uId: str):
    if uId not in me.users:
        return formatResponse(statusCode=401)
    
    userData = me.users[uId]
    if userData["walletBalance"] < amount:
        return formatResponse(statusCode=403)

    data = {"uId": uId, "amount": amount, "action": "withdraw", "message": "Withdrawal Successful"}
    financeDb.insert(data)
    
    userData["walletBalance"] -= amount
    me.users[uId] = userData
    return formatResponse(statusCode=200, description="Amount withdrawn from wallet", resource="wallet", state="finance:withdraw")

@router.post("/transaction/new")
async def newTransaction(transactionRequest: TransactionIn):
    # print(me.tradedStocks)
    # print(transactionRequest.dict())
    statusCode = pushTransaction(transactionRequest.dict())
    if statusCode == 200:
        return formatResponse(statusCode=statusCode, description="Transaction Accepted", resource="transaction", state="action:transaction")
    return formatResponse(statusCode=statusCode)
    
@router.get("/details")
async def fetchBalance(uId: str):
    if uId in me.users:
        userData = me.users[uId]
        return userData
    
    return formatResponse(statusCode=404, description="User Not Found", resource="input", state="user:notfound")

@router.get("/stock/fetchBBO")
async def fetchBBO(stockId: str):
    if stockId not in me.stockTransactions:
        return formatResponse(404)
    stockData = me.stockTransactions[stockId]["data"]
    return stockData

@router.get("/transaction/details")
async def fetchTransactionDetails(uId: str = None, tId: str = None):
    query = Query()
    if uId and tId:
        results = transactionDb.search((query.tId == tId) & (query.uId == uId))
    
    elif uId:
        results = transactionDb.search((query.uId == uId))
    
    elif tId:
        results = transactionDb.search((query.tId == tId))
    
    else:
        results = formatResponse(statusCode=404, description="Fields missing. Require atleast transactionId or userId", resource="input", state="input:fieldsmissing")
    
    return results

@router.delete("/transaction/delete")
async def deleteTransaction(uId: str, tId: str):
    transaction = Query()
    results = transactionDb.search((transaction.tId == tId))

    if len(results) == 0:
        # No such transaction exists
        return formatResponse(statusCode=404, description="No such transaction exists", resource="transaction", state="data:notfound")
    
    dbUid = results[0].get("uId")
    if uId != dbUid:
        # Not Authorized to perform this transaction
        return formatResponse(statusCode=401, description="Not Authorized to perform this transaction", resource="transaction", state="action:unauth")

    for result in results:
        status = result.get("status")
        if status == "COMPLETED" or status == "IN-COMPLETE":
            return formatResponse(statusCode=401, description="Transaction already complete.", resource="transaction", state="action:unauth")

    # All the updates will be done at the matching engine    
    stockId = results[0].get("stockId"); side = results[0].get("side")
    transactionRequest = {
        "action": "remove-transaction",
        "tId": tId,
        "side": side,
        "stockId": stockId
    }
    me.transactionQueue.put(transactionRequest)

    return formatResponse(statusCode=200, description="Cancellation Reuqest Submitted", resource="transaction", state="action:success")
      
@router.get("/stocks")
async def fetchTradedStocks():
    # Returns all the stocks that are traded by the trading engine
    return {
        "statusCode": 200,
        "tradedStocks": me.tradedStocks
    }