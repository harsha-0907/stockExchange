
import time
from uuid import uuid4
from apps.transactionEngine import me
from apps.database import financeDb
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
        
        if side == "buy":
            pricePerUnit = transactionRequest.get("pricePerUnit")
            quantityOfTransaction = transactionRequest.get("quantity")
            walletBalance = userData.get("walletBalance")
            if walletBalance <= pricePerUnit * quantityOfTransaction:
                return 403
            
            # User can buy the stock
            transactionRequest["status"] = status
            transactionRequest["tId"] = tId
            me.dbQueue.put(transactionRequest)
            transactionRequest["action"] = "transaction"
            # print(transactionRequest)
            me.transactionQueue.put(transactionRequest)
            userData["walletBalance"] -= pricePerUnit * quantityOfTransaction
            me.users[uId] = userData
        
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
                me.users[uId] = userData
        return 200

    except Exception as _e:
        print("Error in Push Transaction :", str(_e))
        return 500

@router.get("user/new")
async def newUser():
    # Add the user into the database and add into the TransactionEngine
    uId = "user" + str(me.numberOfUsers + 1)
    me.numberOfUsers += 1
    me.users[uId] = {"walletBalance": 0.00, "stocks": {}}
    return uId

@router.get("user/finance/add")
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

@router.get("user/finance/withdraw")
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
    return response
    