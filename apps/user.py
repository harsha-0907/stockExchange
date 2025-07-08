
from apps.transactionEngine import me
from apps.database import financeDb
from fastapi import APIRouter
from apps.helperFunctions import formatResponse

router = APIRouter()

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

@router.get("/details")
async def fetchBalance(uId: str):
    if uId in me.users:
        userData = me.users[uId]
        return userData
    
    return formatResponse(statusCode=404, description="User Not Found", resource="input", state="user:notfound")
