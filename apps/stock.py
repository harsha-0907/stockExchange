
from fastapi import APIRouter
from apps.transactionEngine import me
from apps.helperFunctions import formatResponse

router = APIRouter()

@router.get("/stock/fetchBBO")
async def fetchBBO(stockId: str):
    if stockId not in me.stockTransactions:
        return formatResponse(404)
    stockData = me.stockTransactions[stockId]["data"]
    return stockData

@router.get("/stocks")
async def fetchTradedStocks():
    # Returns all the stocks that are traded by the trading engine
    return {
        "statusCode": 200,
        "tradedStocks": me.tradedStocks
    }
