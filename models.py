
from pydantic import BaseModel, validator

class User(BaseModel):
    name: str
    uid: str
    accountBalance: float
    stockAggregate: dict
    pastTransactions: list
    currentTransactions: list

class TransactionIn(BaseModel):
    uId: str
    stockId: str
    orderType: str
    side: str
    quantity: float
    pricePerUnit: float

    @validator("orderType")
    def validateOrderType(cls, v):
        if v.lower() not in ["market", "limit", "ioc", "fok"]:
            raise ValueError("Invalid Order Type")
        return v
    @validator("side")
    def validateSide(cls, v):
        if v.lower() not in ["buy", "sell"]:
            raise ValueError("Invalid Side")
        return v
    @validator("quantity")
    def validateQuantity(cls, v):
        if v <= 0:
            raise ValueError("Invalid Quantity")
        return v
    @validator("pricePerUnit")
    def validatePricePerUnit(cls, v):
        if v <= 0:
            raise ValueError("Invalid Price Per Unit")
        return v

class Transaction(TransactionIn):
    tId: str
    status: str

class StockItem(BaseModel):
    name: str
    stockId: str
    pricePerUnit: float
    bids: list
    asks: list
    bestBid: float
    bestAsk: float

