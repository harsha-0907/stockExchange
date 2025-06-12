import time
import traceback
from queue import Empty as QueueEmpty
import heapq
import json

def matchingEngine(mainTransactions, stockId, queue, dbQueue, iTQueue, logQueue, users, shutdownEvent):
    def loadTransactions(stockId):
        data = {}
        try:
            with open(f"database/stocks/{stockId}.json", 'r') as file:
                data = json.load(file)
        except Exception as _e:
            pass
        finally:
            return data
    
    def writeTransactions(stockId, transactions):
        with open(f"database/stocks/{stockId}.json", 'w') as file:
            json.dump(transactions, file, indent=4)
        print("Data Written Successfully")

    def marketTransaction(request):
        nonlocal transactions, stockId
        side = request.get("side")
        internalTransactions = []; dbTransactions = []; userTransactions = []
        timeStamp = time.time()

        if side == "buy":
            buyerId, buyerTid = request.get("uId"), request.get("tId")
            numberOfStocksRequired = totalNumberOfStocksRequired = request.get("quantity"); amountPaid = request.get("pricePerUnit") * numberOfStocksRequired
            totalAmountRequired = 0; totalStocksBrought = 0

            while numberOfStocksRequired > 0 and len(transactions["sell"]) > 0:
                sellingPrice, sellingTimeStamp, sellRequest = heapq.heappop(transactions["sell"])
                stocksAvaliableInThisTransaction = sellRequest.get("quantity")

                stocksInThisTransaction = min(numberOfStocksRequired, stocksAvaliableInThisTransaction)
                numberOfStocksRequired -= stocksInThisTransaction
                stocksAvaliableInThisTransaction -= stocksInThisTransaction

                priceOfThisTransaction = stocksInThisTransaction * sellingPrice
                totalAmountRequired += priceOfThisTransaction
                totalStocksBrought += stocksInThisTransaction
                transactions["marketPrice"] = sellingPrice

                if stocksAvaliableInThisTransaction > 0:
                    # Update this back to the heap
                    sellRequest["quantity"] = stocksAvaliableInThisTransaction
                    heapq.heappush(transactions["sell"], [sellingPrice, sellingTimeStamp, sellRequest])

                sellerId, sellerTid = sellRequest.get("uId"), sellRequest.get("tId")
                internalTransactionRequest = {
                    "stockId": stockId,
                    "sellerId": sellerId,
                    "sellerTid": sellerTid,
                    "buyerId": buyerId,
                    "buyerTid": buyerTid,
                    "noOfStocks": stocksInThisTransaction,
                    "amount": priceOfThisTransaction,
                    "timeStamp": timeStamp
                }

                dbTransactionRequest = {
                    "tId": sellerTid,
                    "uId": sellerId,
                    "stockId": stockId,
                    "side": "sell",
                    "orderType": "market",
                    "quantity": stocksInThisTransaction,
                    "pricePerUnit": sellingPrice,
                    "status": "PARTIAL" if stocksAvaliableInThisTransaction > 0 else "COMPLETED",
                    "timeStamp": timeStamp
                }

                userTransactionRequest = [
                        {
                            "action": "add",
                            "resource": "money",
                            "uId": sellerId,
                            "quantity": priceOfThisTransaction
                        }
                    ]

                internalTransactions.append(internalTransactionRequest)
                dbTransactions.append(dbTransactionRequest)
                userTransactions.append(userTransactionRequest)

            if totalStocksBrought == 0:
                dbTransactionRequest = {
                        "tId": buyerTid,
                        "uId": buyerId,
                        "stockId": stockId,
                        "side": "buy",
                        "orderType": "market",
                        "quantity": numberOfStocksRequired,
                        "pricePerUnit": request.get("pricePerUnit"),
                        "status": "IN-COMPLETE",
                        "timeStamp": timeStamp
                    }
                userTransactionRequest = {
                    "action": "add",
                    "resource": "money",
                    "quantity": amountPaid, # Refund the total amount to the user
                    "uId": buyerId
                }

                return [], [dbTransactionRequest], [userTransactionRequest]
            
            dbTransactionRequest = {
                "tId": buyerTid,
                "uId": buyerId,
                "stockId": stockId,
                "side": "buy",
                "orderType": "market",
                "quantity": totalStocksBrought,
                "pricePerUnit": totalAmountRequired / totalStocksBrought if totalStocksBrought > 0 else 0,
                "status": "PARTIAL" if numberOfStocksRequired > 0 else "COMPLETED",
                "timeStamp": timeStamp
            }

            userTransactionRequest = {
                "action": "add",
                "resource": "stock",
                "stockId": stockId,
                "quantity": totalStocksBrought,
                "uId": buyerId
            }
            
            dbTransactions.append(dbTransactionRequest)
            userTransactions.append(userTransactionRequest)
            amountDifference = amountPaid - totalAmountRequired
            if amountDifference != 0:
                userTransactionRequest = {
                    "action": "add" if amountPaid > totalAmountRequired else "remove",
                    "resource": "money",
                    "uId": buyerId,
                    "quantity": abs(amountDifference)
                }

                userTransactions.append(userTransactionRequest)
            
            return internalTransactions, dbTransactions, userTransactions
        
        else:
            sellerId, sellerTid = request.get("uId"), request.get("tId")
            numberOfStocksToSell = totalNumberOfStocksToSell = request.get("quantity")
            totalStocksSold = totalAmountRecieved = 0

            while numberOfStocksToSell > 0 and len(transactions["buy"]) > 0:
                buyingPrice, buyingTimeStamp, buyRequest = heapq.heappop(transactions["buy"])
                buyingPrice *= -1
                numberOfStocksRequired = buyRequest.get("quantity")

                stocksInThisTransaction = min(numberOfStocksRequired, numberOfStocksToSell)
                numberOfStocksRequired -= stocksInThisTransaction
                numberOfStocksToSell -= stocksInThisTransaction

                priceOfThisTransaction = stocksInThisTransaction * buyingPrice
                totalStocksSold += stocksInThisTransaction
                totalAmountRecieved += priceOfThisTransaction
                transactions["marketPrice"] = buyingPrice

                if numberOfStocksRequired > 0:
                    # Push the transaction back to the heap
                    buyRequest["quantity"] = numberOfStocksRequired
                    heapq.heappush(transactions["buy"], [-buyingPrice, buyingTimeStamp, buyRequest])
                
                buyerId, buyerTid = buyRequest.get("uId"), buyRequest.get("tId")
                internalTransactionRequest = {
                    "stockId": stockId,
                    "sellerId": sellerId,
                    "sellerTid": sellerTid,
                    "buyerId": buyerId,
                    "buyerTid": buyerTid,
                    "noOfStocks": stocksInThisTransaction,
                    "amount": priceOfThisTransaction,
                    "timeStamp": timeStamp
                }

                dbTransactionRequest = {
                    "tId": sellerTid,
                    "uId": sellerId,
                    "stockId": stockId,
                    "side": "buy",
                    "orderType": "market",
                    "quantity": stocksInThisTransaction,
                    "pricePerUnit": buyingPrice,
                    "status": "PARTIAL" if numberOfStocksRequired > 0 else "COMPLETED",
                    "timeStamp": timeStamp
                }

                userTransactionRequest = {
                    "action": "add",
                    "resource": "stock",
                    "stockId": stockId,
                    "quantity": stocksInThisTransaction,
                    "uId": buyerId
                }

                internalTransactions.append(internalTransactionRequest)
                dbTransactions.append(dbTransactionRequest)
                userTransactions.append(userTransactionRequest)
            
            if totalStocksSold == 0:
                userTransactionRequest = {
                    "action": "add",
                    "resource": "stock",
                    "stockId": stockId,
                    "uId": sellerId,
                    "quantity": totalNumberOfStocksToSell
                }

                dbTransactionRequest = {
                    "tId": sellerTid,
                    "uId": sellerId,
                    "stockId": stockId,
                    "side": "sell",
                    "orderType": "market",
                    "quantity": totalNumberOfStocksToSell,
                    "pricePerUnit": request.get("pricePerUnit"),
                    "status": "IN-COMPLETE",
                    "timeStamp": timeStamp
                }

                return [], [dbTransactions], [userTransactionRequest]

            dbTransactionRequest = {
                "tId": sellerTid,
                "uId": sellerId,
                "stockId": stockId,
                "side": "sell",
                "orderType": "market",
                "quantity": totalStocksSold,
                "pricePerUnit": totalAmountRecieved / totalStocksSold if totalStocksSold else 0,
                "status": "PARTIAL" if numberOfStocksToSell > 0 else "COMPLETED",
                "timeStamp": timeStamp
            }
            dbTransactions.append(dbTransactionRequest)

            userTransactionRequest = {
                "action": "add",
                "resource": "money",
                "uId": sellerId,
                "quantity": totalAmountRecieved
            }

            userTransactions.append(userTransactionRequest)

            if numberOfStocksToSell > 0:
                userTransactionRequest = {
                    "action": "add",
                    "resource": "stock",
                    "stockId": stockId,
                    "uId": sellerId,
                    "quantity": numberOfStocksToSell
                }
                userTransactions.append(userTransactionRequest)
            return internalTransactions, dbTransactions, userTransactions
    
    def limitTransaction(request):
        nonlocal transactions, stockId
        side = request.get("side")
        internalTransactions = []; dbTransactions = []; userTransactions = []
        timeStamp = time.time()

        if side == "buy":
            buyerId, buyerTid = request.get("uId"), request.get("tId")
            numberOfStocksRequired = request.get("quantity"); maxBuyingPrice = request.get("pricePerUnit")
            totalNumberOfStocksBrought = totalAmountRequired = 0

            while numberOfStocksRequired > 0 and len(transactions["sell"]) > 0 and transactions["sell"][0][0] <= maxBuyingPrice:
                sellingPrice, sellingTimeStamp, sellRequest = heapq.heappop(transactions["sell"])
                numberOfStocksToSell = sellRequest.get("quantity")

                numberOfStocksInTransaction = min(numberOfStocksRequired, numberOfStocksToSell)
                numberOfStocksToSell -= numberOfStocksInTransaction
                numberOfStocksRequired -= numberOfStocksInTransaction
                transactions["marketPrice"] = sellingPrice

                priceOfThisTransaction = numberOfStocksInTransaction * sellingPrice
                totalAmountRequired += priceOfThisTransaction
                totalNumberOfStocksBrought += numberOfStocksInTransaction

                if numberOfStocksToSell > 0:
                    sellRequest["quantity"] = numberOfStocksToSell
                    heapq.heappush(transactions["sell"], [sellingPrice, sellingTimeStamp, sellRequest])
                
                sellerId, sellerTid = sellRequest.get("uId"), sellRequest.get("tId")
                internalTransactionRequest = {
                    "stockId": stockId,
                    "sellerId": sellerId,
                    "sellerTid": sellerTid,
                    "buyerId": buyerId,
                    "buyerTid": buyerTid,
                    "noOfStocks": numberOfStocksInTransaction,
                    "amount": priceOfThisTransaction,
                    "timeStamp": timeStamp
                }

                dbTransactionRequest = {
                    "sellerId": sellerId,
                    "sellerTid": sellerTid,
                    "stockId": stockId,
                    "side": "sell",
                    "orderType": "limit",
                    "quantity": numberOfStocksInTransaction,
                    "pricePerUnit": sellingPrice,
                    "status": "PARTIAL" if numberOfStocksToSell> 0 else "COMPLETED",
                    "timeStamp": timeStamp
                }

                userTransactionRequest = {
                    "action": "add",
                    "resource": "money",
                    "uId": sellerId,
                    "quantity": priceOfThisTransaction
                }

                internalTransactions.append(internalTransactionRequest)
                dbTransactions.append(dbTransactionRequest)
                userTransactions.append(userTransactionRequest)
            
            if totalNumberOfStocksBrought == 0:
                heapq.heappush(transactions["buy"], [-request.get("pricePerUnit"), request.get("timeStamp"), request])
                return [], [], []

            dbTransactionRequest = {
                "uId": buyerId,
                "tId": buyerTid,
                "stockId": stockId,
                "side": "buy",
                "orderType": "limit",
                "quantity": numberOfStocksRequired,
                "pricePerUnit": totalAmountRequired / totalNumberOfStocksBrought,
                "status": "PARTIAL" if numberOfStocksRequired > 0 else "COMPLETED",
                "timeStamp": timeStamp
            }

            dbTransactions.append(dbTransactionRequest)
            
            if numberOfStocksRequired > 0:
                request["quantity"] = numberOfStocksRequired
                heapq.heappush(transactions["buy"], [-request.get("pricePerUnit"), request.get("timeStamp"), request])
            
            userTransactionRequest = {
                "action": "add",
                "resource": "stock",
                "stockId": stockId,
                "uId": buyerId,
                "quantity": totalNumberOfStocksBrought
            }

            userTransactions.append(userTransactionRequest)

            amountPaidForTheseStocks = request.get("pricePerUnit") * totalNumberOfStocksBrought
            amountDif = amountPaidForTheseStocks - totalAmountRequired

            if amountDif != 0:
                userTransactionRequest = {
                    "action": "add" if amountDif > 0 else "remove",
                    "uId": buyerId,
                    "resource": "money",
                    "quantity": abs(amountDif)
                }
                userTransactions.append(userTransactionRequest)
            
            return internalTransactions, dbTransactions, userTransactions

        else:
            sellerId, sellerTid = request.get("uId"), request.get("tId")
            numberOfStocksToSell = request.get("quantity"); minSellPrice = request.get("pricePerUnit")
            numberOfStocksSold = totalAmountRecieved = 0

            while numberOfStocksToSell > 0 and len(transactions["buy"]) > 0 and -transactions["buy"][0][0] >= minSellPrice:
                buyingPrice, buyingTimeStamp, buyRequest = heapq.heappop(transactions["buy"])
                buyingPrice *= -1
                numberOfStocksRequired = buyRequest.get("quantity")
                buyerId, buyerTid = buyRequest.get("uId"), buyRequest.get("tId")

                numberOfStocksInTransaction = min(numberOfStocksRequired, numberOfStocksToSell)
                numberOfStocksRequired -= numberOfStocksInTransaction
                numberOfStocksToSell -= numberOfStocksInTransaction
                transactions["marketPrice"] = buyingPrice

                priceOfThisTransaction = numberOfStocksInTransaction * buyingPrice
                totalAmountRecieved += priceOfThisTransaction
                numberOfStocksSold += numberOfStocksInTransaction

                if numberOfStocksRequired > 0:
                    buyRequest["quantity"] = numberOfStocksRequired
                    heapq.heappush(transactions["buy"], [-buyingPrice, buyingTimeStamp, buyRequest])

                internalTransactionRequest = {
                    "stockId": stockId,
                    "sellerId": sellerId,
                    "sellerTid": sellerTid,
                    "buyerId": buyerId,
                    "buyerTid": buyerTid,
                    "noOfStocks": numberOfStocksInTransaction,
                    "amount": priceOfThisTransaction,
                    "timeStamp": timeStamp
                }

                dbTransactionRequest = {
                    "tId": buyerTid,
                    "uId": buyerId,
                    "stockId": stockId,
                    "side": "buy",
                    "orderType": "limit",
                    "quantity": numberOfStocksInTransaction,
                    "pricePerUnit": buyingPrice,
                    "status": "PARTIAL" if numberOfStocksRequired > 0 else "COMPLETED",
                    "timeStamp": timeStamp
                }

                userTransactionRequest = {
                    "action": "add",
                    "resource": "stock",
                    "stockId": stockId,
                    "quantity": numberOfStocksInTransaction,
                    "uId": buyerId
                }

                internalTransactions.append(internalTransactionRequest)
                dbTransactions.append(dbTransactionRequest)
                userTransactions.append(userTransactionRequest)
            
            if numberOfStocksSold == 0:
                heapq.heappush(transactions["sell"], [request.get("pricePerUnit"), request.get("timeStamp"), request])
                return [], [], []
            
            dbTransactionRequest = {
                "tId": sellerTid,
                "uId": sellerId,
                "stockId": stockId,
                "side": "sell",
                "orderType": "limit",
                "quantity": numberOfStocksSold,
                "pricePerUnit": totalAmountRecieved / numberOfStocksSold,
                "status": "PARTIAL" if numberOfStocksToSell > 0 else "COMPLETED",
                "timeStamp": timeStamp
            }
            dbTransactions.append(dbTransactionRequest)

            if numberOfStocksToSell > 0:
                request["quantity"] = numberOfStocksToSell
                heapq.heappush(transactions["sell"], [request.get("pricePerUnit"), request.get("timeStamp"), request])
            
            userTransactionRequest = {
                "action": "add",
                "resource": "money",
                "uId": sellerId,
                "quantity": totalAmountRecieved
            }

            userTransactions.append(userTransactionRequest)

            return internalTransactions, dbTransactions, userTransactions

    def iocTransaction(request):
        nonlocal transactions, stockId
        side = request.get("side")
        internalTransactions = []; dbTransactions = []; userTransactions = []
        timeStamp = time.time()

        if side == "buy":
            buyerId, buyerTid = request.get("uId"), request.get("tId")
            numberOfSharesRequired = totalNumberOfSharesRequired = request.get("quantity"); maxBuyingPrice = request.get("pricePerUnit")
            totalAmount = totalSharesBrought = 0

            if len(transactions["sell"]) == 0:
                # No transaction occured, refund the money and update the transaction as incomplete
                dbTransactionRequest  = {
                    "tId": buyerTid,
                    "uId": buyerId,
                    "stockId": stockId,
                    "side": "buy",
                    "orderType": "ioc",
                    "quantity": numberOfSharesRequired,
                    "pricePerUnit": maxBuyingPrice,
                    "status": "IN-COMPLETE",
                    "timeStamp": timeStamp
                }

                userTransactionRequest = {
                    "action": "add",
                    "resource": "money",
                    "uId": buyerId,
                    "quantity": numberOfSharesRequired * maxBuyingPrice
                }

                return [], [dbTransactionRequest], [userTransactionRequest]
            
            iocPrice = transactions["sell"][0][0]
            while numberOfSharesRequired > 0 and len(transactions["sell"]) > 0 and iocPrice <= transactions["sell"][0][0]:
                sellingPrice, sellingTimeStamp, sellRequest = heapq.heappop(transactions["sell"])
                numberOfSharesToSell = sellRequest.get("pricePerUnit")
                sellerId, sellerTid = sellRequest.get("uId"), sellRequest.get("tId")

                numberOfSharesInTransaction = min(numberOfSharesToSell, numberOfSharesRequired)
                numberOfSharesToSell -= numberOfSharesInTransaction
                numberOfSharesRequired -= numberOfSharesInTransaction

                priceOfThisTransaction = numberOfSharesInTransaction * sellingPrice
                totalSharesBrought += numberOfSharesInTransaction
                totalAmount += priceOfThisTransaction

                if numberOfSharesToSell > 0:
                    # Add the transaction back to the heap
                    sellRequest["quantity"] = numberOfSharesToSell
                    heapq.heappush(transactions["sell"], [sellingPrice, sellingTimeStamp, sellRequest])
                
            
                # Atleast one share is brought (at ioc price)
                internalTransactionRequest = {
                    "stockId": stockId,
                    "sellerId": sellerId,
                    "sellerTid": sellerTid,
                    "buyerId": buyerId,
                    "buyerTid": buyerTid,
                    "noOfStocks": numberOfSharesInTransaction,
                    "amount": priceOfThisTransaction,
                    "timeStamp": timeStamp
                }

                dbTransactionRequest = {
                    "tId": sellerTid,
                    "uId": sellerId,
                    "stockId": stockId,
                    "side": "sell",
                    "orderType": "ioc",
                    "quantity": numberOfSharesInTransaction,
                    "pricePerUnit": sellingPrice,
                    "status": "PARTIAL" if numberOfSharesToSell > 0 else "COMPLETED",
                    "timeStamp": timeStamp
                }

                userTransactionRequest = {
                    "action": "add",
                    "resource": "money",
                    "uId": sellerId,
                    "quantity": priceOfThisTransaction

                }

                internalTransactions.append(internalTransactionRequest)
                dbTransactions.append(dbTransactionRequest)
                userTransactions.append(userTransactionRequest)

            dbTransactionRequest = {
                "tId": buyerTid,
                "uId": buyerId,
                "stockId": stockId,
                "side": "buy",
                "orderType": "ioc",
                "quantity": totalSharesBrought,
                "pricePerUnit": totalAmount / totalSharesBrought if totalSharesBrought else 0,
                "status": "PARTIAL" if numberOfSharesRequired > 0 else "COMPLETED",
                "timeStamp": timeStamp
            }

            amountDiff = totalNumberOfSharesRequired * maxBuyingPrice - totalAmount
            if amountDiff != 0:
                userTransactionRequest = {
                    "action": "add" if amountDiff > 0 else "remove",
                    "resource": "money",
                    "uId": buyerId,
                    "quantity": abs(amountDiff)
                }

                userTransactions.append(userTransactionRequest)

            userTransactionRequest = {
                "action": "add",
                "resource": "stock",
                "stockId": stockId,
                "uId": buyerId,
                "quantity": totalSharesBrought

            }

            userTransactions.append(userTransactionRequest)
            return internalTransactions, dbTransactions, userTransactions

        else:
            sellerId, sellerTid = request.get("uId"), request.get("tId")
            numberOfSharesAvailableToSell = request.get("quantity"); minSellingPrice = request.get("pricePerUnit")
            totalSharesSold = totalAmountRecieved = 0

            if len(transactions["buy"]) == 0:
                dbTransactionRequest = {
                    "tId": sellerTid,
                    "uId": sellerId,
                    "stockId": stockId,
                    "side": "sell",
                    "orderType": "ioc",
                    "quantity": numberOfSharesAvailableToSell,
                    "pricePerUnit": minSellingPrice,
                    "status": "IN-COMPLETE",
                    "timeStamp": timeStamp
                }

                userTransactionRequest = {
                    "action": "add",
                    "resource": "money",
                    "uId": sellerId,
                    "quantity": minSellingPrice * numberOfSharesAvailableToSell
                }
                return [], [dbTransactionRequest], [userTransactionRequest]
            
            iocPrice = -transactions["buy"][0][0]
            while numberOfSharesAvailableToSell > 0 and len(transactions["buy"]) > 0 and -transactions["buy"][0][0] >= iocPrice:
                buyingPrice, buyingTimeStamp, buyRequest = heapq.heappop(transactions["buy"])
                numberOfStocksRequired = buyRequest.get("quantity"); maxBuyingPrice = buyRequest.get("pricePerUnit")
                buyerId, buyerTid = buyRequest.get("uId"), buyRequest.get("tId")
                
                stocksInThisTransaction = min(numberOfStocksRequired, numberOfSharesAvailableToSell)
                numberOfSharesAvailableToSell -= stocksInThisTransaction
                numberOfStocksRequired -= stocksInThisTransaction
                totalSharesSold += stocksInThisTransaction

                priceOfThisTransaction = stocksInThisTransaction * buyingPrice
                totalAmountRecieved += priceOfThisTransaction

                if numberOfStocksRequired > 0:
                    buyRequest["quantity"] = numberOfStocksRequired
                    heapq.heappush(transactions["buy"], [-buyingPrice, buyingTimeStamp, buyRequest])
                
                internalTransactionRequest = {
                    "stockId": stockId,
                    "sellerId": sellerId,
                    "sellerTid": sellerTid,
                    "buyerId": buyerId,
                    "buyerTid": buyerTid,
                    "noOfStocks": stocksInThisTransaction,
                    "amount": buyingPrice,
                    "timeStamp": timeStamp
                }

                dbTransactionRequest = {
                    "tId": buyerTid,
                    "uId": buyerId,
                    "stockId": stockId,
                    "side": "buy",
                    "orderType": "ioc",
                    "quantity": stocksInThisTransaction,
                    "pricePerUnit": buyingPrice,
                    "status": "PARTIAL" if numberOfStocksRequired > 0 else "COMPLETED",
                    "timeStamp": timeStamp
                }

                userTransactionRequest = {
                    "action": "add",
                    "resource": "stock",
                    "stockId": stockId,
                    "uId": buyerId,
                    "quantity": stocksInThisTransaction
                }

                internalTransactions.append(internalTransactionRequest)
                dbTransactions.append(dbTransactionRequest)
                userTransactions.append(userTransactionRequest)
            
            userTransactionRequest = {
                "action": "add",
                "resource": "money",
                "uId": sellerId,
                "quantity": totalAmountRecieved
            }

            dbTransactionRequest = {
                "tId": sellerTid,
                "uId": sellerId,
                "stockId": stockId,
                "side": "sell",
                "orderType": "ioc",
                "quantity": totalSharesSold,
                "pricePerUnit": totalAmountRecieved / totalSharesSold if totalSharesSold > 0 else 0,
                "status": "PARTIAL" if numberOfSharesAvailableToSell > 0 else "COMPLETED",
                "timeStamp": timeStamp
            }
            dbTransactions.append(dbTransactionRequest)
            userTransactions.append(userTransactionRequest)

            if numberOfSharesAvailableToSell > 0:
                userTransactionRequest = {
                    "action": "add",
                    "resource": "stock",
                    "uId": sellerId,
                    "quantity": numberOfSharesAvailableToSell,
                    "stockId": stockId
                }
                userTransactions.append(userTransactionRequest)

            return internalTransactions, dbTransactions, userTransactions

    def fokTransaction(request):
        nonlocal transactions, stockId
        side = request.get("side")
        internalTransactions = []; dbTransactions = []; userTransactions = []
        timeStamp = time.time()

        if side == "buy":
            buyerId, buyerTid = request.get("uId"), request.get("tId")
            numberOfSharesRequired = request.get("quantity"); buyingPrice = request.get("pricePerUnit")
            totalAmountRequired = totalNumberOfSharesRecieved = 0


            if not (transactions["sell"] and transactions["sell"][0][0] <= buyingPrice and sum(tx["quantity"] for price, ts, tx in transactions["sell"] if price == transactions["sell"][0][0]) >= numberOfSharesRequired):
                dbTransactionRequest = {
                    "tId": buyerTid,
                    "uId": buyerId,
                    "stockId": stockId,
                    "side": "buy",
                    "orderType": "fok",
                    "quantity": numberOfSharesRequired,
                    "pricePerUnit": buyingPrice,
                    "status": "IN-COMPLETE",
                    "timeStamp": timeStamp
                }

                userTransactionRequest = {
                    "action": "add",
                    "resource": "money",
                    "uId": buyerId,
                    "quantity": numberOfSharesRequired * buyingPrice
                }

                return [], [dbTransactionRequest], [userTransactionRequest]
            
            tempTransactions = []
            fokPrice = transactions["sell"][0][0]
            while numberOfSharesRequired > 0 and len(transactions["sell"]) > 0 and transactions["sell"][0][0] == fokPrice and transactions["sell"][0][0] <= buyingPrice:
                sellingPrice, sellingTimeStamp, sellRequest = heapq.heappop(transactions["sell"])
                numberOfStocksToSell = sellRequest.get("quantity")

                stocksInThisTransaction = min(numberOfStocksToSell, numberOfSharesRequired)
                numberOfSharesRequired -= stocksInThisTransaction
                numberOfStocksToSell -= stocksInThisTransaction

                totalNumberOfSharesRecieved += stocksInThisTransaction
                priceOfThisTransaction = stocksInThisTransaction * sellingPrice
                totalAmountRequired += priceOfThisTransaction

                if numberOfSharesRequired > 0:
                    # If it comes to this part, it means that the numberofSharesRequired are zero so all the modified transaction to the tempTransactions
                    sellRequest["quantity"] = numberOfStocksToSell
                    heapq.heappush(transactions["sell"], [sellingPrice, sellingTimeStamp, sellRequest])
                    tempTransactions.append(("PARTIAL", stocksInThisTransaction, sellingPrice, sellingTimeStamp, sellRequest))

                tempTransactions.append(("COMPLETED", stocksInThisTransaction, sellingPrice, sellingTimeStamp, sellRequest))


            if numberOfSharesRequired != 0:
                # All requests are satisfied
                for transaction in tempTransactions:
                    heapq.heappush(transactions["sell"], transaction[-2:])
                
                dbTransactionRequest = {
                    "tId": buyerTid,
                    "uId": buyerId,
                    "stockId": stockId,
                    "side": "buy",
                    "orderType": "fok",
                    "quantity": numberOfSharesRequired + totalNumberOfSharesRecieved,
                    "pricePerUnit": buyingPrice,
                    "status": "IN-COMPLETE",
                    "timeStamp": timeStamp
                }

                userTransactionRequest = {
                    "action": "add",
                    "resource": "money",
                    "uId": buyerId,
                    "quantity": buyingPrice * (numberOfSharesRequired + totalNumberOfSharesRecieved)
                }

                return [], [dbTransactionRequest], [userTransactionRequest]

            else:
                for transaction in tempTransactions:
                    status, stocksInThisTransaction, sellingPrice, sellingTimeStamp, sellRequest = transaction
                    sellerId, sellerTid = sellRequest.get("uId"), sellRequest.get("tId")
                    internalTransactionRequest = {
                        "stockId": stockId,
                        "sellerId": sellerId,
                        "sellerTid": sellerTid,
                        "buyerId": buyerId,
                        "buyerTid": buyerTid,
                        "noOfStocks": stocksInThisTransaction,
                        "amount": stocksInThisTransaction * sellingPrice,
                        "timeStamp": timeStamp
                    }
                    dbTransactionRequest = {
                        "tId": sellerTid,
                        "uId": sellerId,
                        "stockId": stockId,
                        "side": "sell",
                        "orderType": "fok",
                        "quantity": stocksInThisTransaction,
                        "pricePerUnit": sellingPrice,
                        "status": status,
                        "timeStamp": timeStamp
                    }
                    userTransactionRequest = {
                        "action": "add",
                        "resource": "money",
                        "uId": sellerId,
                        "quantity": stocksInThisTransaction * sellingPrice
                    }

                    internalTransactions.append(internalTransactionRequest)
                    dbTransactions.append(dbTransactionRequest)
                    userTransactions.append(userTransactionRequest)
                
                dbTransactionRequest = {
                    "tId": buyerTid,
                    "uId": buyerId,
                    "stockId": stockId,
                    "side": "buy",
                    "orderType": "fok",
                    "quantity": totalNumberOfSharesRecieved,
                    "pricePerUnit": totalAmountRequired / totalNumberOfSharesRecieved if totalNumberOfSharesRecieved else 0,
                    "status": "COMPLETED",
                    "timeStamp": timeStamp
                }
                dbTransactions.append(dbTransactionRequest)

                userTransactionRequest = {
                    "action": "add",
                    "resource": "money",
                    "uId": buyerId,
                    "quantity": totalNumberOfSharesRecieved
                }
                userTransactions.append(userTransactionRequest)

                priceDiff = buyingPrice * totalNumberOfSharesRecieved - totalAmountRequired
                if priceDiff != 0:
                    userTransactionRequest = {
                        "action": "add" if priceDiff > 0 else "remove",
                        "resource": "money",
                        "uId": buyerId,
                        "quantity": abs(priceDiff)
                    }
                    userTransactions.append(userTransactionRequest)

            return internalTransactions, dbTransactions, userTransactions
        
        else:
            sellerId, sellerTid = request.get("uId"), request.get("tId")
            numberOfSharesAvailable = request.get("quantity"); sellingPrice = request.get("pricePerUnit")
            totalSharesSold = totalAmountRecieved = 0
            if not (transactions["buy"] and -transactions["buy"][0][0] >= sellingPrice and sum(tx["quantity"] for price, ts, tx in transactions["buy"] if price == transactions["buy"][0][0]) >= numberOfSharesAvailable):
                dbTransactionRequest = {
                    "tId": sellerTid,
                    "uId": sellerId,
                    "stockId": stockId,
                    "side": "sell",
                    "orderType": "fok",
                    "quantity": numberOfSharesAvailable,
                    "pricePerUnit": sellingPrice,
                    "status": "IN-COMPLETE",
                    "timeStamp": timeStamp
                }

                userTransactionRequest = {
                    "action": "add",
                    "resource": "stock",
                    "stock": stockId,
                    "uId": sellerId,
                    "quantity": numberOfSharesAvailable * sellingPrice
                }

                return [], [dbTransactionRequest], [userTransactionRequest]
        
            tempTransactions = []
            fokPrice = -transactions["buy"][0][0]

            while numberOfSharesAvailable > 0 and len(transactions["buy"]) > 0 and transactions["buy"][0][0] == fokPrice and -sellingPrice <= transactions["buy"][0][0]:
                buyingPrice, buyingTimeStamp, buyRequest = heapq.heappop(transactions["buy"])
                buyingPrice *= -1
                buyerId, buyerTid = buyRequest.get("uId"), buyRequest.get("tId")
                numberOfSharesRequired = buyRequest.get("quantity")

                stocksInThisTransaction = min(numberOfSharesAvailable, numberOfSharesRequired)
                numberOfSharesRequired -= stocksInThisTransaction
                numberOfSharesAvailable -= stocksInThisTransaction
                totalSharesSold += stocksInThisTransaction

                priceOfThisTransaction = stocksInThisTransaction * buyingPrice
                totalAmountRecieved += priceOfThisTransaction

                if numberOfSharesRequired > 0:
                    buyRequest["quantity"] = numberOfSharesRequired
                    heapq.heappush(transactions["buy"], [-buyingPrice, buyingTimeStamp, buyRequest])
                    tempTransactions.append(("PARTIAL", stocksInThisTransaction, buyingPrice, buyingTimeStamp, buyRequest))
                
                tempTransactions.append(("COMPLETED", stocksInThisTransaction, buyingPrice, buyingTimeStamp, buyRequest))
            
            if numberOfSharesAvailable != 0:
                for transaction in tempTransactions:
                    heapq.heappush(transactions["buy"], transaction[-2:])
                
                dbTransactionRequest = {
                    "tId": sellerTid,
                    "uId": sellerId,
                    "stockId": stockId,
                    "side": "sell",
                    "orderType": "fok",
                    "quantity": totalSharesSold + numberOfSharesAvailable,
                    "pricePerUnit": sellingPrice,
                    "status": "IN-COMPLETE",
                    "timeStamp": timeStamp
                }
                userTransactionRequest = {
                    "action": "add",
                    "resource": "stock",
                    "stock": stockId,
                    "uId": sellerId,
                    "quantity": totalSharesSold + numberOfSharesAvailable
                }

                return [], [dbTransactionRequest], [userTransactionRequest]

            for transaction in tempTransactions:
                status, stocksInThisTransaction, buyingPrice, buyTimeStamp, buyRequest = transaction
                buyerId, buyerTid = buyRequest.get("uId"), buyRequest.get("tId")

                internalTransactionRequest = {
                    "stockId": stockId,
                    "sellerId": sellerId,
                    "sellerTid": sellerTid,
                    "buyerId": buyerId,
                    "buyerTid": buyerTid,
                    "noOfStocks": stocksInThisTransaction,
                    "amount": stocksInThisTransaction * buyingPrice,
                    "timeStamp": timeStamp
                }
                dbTransactionRequest = {
                    "tId": buyerTid,
                    "uId": buyerId,
                    "stockId": stockId,
                    "side": "buy",
                    "orderType": "fok",
                    "quantity": stocksInThisTransaction,
                    "pricePerUnit": buyingPrice,
                    "status": status,
                    "timeStamp": timeStamp
                }
                userTransactionRequest = {
                    "action": "add",
                    "resource": "stock",
                    "stockId": stockId,
                    "uId": buyerId,
                    "quantity": stocksInThisTransaction * buyingPrice
                }

                internalTransactions.append(internalTransactionRequest)
                dbTransactions.append(dbTransactionRequest)
                userTransactions.append(userTransactionRequest)


            dbTransactionRequest = {
                "tId": sellerTid,
                "uId": sellerId,
                "stockId": stockId,
                "side": "sell",
                "orderType": "fok",
                "quantity": totalSharesSold,
                "pricePerUnit": totalAmountRecieved / totalSharesSold if totalSharesSold else 0,
                "status": "COMPLETED",
                "timeStamp": timeStamp
            }
            dbTransactions.append(dbTransactionRequest)

            userTransactionRequest = {
                "action": "add",
                "resource": "money",
                "uId": sellerId,
                "quantity": totalAmountRecieved
            }
            userTransactions.append(userTransactionRequest)

            return internalTransactions, dbTransactions, userTransactions


    transactions = {"buy":[], "sell": [], "marketPrice": 0.0}
    try:
        pastTransactions = loadTransactions(stockId)
        if  pastTransactions:
            transactions = pastTransactions
            
        mainTransactions["data"] = transactions
        while not shutdownEvent.is_set():
            request = None
            try:
                request = queue.get(timeout=0.01)
            except QueueEmpty as qe:
                # print("No Transaction recieved, Lets wait!")
                continue
            
            if request is None:
                continue
            orderType = request.get("orderType")
            internalTxns, dbTxns, userTxns =[], [], []
            if orderType == "limit":
                internalTxns, dbTxns, userTxns = limitTransaction(request)
            elif orderType == "market":
                internalTxns, dbTxns, userTxns = marketTransaction(request)
            elif orderType == "ioc":
                internalTxns, dbTxns, userTxns = iocTransaction(request)
            elif orderType == "fok":
                internalTxns, dbTxns, userTxns = fokTransaction(request)
            else:
                # Un-known order type
                print("Recieved Unknown order type: ", request)
            
            mainTransactions["data"] = transactions

            print("Request: ", request)
            print(transactions)
            print(internalTxns, dbTxns, userTxns)
            if dbTxns:
                for txn in dbTxns:
                    dbQueue.put(txn)
            
            if internalTxns:
                for txn in internalTxns:
                    iTQueue.put(txn)
            
            if userTxns:
                for txn in userTxns:
                    if isinstance(txn, list):
                        txn = txn[0] # type: ignore
                    uId = txn.get("uId")
                    userData = users[uId]
                    resource = txn.get("resource")
                    if resource == "stock":
                        action = txn.get("action")
                        stockId = txn.get("stockId")
                        quantity = txn.get("quantity")
                        if action == "add":    
                            if stockId in userData["stocks"]:
                                userData["stocks"][stockId] += quantity
                            else:
                                userData["stocks"][stockId] = quantity
                        else:
                            if stockId in userData["stocks"]:
                                userData["stocks"][stockId] -= quantity
                            else:
                                userData["stocks"][stockId] = -quantity
                    
                    else:
                        action = txn.get("action")
                        amount = txn.get("quantity")
                        if action == "add":
                            userData["walletBalance"] += amount
                        else:
                            userData["walletBalance"] -= amount
                    users[uId] = userData

    except Exception as _e:
        logQueue.put(f"Exception at transaction-engine {stockId}: {str(_e)}")
        print("Exception :", str(_e))
        print("Traceback: ", traceback.format_exc())
    
    finally:
        writeTransactions(stockId, transactions)
        print("Exiting Matching Engine - ", stockId)
