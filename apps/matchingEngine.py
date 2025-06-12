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

    def fokTransaction(request):
        return [], [], []

    def iocTransaction(request):
        
        return [], [], []

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
                        txn = txn[0]
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
