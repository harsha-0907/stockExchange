
import os
import time
from queue import Empty as QueueEmpty
from multiprocessing import Queue, Manager, Process, Event
from apps.database import transactionDb, internalTransactionDb, userDb
from apps.matchingEngine import matchingEngine

class StockAggregator:
    def __init__(self):
        self.numberOfUsers = 0
        self.shutdownEvent = Event()
        self.isWorking = True
        self.tradedStocks = []
        self.manager = Manager()
        self.users = self.manager.dict()
        self.stockQueues = self.manager.dict()
        self.stockTransactions = self.manager.dict()

    def restoreState(self):
        def restoreUsers():
            try:
                results = userDb.all()
                for result in results:
                    uId = result.get("uId")
                    userData = result.get("userData")
                    self.users[uId] = userData
                self.numberOfUsers = len(self.users)
                print(self.users)
                return True
        
            except Exception as _e:
                self.isWorking = False
                return False

        def restoreStocks():
            _stocks = os.listdir("database/stocks")
            for stock in _stocks:
                stockId = stock.split('.')[0]
                self.addStock(stockId)
                self.tradedStocks.append(stockId)
            return True

        restoreUsers()  
        restoreStocks()

    def initializeQueues(self):
        self.dbQueue = Queue()
        self.logQueue = Queue()
        self.transactionQueue = Queue()
        self.internalTransactionQueue = Queue()

    def initializeProcesses(self):
        def initializeDBProcess():
            def updateTransaction(queue: Queue, transactionsDb, logQueue, shutdownEvent):
                transactionBatch = []; startTime = time.time()
                try:
                    while not shutdownEvent.is_set():
                        try:
                            transactionRequest = queue.get(timeout=0.01)
                            # print(transactionRequest)
                            if transactionRequest:
                                transactionBatch.append(transactionRequest)
                        except QueueEmpty:
                            pass

                        if (time.time() - startTime > 1.0 and len(transactionBatch) > 0) or len(transactionBatch) > 1000:
                            transactionsDb.insert_multiple(transactionBatch)
                            transactionBatch = []
                            startTime = time.time()
                except Exception as e:
                    logQueue.put("Update-Transaction: " + str(e))
                finally:
                    if transactionBatch:
                        transactionsDb.insert_multiple(transactionBatch)

            process = Process(target=updateTransaction, args=(self.dbQueue, transactionDb, self.logQueue, self.shutdownEvent))
            process.start()
            self.processes.append(process)

        def initializeSegregator():
            def segregateTransactions(queue: Queue, stockQueues, logQueue, shutdownEvent):
                try:
                    while not shutdownEvent.is_set():
                        try:
                            request = queue.get(timeout=0.01)
                        except QueueEmpty:
                            time.sleep(0.1)
                            continue

                        # print(request)
                        action = request.get("action")
                        if action == "transaction":
                            stockId = request.get("stockId")
                            if stockId in stockQueues:
                                # print("Recieved request to :", stockId)
                                stockQueues[stockId].put(request)
                                # print("Sent the request")
                        elif action == "remove-transaction":
                            stockId = request.get("stockId")
                            if stockId in stockQueues:
                                stockQueues[stockId].put(request)

                        elif action == "addStock":
                            # No need to do anything here; stockQueues already updated
                            # print("Added New Stock")
                            pass
                        
                        elif action == "removeStock":
                            stockId = request.get("stockId")
                            if stockId in stockQueues:
                                del stockQueues[stockId]
                except Exception as e:
                    logQueue.put("Segregate-Transactions: " + str(e))

            process = Process(target=segregateTransactions,
                              args=(self.transactionQueue, self.stockQueues, self.logQueue, self.shutdownEvent))
            process.start()
            self.processes.append(process)

        def initializeInternalTransactionProcess():
            def internalTransactions(queue: Queue, internalTransactionsDb, logQueue, shutdownEvent):
                transactionBatch = []; startTime = time.time()
                try:
                    while not shutdownEvent.is_set():
                        try:
                            transactionRequest = queue.get(timeout=0.01)
                            transactionBatch.append(transactionRequest)
                        except QueueEmpty:
                            pass

                        if (time.time() - startTime > 1.0 and len(transactionBatch) > 0) or len(transactionBatch) > 1000:
                            internalTransactionsDb.insert_multiple(transactionBatch)
                            transactionBatch = []
                            startTime = time.time()
                except Exception as e:
                    logQueue.put("Internal-Transaction: " + str(e))
                finally:
                    if transactionBatch:
                        internalTransactionsDb.insert_multiple(transactionBatch)

            process = Process(target=internalTransactions,
                              args=(self.internalTransactionQueue, internalTransactionDb, self.logQueue, self.shutdownEvent))
            process.start()
            self.processes.append(process)

        def initializeLogProcess():
            def logTransactions(queue: Queue, shutdownEvent):
                stTime = time.time(); transactionBatch = []
                try:
                    while not shutdownEvent.is_set():
                        try:
                            event = queue.get(timeout=0.1)
                        except QueueEmpty as qe:
                            time.sleep(0.1)
                            continue

                        if event:
                            transactionBatch.append(event + str(time.time()) + '\n')
                        
                        if (time.time() - stTime > 1.0 and transactionBatch) or len(transactionBatch) > 1000:
                            with open("logs/system.log", 'a') as file:
                                file.write('\n'.join(transactionBatch))
                        
                    if len(transactionBatch):
                        with open("logs/system.log", 'a') as file:
                                file.write('\n'.join(transactionBatch))
                except Exception as _e:
                    print("Error in Log Process :", str(_e))

                finally:
                    print("Exiting Log Process")
                    return True
            
            process = Process(target=logTransactions, args=(self.logQueue, self.shutdownEvent))
            process.start()
            self.processes.append(process)

        self.processes = []
        self.stockProcesses = {}
        initializeLogProcess()
        initializeDBProcess()
        initializeSegregator()
        initializeInternalTransactionProcess()

    def stopProcesses(self):
        self.shutdownEvent.set()
        time.sleep(3)
        for process in self.processes:
            process.join(timeout=2)

            if process.is_alive():
                process.terminate()
                process.join()
                print("Force Shutdown")
            else:
                print("Clean Exit")

    def stopEngine(self):
        self.saveData()
        self.stopProcesses()

    def saveData(self):
        def saveUsers():
            try:
                print("Saving User data to Storage")
                userDb.truncate()
                users = self.users
                listOfUsers = []
                for uId, userData in users.items():
                    listOfUsers.append(
                        {
                            "uId": uId,
                            "userData": userData
                        }
                    )
                userDb.insert_multiple(listOfUsers)
                return True
            
            except Exception as _e:
                print("Unable to move data to Storage.\n Data Inconsistent")
                return False
        
        saveUsers()

class TransactionEngine(StockAggregator):
    def __init__(self, newStocks=["btc", "gold", "usd"], minStocks = 100000):
        super().__init__()
        self.initializeQueues()
        self.initializeProcesses()
        self.restoreState()

        # If there are no users create admin user first
        if not self.users:
            self.users["admin"] = {"walletBalance": 10000000, "stocks": {}}

        for stockId in newStocks:
            if stockId not in self.tradedStocks:
                self.addStock(stockId, addUser=True)
        
    
    def addNewProcess(self, stockId, stockQueue, logQueue, internalQueue, dbQueue, stockTransaction):
        process = Process(target=matchingEngine,
                          args=(stockTransaction, stockId, stockQueue, dbQueue, internalQueue, logQueue, self.users, self.shutdownEvent))
        process.start()
        self.processes.append(process)
        return True

    def addStock(self, stockId, addUser: bool = False):
        if stockId in self.tradedStocks:
            return False

        stockQueue = self.manager.Queue()
        self.stockQueues[stockId] = stockQueue
        stockTransaction = self.manager.dict()
        self.stockTransactions[stockId] = stockTransaction

        request = {
            "action": "addStock",
            "stockId": stockId
        }
        self.transactionQueue.put(request)
        self.tradedStocks.append(stockId)
        self.addNewProcess(stockId, stockQueue, self.logQueue, self.internalTransactionQueue, self.dbQueue, stockTransaction)
        
        if addUser:
            initRequest = {
                "tId": "1234567890123",
                "uId": "admin",
                "stockId": stockId,
                "side": "sell",
                "orderType": "limit",
                "quantity": 100000,
                "pricePerUnit": 100,
                "status": "RECIEVED",
                "action": "transaction",
                "timeStamp": time.time()
            }
            self.dbQueue.put(initRequest)
            self.transactionQueue.put(initRequest)
            adminData = self.users["admin"]
            adminData["stocks"][stockId] = 0
            self.users["admin"] = adminData
        return True

me = TransactionEngine()
