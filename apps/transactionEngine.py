import time
from queue import Empty as QueueEmpty
from multiprocessing import Queue, Manager, Process, Event
from apps.database import transactionDb, internalTransactionDb
from apps.matchingEngine import matchingEngine


class StockAggregator:
    def __init__(self):
        self.numberOfUsers = 0
        self.shutdownEvent = Event()
        self.isWorking = True
        self.tradedStocks = []
        self.manager = Manager()
        self.users = self.manager.dict()
        self.stockQueues = self.manager.dict()  # ✅ Shared across processes

    def initializeQueues(self):
        self.dbQueue = Queue()
        self.logQueue = Queue()
        self.transactionQueue = Queue()

    def initializeProcesses(self):
        def initializeDBProcess():
            def updateTransaction(queue: Queue, transactionsDb, logQueue, shutdownEvent):
                transactionBatch = []; startTime = time.time()
                try:
                    while not shutdownEvent.is_set():
                        try:
                            transactionRequest = queue.get(timeout=0.01)
                            transactionBatch.append(transactionRequest)
                        except QueueEmpty:
                            pass

                        if (time.time() - startTime > 0.1 and transactionBatch) or len(transactionBatch) > 100:
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

                        action = request.get("action")
                        if action == "transaction":
                            stockId = request.get("stockId")
                            if stockId in stockQueues:
                                stockQueues[stockId].put(request)
                        elif action == "addStock":
                            # No need to do anything here; stockQueues already updated
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

                        if (time.time() - startTime > 0.1 and transactionBatch) or len(transactionBatch) > 100:
                            internalTransactionsDb.insert_multiple(transactionBatch)
                            transactionBatch = []
                            startTime = time.time()
                except Exception as e:
                    logQueue.put("Internal-Transaction: " + str(e))
                finally:
                    if transactionBatch:
                        internalTransactionsDb.insert_multiple(transactionBatch)

            process = Process(target=internalTransactions,
                              args=(self.transactionQueue, internalTransactionDb, self.logQueue, self.shutdownEvent))
            process.start()
            self.processes.append(process)

        self.processes = []
        self.stockProcesses = {}
        initializeDBProcess()
        initializeSegregator()
        initializeInternalTransactionProcess()

    def stopProcesses(self):
        self.shutdownEvent.set()
        time.sleep(0.5)
        for process in self.processes:
            process.terminate()
            process.join()


class TransactionEngine(StockAggregator):
    def __init__(self, initialStocks=["btc"]):
        super().__init__()
        self.initializeQueues()
        self.initializeProcesses()
        for stockId in initialStocks:
            print("New Stock", stockId)
            self.isWorking = self.addStock(stockId) and self.isWorking

    def addNewProcess(self, stockId, stockQueue, logQueue, internalQueue, dbQueue):
        process = Process(target=matchingEngine,
                          args=(stockId, stockQueue, dbQueue, internalQueue, logQueue, self.users, self.shutdownEvent))
        process.start()
        self.processes.append(process)
        return True

    def addStock(self, stockId):
        if stockId in self.tradedStocks:
            return False

        stockQueue = self.manager.Queue()
        self.stockQueues[stockId] = stockQueue  # ✅ Shared dict is updated here

        request = {
            "action": "addStock",
            "stockId": stockId  # ✅ Only send stockId — not the queue
        }
        self.transactionQueue.put(request)

        self.tradedStocks.append(stockId)
        return True


me = TransactionEngine()
