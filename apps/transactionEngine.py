
import time
from queue import Empty as QueueEmpty
from multiprocessing import Queue, Manager, Process, Event
from apps.database import transactionDb, internalTransactionDb


class StockAggregator:
    """
        Responsible for segregating stocks and helping the transaction engine to process them.
        Functions:
            1. Update the `transactions` table
            2. 
    """
    def __init__(self, initialStocks=["btc"]):
        self.numberOfUsers = 0
        self.shutdownEvent = Event()
        self.isWorking = True
        self.tradedStocks = []
        self.manager = Manager()
        self.users = Manager().dict()
        self.initializeQueues()
        self.initializeProcesses()
        for stockId in initialStocks:
            print("New Stock", stockId)
            self.isWorking = self.addStock(stockId) and self.isWorking

    def initializeQueues(self):
        self.dbQueue = Queue()  # Inserts transaction to the table
        self.logQueue = Queue()
        self.transactionQueue = Queue() # List of all the new transactions to be processed
        self.stockQueues = {}
    
    def initializeProcesses(self):
        def initializeDBProcess():
            def updateTransaction(queue: Queue, transactionsDb, logQueue, shutdownEvent):
                transactionBatch = []; startTime = time.time()
                try:
                    while True:
                        try:
                            if shutdownEvent.is_set():
                                break
                            transactionRequest = queue.get(timeout=0.01)
                            transactionBatch.append(transactionRequest)
                        
                        except QueueEmpty as _qe:
                            pass

                        if (time.time() - startTime > 0.1 and len(transactionBatch) > 0) or len(transactionBatch) > 100:
                            transactionDb.insert_multiple(transactionBatch)
                            transactionBatch = []
                            startTime = time.time()

                except Exception as _e:
                    print("Error has occured", str(_e))
                    logQueue.put("Update-Transaction" + str(_e))
                
                finally:
                    print("Exit Initiated, flushing transactions")
                    if transactionBatch:
                        print("Flushing Transactions")
                        transactionsDb.insert_multiple(transactionBatch)
                    print("Exiting Update-Transaction")

            process = Process(target=updateTransaction, args=(self.dbQueue, transactionDb, self.logQueue, self.shutdownEvent))
            process.start()
            self.processes.append(process)
            return True
        
        def initializeSegregator():
            def segregateTransactions(queue: Queue, logQueue: Queue, shutdownEvent):
                # TO-DO: Add statistics for the number of transactions ocured in each category
                try:
                    stockQueues = {}
                    while True:
                        if shutdownEvent.is_set():
                            break
                        try:
                            request = queue.get(timeout=0.01)
                        except QueueEmpty as _qe:
                            request = None
                            time.sleep(0.1)
                            continue
                        print("requst in sgregator", request)
                        action = request.get("action")
                        if action == "transaction":
                            # Segregate the transaction based on the stockId
                            stockId = request.get("stockId", None)
                            if not stockId:
                                continue
                            print("Stock is present", stockId)
                            stockQueues[stockId].put(request)
                        
                        elif action == "addStock":
                            print(action)
                            stockId = request.get("stockId", None)
                            queueObj = request.get("stockQueue", None)

                            if stockId and queueObj:
                                stockQueues[stockId] = queueObj
                            print(stockQueues)
                        
                        elif action == "removeStock":
                            stockId = request.get("stockId", None)
                            if stockId in stockQueues:
                                del stockQueues[stockId]
                        
                        else:
                            pass
                
                except Exception as _e:
                    print("Error has occured", str(_e))
                    logQueue.put("Segregate-Transactions" + str(_e))
                
                finally:
                    print("Exiting the Segregate-Transactions")

            process = Process(target=segregateTransactions, args=(self.transactionQueue, self.logQueue, self.shutdownEvent))
            process.start()
            self.processes.append(process)
            return True

        def initializeInternalTransactionProcess():
            def internalTransactions(queue: Queue, internalTransactionsDb, logQueue, shutdownEvent):
                transactionBatch = []; startTime = time.time()
                try:
                    while True:
                        try:
                            if shutdownEvent.is_set():
                                break
                            transactionRequest = queue.get(timeout=0.01)
                            transactionBatch.append(transactionRequest)
                        
                        except QueueEmpty as _qe:
                            pass

                        if (time.time() - startTime > 0.1 and len(transactionBatch) > 0) or len(transactionBatch) > 100:
                            transactionDb.insert_multiple(transactionBatch)
                            transactionBatch = []
                            startTime = time.time()

                except Exception as _e:
                    print("Error has occured", str(_e))
                    logQueue.put("Update-Transaction" + str(_e))
                
                finally:
                    print("Exit Initiated, flushing transactions")
                    if transactionBatch:
                        print("Flushing Transactions")
                        internalTransactionDb.insert_multiple(transactionBatch)
                    print("Exiting Update-Transaction")

            process = Process(target=internalTransactions, args=(self.transactionQueue, internalTransactionDb, self.logQueue, self.shutdownEvent))
            process.start()
            self.processes.append(process)
            return True


        self.processes = []; self.stockProcesses = {}
        initializeDBProcess()
        initializeSegregator()
        initializeInternalTransactionProcess()

    def addStock(self, stockId):
        if stockId in self.tradedStocks:
            return False

        stockQueue = self.manager.Queue()
        request = {
            "action": "addStock",
            "stockId": stockId,
            "stockQueue": stockQueue
        }
        self.transactionQueue.put(request)
        self.tradedStocks.append(stockId)
        self.stockQueues[stockId] = stockQueue
        return True

    def stopProcesses(self):
        self.shutdownEvent.set()
        time.sleep(0.5) # TO-DO (Wait for some time to flush all records)
        for process in self.processes:
            process.terminate()
            process.join()

class TransactionEngine(StockAggregator):
    def __init__(self):
        super().__init__()
    
    def addNewProcess(self, stockId, stockQueue, logQueue):
        # This function adds new processes
        pass

me = TransactionEngine()

