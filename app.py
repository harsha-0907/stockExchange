
import time
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
import apps.database as db
from apps.transactionEngine import me
from apps import user, stock, transactions
import uvicorn
from sys import exit

totalRequests = 0
totalTimeTaken = 0
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting the Engine")
    yield
    me.stopEngine()

app = FastAPI(lifespan=lifespan)

@app.middleware("http")
def calculateProcessingTime(request: Request, call_next):
    global totalRequests, totalTimeTaken
    start = time.perf_counter_ns()
    response = call_next(request)
    end = time.perf_counter_ns()
    totalRequests += 1
    totalTimeTaken += end-start

    print("\n\nProcesing Time - ", end - start)
    print("Average Time - ", totalTimeTaken / totalRequests)
    print("Total Time : ", totalTimeTaken)
    print("Total Requests : ", totalRequests)
    return response

app.include_router(user.router, prefix="/user", tags=["user"])
app.include_router(stock.router, prefix="/stock", tags=["stock"])
app.include_router(transactions.router, prefix="/transaction", tags=["transaction"])

@app.get("/")
def getHomePage():
    return {
        "statusCode": 200,
        "description": "Server Running, Go to /docs for more information",
        "state": "Running",
        "resource": "Server"
    }

if __name__ == "__main__":
    try:
        uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
    except KeyboardInterrupt:
        print("Exiting")
        me.stockProcesses()


