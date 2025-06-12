
from fastapi import FastAPI
from contextlib import asynccontextmanager
import apps.database as db
from apps.transactionEngine import me
from apps import user
import uvicorn
from sys import exit

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting the Engine")
    yield
    me.stopProcesses()

app = FastAPI(lifespan=lifespan)
app.include_router(user.router, prefix="/user", tags=["user"])

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

