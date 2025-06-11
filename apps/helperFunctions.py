
from datetime import datetime

def fetchTimeStamp():
    now = datetime.now()
    formattedTime = now.strftime("%d/%m/%Y %H:%M:%S")
    return formattedTime


defaultResponses = {
    200: {
        "statusCode": 200,
        "description": "Success",
        "resource": "",
        "state": ""
    },
    401: {
        "statusCode": 401,
        "description": "User Not Authenticated",
        "resource": "Input",
        "state": "User Not Present"
    },
    403: {
        "statusCode": 403,
        "description": "Transaction Failed",
        "resource": "wallet",
        "state": "Insufficient funds in the wallet"
    },
    404: {
        "statusCode": 404,
        "description": "Resource/Stock Not Found",
        "resource": "stock/resource",
        "state": "Stock/Resource Not Found"
    },
    422: {
        "statusCode": 422,
        "description": "Validation Error",
        "resource": "Input",
        "state": "Invalid Input"
    },
    500: {
        "statusCode": 500,
        "description": "Internal Server Error",
        "resource": "",
        "state": ""
    }
}

def formatResponse(statusCode, description: str = None, resource: str = None, state: str = None):
    templateResponse = defaultResponses[statusCode].copy()
    if description:
        templateResponse["description"] = description
    if resource:
        templateResponse["resource"] = resource
    if state:
        templateResponse["state"] = state
    
    return templateResponse

