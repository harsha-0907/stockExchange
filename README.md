# CryptoExchange

## A Leading Cryptocurrency exchange platform

### ABOUT THIS PROJECT (FEATURES)
1. Built Matching Engine with REG-NMS principles in consideration
2. Internal Order Protection & Time-Based Priority
3. Supports multiple order type(market/limit/ioc/fok) with support for future integration
4. High-Performance using multi-processing
5. Ultra-Low latency of upto 0.0028 ms or 2.86 microseconds
6. Ability to add new stocks dynamically


### Architecture
Micro-Service Architecture

### Steps to Install
1. Clone the repository `git clone https://github.com/harsha-0907/stockExchange.git`
2. Change directory to the repository `cd stockExchange`
3. Create Virtual Environment `python3 -m venv .venv`
4. Activate the virtual environment:
    a. For Linux `source .venv/bin/activate`
    b. For Windows `.\.venv\Scripts\activate`
5. Install the necessary packages using `pip install -r requirments.txt`
6. Run the code using `python3 app.py`

Now Head to `http://localhost:8000/docs` in your 

### Benchmarking:
    Remote Client:
        Number of Transactions : 1000
        Successful Transactions: 1000
        Rejected Transactions: 0
        Total Time Taken: 19.904207468032837
        Average Time Per Request: 190 ms

    Server-Side:
        Average Time     : 2.502 µs
        Total Time       : 2501.653 µs
        Total Requests   : 1000


Feel Free to add a million to your account :)
