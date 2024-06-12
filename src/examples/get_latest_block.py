import os
from dotenv import load_dotenv
import requests
import json

load_dotenv()

url = os.getenv("QUICK_NODE_URL")
payload = json.dumps(
    {"method": "eth_blockNumber", "params": [], "id": 1, "jsonrpc": "2.0"}
)
headers = {"Content-Type": "application/json"}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
