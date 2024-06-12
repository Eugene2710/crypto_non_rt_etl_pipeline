import requests
import json
from dotenv import load_dotenv
import os

# search for .env file in your project, and import them as env vars
load_dotenv()

url: str = os.getenv("QUICK_NODE_URL")

print(url)

payload: str = json.dumps(
    {"method": "eth_blobBaseFee", "params": [], "id": 1, "jsonrpc": "2.0"}
)
headers = {"Content-Type": "application/json"}

response: requests.Response = requests.request(
    "POST", url, headers=headers, data=payload
)

print(response.text)
