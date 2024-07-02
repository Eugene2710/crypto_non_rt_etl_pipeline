import requests
import json

url = "https://docs-demo.quiknode.pro/"     # "https://stylish-crimson-breeze.quiknode.pro/4c6bc9e96391ab9474c8364cdcbdad953e065a7d/"

payload = json.dumps({
  "method": "eth_accounts",
  "id": 1,
  "jsonrpc": "2.0"
})
headers = {
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
