import requests

print(requests.get("http://localhost:8888/authorize").text)
print(requests.get("http://localhost:8888/callback").text)
print(requests.get("http://localhost:8888/").text)