import requests

URL = "https://shaggy-lorna-melody-now-eea13251.koyeb.app/listeners"

data = requests.get(URL, timeout=10, headers={"Accept":"application/json"}).json()
print(f"{data['listeners']}")
