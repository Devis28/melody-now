import requests

URL = "https://shaggy-lorna-melody-now-eea13251.koyeb.app/now"

data = requests.get(URL, timeout=10, headers={"Accept":"application/json"}).json()
print(f"{data['station']} | {data['artist']} — {data['title']} | {data['date']} {data['time']} | poslucháči: {data['listeners']}")
