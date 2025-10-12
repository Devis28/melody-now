import asyncio, json, websockets

URL = "wss://…koyeb.app/ws/now"

async def main():
    async with websockets.connect(URL) as ws:
        async for msg in ws:
            data = json.loads(msg)
            print(f"{data['station']} — {data['artist']} — {data['title']} | {data['date']} {data['time']} | poslucháči: {data['listeners']}")

asyncio.run(main())