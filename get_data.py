import asyncio, json, websockets

URL = "wss://shaggy-lorna-melody-now-eea13251.koyeb.app/ws/now/"  # ← wss://

async def run():
    while True:
        try:
            async with websockets.connect(
                URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
            ) as ws:
                async for msg in ws:
                    data = json.loads(msg)
                    print(f"{data['station']} — {data['artist']} — {data['title']} | "
                          f"{data['date']} {data['time']} | poslucháči: {data['listeners']}")
        except Exception as e:
            print("WS reconnect in 3s ->", e)
            await asyncio.sleep(3)

asyncio.run(run())
