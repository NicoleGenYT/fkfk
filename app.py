from fastapi import FastAPI, WebSocket
import asyncio
import time
import json

app = FastAPI()
clients = {}
viewers = {}
client_last_ping = {}

async def check_clients():
    while True:
        now = time.time()
        dead = []
        for cid, last in list(client_last_ping.items()):
            if now - last > 20:
                dead.append(cid)
        for cid in dead:
            if cid in clients:
                del clients[cid]
            if cid in client_last_ping:
                del client_last_ping[cid]
            for v in list(viewers.values()):
                try:
                    await v.send_bytes(json.dumps({"type": "disconnect", "client_id": cid}).encode())
                except:
                    pass
        await asyncio.sleep(5)

@app.on_event("startup")
async def startup():
    asyncio.create_task(check_clients())

@app.get("/")
def index():
    return {"status": "OK"}

@app.websocket("/ws")
async def ws(websocket: WebSocket):
    await websocket.accept()
    
    role = websocket.query_params.get("role", "")
    cid = str(id(websocket))[:8]
    
    if role == "client":
        clients[cid] = websocket
        client_last_ping[cid] = time.time()
        try:
            while True:
                data = await websocket.receive_bytes()
                # Обновляем время при ЛЮБЫХ данных
                client_last_ping[cid] = time.time()
                
                if data == b'ping':
                    continue  # Не пересылаем пинг viewer'у
                
                # Пытаемся распарсить JSON и добавить client_id
                try:
                    msg = json.loads(data.decode())
                    if isinstance(msg, dict):
                        msg["client_id"] = cid
                        data = json.dumps(msg).encode()
                except:
                    # Бинарные данные (JPEG) — добавляем префикс с client_id
                    prefix = cid.encode() + b'|'
                    data = prefix + data
                
                for v in list(viewers.values()):
                    try:
                        await v.send_bytes(data)
                    except:
                        pass
        except:
            if cid in clients:
                del clients[cid]
            if cid in client_last_ping:
                del client_last_ping[cid]
            for v in list(viewers.values()):
                try:
                    await v.send_bytes(json.dumps({"type": "disconnect", "client_id": cid}).encode())
                except:
                    pass
            
    elif role == "viewer":
        viewers[cid] = websocket
        try:
            while True:
                await websocket.receive_bytes()
        except:
            if cid in viewers:
                del viewers[cid]
