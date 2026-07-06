from fastapi import FastAPI, WebSocket
import asyncio
import time
import json

app = FastAPI()
clients = {}  # client_id -> websocket
viewers = {}  # viewer_id -> websocket
client_last_ping = {}  # client_id -> timestamp

async def check_clients():
    global clients, client_last_ping, viewers
    while True:
        dead = []
        for cid, ws in clients.items():
            if time.time() - client_last_ping.get(cid, 0) > 20:
                dead.append(cid)
                # Уведомляем всех viewer'ов
                for v in viewers.values():
                    try:
                        await v.send_bytes(json.dumps({"type": "disconnect", "client_id": cid}).encode())
                    except:
                        pass
        for cid in dead:
            if cid in clients:
                del clients[cid]
            if cid in client_last_ping:
                del client_last_ping[cid]
        await asyncio.sleep(5)

@app.on_event("startup")
async def startup():
    asyncio.create_task(check_clients())

@app.get("/")
def index():
    return {"status": "OK"}

@app.websocket("/ws")
async def ws(websocket: WebSocket):
    global clients, viewers, client_last_ping
    await websocket.accept()
    
    role = websocket.query_params.get("role", "")
    cid = str(id(websocket))
    
    if role == "client":
        clients[cid] = websocket
        client_last_ping[cid] = time.time()
        try:
            while True:
                data = await websocket.receive_bytes()
                if data == b'ping':
                    client_last_ping[cid] = time.time()
                else:
                    # Пересылаем всем viewer'ам
                    for v in viewers.values():
                        try:
                            await v.send_bytes(data)
                        except:
                            pass
        except:
            if cid in clients:
                del clients[cid]
            if cid in client_last_ping:
                del client_last_ping[cid]
            for v in viewers.values():
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
