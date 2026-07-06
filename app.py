from fastapi import FastAPI, WebSocket
import asyncio
import time
import json
import uuid

app = FastAPI()
clients = {}
viewers = {}
client_last_ping = {}
client_info = {}

async def check_clients():
    while True:
        now = time.time()
        dead = []
        for cid, last in list(client_last_ping.items()):
            if now - last > 20:
                dead.append(cid)
        for cid in dead:
            ws = clients.pop(cid, None)
            client_last_ping.pop(cid, None)
            client_info.pop(cid, None)
            if ws:
                try:
                    await ws.close()
                except:
                    pass
            msg = json.dumps({"type": "disconnect", "client_id": cid}).encode()
            for v in list(viewers.values()):
                try:
                    await v.send_bytes(msg)
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
    cid = str(uuid.uuid4())[:8]
    
    if role == "client":
        clients[cid] = websocket
        client_last_ping[cid] = time.time()
        try:
            while True:
                data = await websocket.receive_bytes()
                client_last_ping[cid] = time.time()
                
                if data == b'ping':
                    continue
                
                try:
                    msg = json.loads(data.decode())
                    if isinstance(msg, dict):
                        msg["client_id"] = cid
                        client_info[cid] = json.dumps(msg).encode()
                        data = client_info[cid]
                except:
                    cid_bytes = cid.encode()
                    prefix = len(cid_bytes).to_bytes(2, 'big') + cid_bytes + b'|'
                    data = prefix + data
                
                for v in list(viewers.values()):
                    try:
                        await v.send_bytes(data)
                    except:
                        pass
        except:
            pass
        finally:
            clients.pop(cid, None)
            client_last_ping.pop(cid, None)
            client_info.pop(cid, None)
            msg = json.dumps({"type": "disconnect", "client_id": cid}).encode()
            for v in list(viewers.values()):
                try:
                    await v.send_bytes(msg)
                except:
                    pass
            
    elif role == "viewer":
        viewers[cid] = websocket
        for info_data in client_info.values():
            try:
                await websocket.send_bytes(info_data)
            except:
                pass
        try:
            while True:
                data = await websocket.receive_bytes()
                try:
                    msg = json.loads(data.decode())
                    if isinstance(msg, dict) and "target" in msg:
                        target_cid = msg.pop("target")  # Убираем target из сообщения
                        if target_cid in clients:
                            # Пересылаем БЕЗ поля target
                            clean_msg = json.dumps(msg).encode()
                            try:
                                await clients[target_cid].send_bytes(clean_msg)
                            except:
                                pass
                except:
                    pass
        except:
            pass
        finally:
            viewers.pop(cid, None)
