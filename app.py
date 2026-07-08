# server.py
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
lock = asyncio.Lock()

async def check_clients():
    while True:
        now = time.time()
        async with lock:
            dead = [cid for cid, last in client_last_ping.items() if now - last > 20]
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
                tasks = [safe_send(v, msg) for v in viewers.values()]
                if tasks:
                    await asyncio.gather(*tasks)
        await asyncio.sleep(5)

async def safe_send(ws, data):
    try:
        await ws.send_bytes(data)
        return True
    except:
        async with lock:
            for vid, v in list(viewers.items()):
                if v == ws:
                    del viewers[vid]
                    break
        return False

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
        async with lock:
            clients[cid] = websocket
            client_last_ping[cid] = time.time()
        try:
            while True:
                data = await websocket.receive_bytes()
                async with lock:
                    client_last_ping[cid] = time.time()
                
                if data == b'ping':
                    continue
                
                try:
                    msg = json.loads(data.decode())
                    if isinstance(msg, dict):
                        msg["client_id"] = cid
                        async with lock:
                            client_info[cid] = json.dumps(msg).encode()
                        data = client_info[cid]
                except:
                    cid_bytes = cid.encode()
                    prefix = len(cid_bytes).to_bytes(2, 'big') + cid_bytes + b'|'
                    data = prefix + data
                
                tasks = [safe_send(v, data) for v in viewers.values()]
                if tasks:
                    await asyncio.gather(*tasks)
        except:
            pass
        finally:
            async with lock:
                clients.pop(cid, None)
                client_last_ping.pop(cid, None)
                client_info.pop(cid, None)
            msg = json.dumps({"type": "disconnect", "client_id": cid}).encode()
            tasks = [safe_send(v, msg) for v in viewers.values()]
            if tasks:
                await asyncio.gather(*tasks)
            
    elif role == "viewer":
        async with lock:
            viewers[cid] = websocket
        async with lock:
            info_list = list(client_info.values())
        for info_data in info_list:
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
                        target_cid = msg.pop("target")
                        clean_msg = json.dumps(msg).encode()
                        async with lock:
                            target_ws = clients.get(target_cid)
                        if target_ws:
                            try:
                                await target_ws.send_bytes(clean_msg)
                            except:
                                async with lock:
                                    if target_cid in clients:
                                        del clients[target_cid]
                except:
                    pass
        except:
            pass
        finally:
            async with lock:
                viewers.pop(cid, None)
