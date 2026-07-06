from fastapi import FastAPI, WebSocket
import asyncio
import time
import json
import uuid

app = FastAPI()
clients = {}           # cid -> websocket
viewers = {}           # cid -> websocket
client_last_ping = {}  # cid -> timestamp
client_info = {}       # cid -> info_bytes (последнее info-сообщение клиента)

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
            # Уведомляем viewer'ов
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
    cid = str(uuid.uuid4())[:8]  # Уникальный короткий ID
    
    if role == "client":
        clients[cid] = websocket
        client_last_ping[cid] = time.time()
        try:
            while True:
                data = await websocket.receive_bytes()
                client_last_ping[cid] = time.time()  # любое данное обновляет таймер
                
                if data == b'ping':
                    continue  # пинг не пересылаем
                
                # Пытаемся распарсить JSON (инфо о системе)
                try:
                    msg = json.loads(data.decode())
                    if isinstance(msg, dict):
                        msg["client_id"] = cid
                        # Сохраняем info для отправки новым viewer'ам
                        client_info[cid] = json.dumps(msg).encode()
                        data = client_info[cid]
                except:
                    # Бинарные данные (JPEG) – добавляем префикс с длиной client_id
                    cid_bytes = cid.encode()
                    prefix = len(cid_bytes).to_bytes(2, 'big') + cid_bytes + b'|'
                    data = prefix + data
                
                # Рассылаем всем viewer'ам
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
            # Уведомляем viewer'ов об отключении
            msg = json.dumps({"type": "disconnect", "client_id": cid}).encode()
            for v in list(viewers.values()):
                try:
                    await v.send_bytes(msg)
                except:
                    pass
            
    elif role == "viewer":
        viewers[cid] = websocket
        # Отправляем новому viewer'у информацию о всех активных клиентах
        for info_data in client_info.values():
            try:
                await websocket.send_bytes(info_data)
            except:
                pass
        try:
            while True:
                await websocket.receive_bytes()  # viewer может слать команды, пока просто читаем
        except:
            pass
        finally:
            viewers.pop(cid, None)
