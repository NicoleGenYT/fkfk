from fastapi import FastAPI, WebSocket
import asyncio
import time
import json

app = FastAPI()

# Ключ – hwid клиента
clients = {}          # hwid -> websocket
viewers = {}          # viewer_id -> websocket
client_last_ping = {} # hwid -> timestamp
client_info = {}      # hwid -> bytes (последнее info-сообщение в JSON)
lock = asyncio.Lock()

async def check_clients():
    while True:
        now = time.time()
        async with lock:
            dead = [hwid for hwid, last in client_last_ping.items() if now - last > 20]
            for hwid in dead:
                ws = clients.pop(hwid, None)
                client_last_ping.pop(hwid, None)
                client_info.pop(hwid, None)
                if ws:
                    try:
                        await ws.close()
                    except:
                        pass
                msg = json.dumps({"type": "disconnect", "hwid": hwid}).encode()
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
    vid = str(id(websocket))  # уникальный ID для вьювера (не важен)
    
    if role == "client":
        hwid = None
        try:
            while True:
                data = await websocket.receive_bytes()
                
                # Попытка распарсить JSON – чтобы извлечь hwid и сохранить info
                try:
                    msg = json.loads(data.decode())
                    if isinstance(msg, dict):
                        # Сохраняем hwid при первом info-сообщении
                        if msg.get("type") == "info":
                            hwid = msg.get("hwid", "")
                            if hwid:
                                async with lock:
                                    clients[hwid] = websocket
                                    client_last_ping[hwid] = time.time()
                                    client_info[hwid] = data
                                # Рассылаем вьюверам
                                tasks = [safe_send(v, data) for v in viewers.values()]
                                if tasks:
                                    await asyncio.gather(*tasks)
                                continue
                        # Другие JSON-сообщения (audio_devices_list, audio_error и т.д.)
                        if hwid:
                            async with lock:
                                client_last_ping[hwid] = time.time()
                            tasks = [safe_send(v, data) for v in viewers.values()]
                            if tasks:
                                await asyncio.gather(*tasks)
                        continue
                except:
                    pass

                # Бинарные данные (видео, аудио) – передаём как есть
                if hwid:
                    async with lock:
                        client_last_ping[hwid] = time.time()
                    tasks = [safe_send(v, data) for v in viewers.values()]
                    if tasks:
                        await asyncio.gather(*tasks)
        except:
            pass
        finally:
            if hwid:
                async with lock:
                    clients.pop(hwid, None)
                    client_last_ping.pop(hwid, None)
                    client_info.pop(hwid, None)
                msg = json.dumps({"type": "disconnect", "hwid": hwid}).encode()
                tasks = [safe_send(v, msg) for v in viewers.values()]
                if tasks:
                    await asyncio.gather(*tasks)
            
    elif role == "viewer":
        async with lock:
            viewers[vid] = websocket
        # Отправить все сохранённые info-сообщения
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
                # Команды от вьювера (могут быть JSON с target)
                try:
                    msg = json.loads(data.decode())
                    if msg.get("type") == "get_clients":
                        # Повторно отправляем все info
                        async with lock:
                            info_list = list(client_info.values())
                        for info_data in info_list:
                            try:
                                await websocket.send_bytes(info_data)
                            except:
                                break
                        continue
                    # Пересылка команды конкретному клиенту по hwid
                    target_hwid = msg.get("target")
                    if target_hwid:
                        async with lock:
                            target_ws = clients.get(target_hwid)
                        if target_ws:
                            try:
                                # Убираем target перед отправкой, чтобы клиент не смущался
                                del msg["target"]
                                await target_ws.send_bytes(json.dumps(msg).encode())
                            except:
                                async with lock:
                                    if target_hwid in clients:
                                        del clients[target_hwid]
                except:
                    pass
        except:
            pass
        finally:
            async with lock:
                viewers.pop(vid, None)
