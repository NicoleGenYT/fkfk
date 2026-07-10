# ==================== СЕРВЕРНАЯ ЧАСТЬ ====================
from fastapi import FastAPI, WebSocket
import asyncio
import time
import json
import os
from contextlib import asynccontextmanager

clients = {}          # hwid -> websocket
viewers = {}          # viewer_id -> websocket
client_last_ping = {} # hwid -> timestamp
client_info = {}      # hwid -> bytes (последнее info-сообщение в JSON)
lock = asyncio.Lock()
dead_viewers = set()  # Отложенное удаление вьюверов


async def cleanup_dead_viewers():
    """Удаляет мёртвых вьюверов без блокировок"""
    async with lock:
        for vid in list(dead_viewers):
            viewers.pop(vid, None)
        dead_viewers.clear()


async def safe_send(ws, data):
    """Безопасная отправка без повторного захвата lock"""
    try:
        await ws.send_bytes(data)
        return True
    except:
        return False


async def notify_disconnect(hwid):
    """Безопасно уведомляет всех подключенных вьюверов об отключении клиента"""
    msg = json.dumps({"type": "disconnect", "hwid": hwid}).encode()
    async with lock:
        viewers_list = list(viewers.values())
    tasks = [safe_send(v, msg) for v in viewers_list]
    if tasks:
        await asyncio.gather(*tasks)


async def check_clients():
    while True:
        try:
            now = time.time()
            dead_clients = []
            async with lock:
                dead_clients = [hwid for hwid, last in client_last_ping.items() if now - last > 20]
                for hwid in dead_clients:
                    ws = clients.pop(hwid, None)
                    client_last_ping.pop(hwid, None)
                    client_info.pop(hwid, None)
                    if ws:
                        try:
                            await ws.close()
                        except:
                            pass
            if dead_clients:
                for hwid in dead_clients:
                    await notify_disconnect(hwid)
            await cleanup_dead_viewers()
        except asyncio.CancelledError:
            break
        except:
            pass
        await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Запуск фоновой задачи проверки пингов
    task = asyncio.create_task(check_clients())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def index():
    return {"status": "OK"}


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.get("/test")
async def test():
    return {"msg": "test ok"}


@app.websocket("/ws")
async def ws(websocket: WebSocket):
    await websocket.accept()

    role = websocket.query_params.get("role", "")
    vid = str(id(websocket))

    if role == "client":
        hwid = None
        try:
            while True:
                data = await websocket.receive_bytes()

                # Попытка распарсить JSON (для команд)
                try:
                    msg = json.loads(data.decode())
                    if isinstance(msg, dict):
                        # Обработка ping
                        if msg.get("type") == "ping":
                            await websocket.send_bytes(json.dumps({"type": "pong"}).encode())
                            if hwid:
                                async with lock:
                                    client_last_ping[hwid] = time.time()
                            continue

                        if msg.get("type") == "info":
                            hwid = msg.get("hwid", "")
                            if hwid:
                                async with lock:
                                    clients[hwid] = websocket
                                    client_last_ping[hwid] = time.time()
                                    client_info[hwid] = data
                                    viewers_list = list(viewers.values())

                                # Рассылаем info всем вьюверам
                                tasks = [safe_send(v, data) for v in viewers_list]
                                if tasks:
                                    await asyncio.gather(*tasks)
                                continue
                except:
                    pass

                # Для всех остальных данных (включая бинарные) - рассылаем всем вьюверам
                if hwid:
                    async with lock:
                        client_last_ping[hwid] = time.time()
                        viewers_list = list(viewers.values())

                    tasks = [safe_send(v, data) for v in viewers_list]
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
                await notify_disconnect(hwid)

    elif role == "viewer":
        async with lock:
            viewers[vid] = websocket
            info_list = list(client_info.values())

        # Отправляем существующие info новому вьюверу
        for info_data in info_list:
            try:
                await websocket.send_bytes(info_data)
            except:
                break
        try:
            while True:
                data = await websocket.receive_bytes()
                try:
                    msg = json.loads(data.decode())
                    if msg.get("type") == "get_clients":
                        async with lock:
                            info_list = list(client_info.values())
                        for info_data in info_list:
                            try:
                                await websocket.send_bytes(info_data)
                            except:
                                break
                        continue

                    target_hwid = msg.get("target")
                    if target_hwid:
                        target_ws = None
                        async with lock:
                            target_ws = clients.get(target_hwid)
                        if target_ws:
                            try:
                                del msg["target"]
                                await target_ws.send_bytes(json.dumps(msg).encode())
                            except:
                                async with lock:
                                    clients.pop(target_hwid, None)
                                    client_last_ping.pop(target_hwid, None)
                                    client_info.pop(target_hwid, None)
                                await notify_disconnect(target_hwid)
                except:
                    pass
        except:
            pass
        finally:
            async with lock:
                viewers.pop(vid, None)


# Для локального запуска
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
