from fastapi import FastAPI, WebSocket
import asyncio
import time

app = FastAPI()
client = None
viewer = None
last_ping = 0

async def check_client():
    global client, last_ping, viewer
    while True:
        if client and time.time() - last_ping > 20:
            # Клиент не пинговал 20 секунд - считаем мёртвым
            if viewer:
                try:
                    await viewer.send_bytes(b'{"type":"disconnect"}')
                except:
                    pass
            client = None
        await asyncio.sleep(5)

@app.on_event("startup")
async def startup():
    asyncio.create_task(check_client())

@app.get("/")
def index():
    return {"status": "OK"}

@app.websocket("/ws")
async def ws(websocket: WebSocket):
    global client, viewer, last_ping
    await websocket.accept()
    
    role = websocket.query_params.get("role", "")
    
    if role == "client":
        client = websocket
        last_ping = time.time()
        try:
            while True:
                data = await websocket.receive_bytes()
                if data == b'ping':
                    last_ping = time.time()
                elif viewer:
                    await viewer.send_bytes(data)
        except:
            if viewer:
                try:
                    await viewer.send_bytes(b'{"type":"disconnect"}')
                except:
                    pass
            client = None
            
    elif role == "viewer":
        viewer = websocket
        try:
            while True:
                data = await websocket.receive_bytes()
        except:
            viewer = None
