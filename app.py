from fastapi import FastAPI, WebSocket
import asyncio

app = FastAPI()
client = None
viewer = None
client_alive = False

async def check_client():
    global client, client_alive, viewer
    while True:
        if client:
            try:
                await asyncio.wait_for(client.send_bytes(b'ping'), timeout=3)
                client_alive = True
            except:
                if client_alive:
                    client_alive = False
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
    global client, viewer, client_alive
    await websocket.accept()
    
    role = websocket.query_params.get("role", "")
    
    if role == "client":
        client = websocket
        client_alive = True
        try:
            while True:
                data = await websocket.receive_bytes()
                if viewer:
                    await viewer.send_bytes(data)
        except:
            if viewer:
                try:
                    await viewer.send_bytes(b'{"type":"disconnect"}')
                except:
                    pass
            client = None
            client_alive = False
            
    elif role == "viewer":
        viewer = websocket
        try:
            while True:
                data = await websocket.receive_bytes()
        except:
            viewer = None
