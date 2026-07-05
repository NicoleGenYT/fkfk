from fastapi import FastAPI, WebSocket
import asyncio

app = FastAPI()
client = None
viewer = None

@app.get("/")
def index():
    return {"status": "OK"}

@app.websocket("/ws")
async def ws(websocket: WebSocket):
    global client, viewer
    await websocket.accept()
    
    role = websocket.query_params.get("role", "")
    
    if role == "client":
        client = websocket
        try:
            while True:
                data = await websocket.receive_bytes()
                if viewer:
                    try:
                        await viewer.send_bytes(data)
                    except:
                        viewer = None
        except:
            client = None
            
    elif role == "viewer":
        viewer = websocket
        try:
            while True:
                data = await websocket.receive_bytes()
                if client:
                    try:
                        await client.send_bytes(data)
                    except:
                        client = None
        except:
            viewer = None
