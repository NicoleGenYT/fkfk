from fastapi import FastAPI, WebSocket

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
                    await viewer.send_bytes(data)
        except:
            client = None
            
    elif role == "viewer":
        viewer = websocket
        try:
            while True:
                data = await websocket.receive_bytes()
                if client:
                    await client.send_bytes(data)
        except:
            viewer = None
