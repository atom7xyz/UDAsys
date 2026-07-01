import asyncio
import os

import zenoh
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

import Producer
from bus import BUS

HERE = os.path.dirname(os.path.abspath(__file__))
STATIC = os.path.join(HERE, "static")
CONFIG = os.environ.get("ZENOH_CONFIG", os.path.join(HERE, "config.json"))

app = FastAPI(title="UDAsys webview")

___state = {}


def load_config() -> zenoh.Config:
    return zenoh.Config.from_file(CONFIG) if os.path.exists(CONFIG) else zenoh.Config()


@app.on_event("startup")
def startup():
    producers, receivers = Producer.main(load_config())
    session = zenoh.open(load_config())
    ___state["producers"] = producers
    ___state["receivers"] = receivers
    ___state["session"] = session


@app.on_event("shutdown")
def shutdown():
    session = ___state.get("session")
    if session is not None:
        session.close()


def do_query(selector: str):
    session = ___state["session"]
    replies = []
    for reply in session.get(selector):
        if reply.ok is not None:
            replies.append({"channel": str(reply.ok.key_expr), "payload": reply.ok.payload.to_string()})
        else:
            replies.append({"channel": selector, "payload": "ERR: " + reply.err.payload.to_string()})
    return replies


@app.get("/api/channels")
def channels():
    producers = ___state.get("producers", [])
    return {"channels": sorted(p.channel() for p in producers)}


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC, "index.html"))


@app.websocket("/ws")
async def ws(socket: WebSocket):
    await socket.accept()
    q = BUS.subscribe()
    loop = asyncio.get_event_loop()

    async def pump_events():
        while True:
            event = await loop.run_in_executor(None, q.get)
            await socket.send_json(event)

    async def pump_commands():
        while True:
            text = await socket.receive_text()
            command = text.strip()
            if command.lower().startswith("query "):
                command = command[len("query "):].strip()
            if not command:
                continue
            replies = await loop.run_in_executor(None, do_query, command)
            await socket.send_json({"kind": "query", "channel": command, "replies": replies})

    events_task = asyncio.create_task(pump_events())
    commands_task = asyncio.create_task(pump_commands())
    try:
        await asyncio.gather(events_task, commands_task)
    except WebSocketDisconnect:
        pass
    finally:
        events_task.cancel()
        commands_task.cancel()
        BUS.unsubscribe(q)


app.mount("/static", StaticFiles(directory=STATIC), name="static")
