"""FastAPI app – REST control plane + WebSocket state stream."""
import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .cluster import cluster

logging.basicConfig(level=logging.WARNING)

# Silence noisy loggers before anything starts.
for _noisy in [
    "uvicorn", "uvicorn.access", "uvicorn.error",
    "distributed", "distributed.worker", "distributed.core",
    "distributed.scheduler", "distributed.nanny", "distributed.client",
    "distributed.protocol", "distributed.batched",
    "tornado", "tornado.application",
    "asyncio", "websockets",
]:
    logging.getLogger(_noisy).setLevel(logging.ERROR)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    asyncio.create_task(cluster.start_broadcast_loop())
    await cluster.start_console_loop()
    yield
    await cluster.stop()
    await cluster.stop_console()
    await cluster._close_dask()


app = FastAPI(title="DP Dashboard", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ------------------------------------------------------------------
# Request / response models
# ------------------------------------------------------------------

class SpawnRequest(BaseModel):
    count: int = 4
    node_type: str = "simulate"

class RunRequest(BaseModel):
    task_type: str = "simulate"
    max_calc_time: float = 2.0
    face_value: float = 1000.0
    coupon_rate: float = 0.05
    ytm: float = 0.05
    periods: int = 10


# ------------------------------------------------------------------
# REST endpoints
# ------------------------------------------------------------------

@app.get("/api/status")
async def status():
    active = sum(1 for s in cluster.node_states.values() if s["state"] == "computing")
    return {
        "total_nodes": len(cluster.node_states),
        "active_nodes": active,
        "is_running": cluster.is_running,
        "task_type": cluster.task_type,
    }


@app.post("/api/spawn")
async def spawn(req: SpawnRequest):
    if not (1 <= req.count <= 64):
        return {"error": "count must be 1–64"}
    ids = cluster.spawn(req.count, req.node_type)   # sync – instant
    await cluster.broadcast()
    return {"spawned": ids, "total_nodes": len(cluster.node_states)}


@app.post("/api/run")
async def run(req: RunRequest):
    params = req.model_dump(exclude={"task_type"})
    await cluster.start(req.task_type, params)
    return {"started": True, "task_type": req.task_type}


@app.post("/api/stop")
async def stop():
    await cluster.stop()
    return {"stopped": True}


@app.post("/api/reset")
async def reset():
    await cluster.clear()       # stops tasks, wipes node_states; keeps Dask cluster
    await cluster.broadcast()
    return {"reset": True}


# ------------------------------------------------------------------
# WebSocket
# ------------------------------------------------------------------

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    cluster.clients.append(ws)
    await cluster.broadcast()
    try:
        while True:
            await ws.receive_text()    # keep-alive; ignore client messages
    except WebSocketDisconnect:
        pass
    finally:
        if ws in cluster.clients:
            cluster.clients.remove(ws)
