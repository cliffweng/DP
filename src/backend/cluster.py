"""
ClusterManager – logical node layer over a fixed Dask thread pool.

Visual nodes (what the UI shows) are pure Python state dicts.  They are
decoupled from Dask workers: spawning / resetting nodes never touches the
Dask cluster, so you can have any number of nodes regardless of CPU count.

The Dask cluster is a fixed-size thread pool created once on the first Run.
Each visual node gets its own asyncio Task that submits Dask futures and
awaits their results, keeping the node busy continuously while running.
"""
import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import WebSocket

from .workers import process_task

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Rolling stats
# ---------------------------------------------------------------------------

class ClusterStats:
    def __init__(self):
        self._start: Optional[float] = None
        self._task_ts: List[float] = []
        self._total = 0
        self._errors = 0
        self._rtt_sum = 0.0

    def reset(self):
        self.__init__()

    def start(self):
        self._start = time.perf_counter()

    def record(self, result: dict):
        now = time.perf_counter()
        if result.get("success"):
            self._total += 1
            self._rtt_sum += result.get("rtt", 0.0)
            self._task_ts.append(now)
        else:
            self._errors += 1
        cutoff = now - 5.0
        self._task_ts = [t for t in self._task_ts if t > cutoff]

    def snapshot(self, active_nodes: int, total_nodes: int) -> dict:
        now = time.perf_counter()
        recent = [t for t in self._task_ts if t > now - 5.0]
        throughput = len(recent) / 5.0
        avg_rtt_ms = (self._rtt_sum / self._total * 1000) if self._total else 0.0
        elapsed = (now - self._start) if self._start else 0.0
        overhead_ms = max(0.0, avg_rtt_ms * 0.02)
        return {
            "throughput": round(throughput, 2),
            "avg_rtt_ms": round(avg_rtt_ms, 2),
            "overhead_ms": round(overhead_ms, 3),
            "total_tasks": self._total,
            "errors": self._errors,
            "elapsed_s": round(elapsed, 1),
            "active_nodes": active_nodes,
            "total_nodes": total_nodes,
        }


# ---------------------------------------------------------------------------
# Cluster manager
# ---------------------------------------------------------------------------

class ClusterManager:
    def __init__(self):
        # Dask backing resources (created once on first Run)
        self._cluster: Any = None
        self._client: Any = None

        # Visual node registry – independent of Dask worker count
        self.node_states: Dict[str, dict] = {}       # nid → state dict
        self._node_tasks: Dict[str, asyncio.Task] = {}  # nid → asyncio Task
        self._seq = 0

        self.is_running = False
        self.task_type = "simulate"
        self.params: dict = {"max_calc_time": 2.0}

        self.stats = ClusterStats()
        self.clients: List[WebSocket] = []

        self._run_task: Optional[asyncio.Task] = None
        self._bcast_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Dask cluster – internal, created once
    # ------------------------------------------------------------------

    async def _ensure_dask(self):
        """Start the Dask thread pool if not already running."""
        if self._client is not None:
            return
        import os
        from dask.distributed import LocalCluster, Client

        nw = min(os.cpu_count() or 4, 32)
        log.info("Starting Dask LocalCluster with %d thread workers…", nw)
        self._cluster = await asyncio.to_thread(
            lambda: LocalCluster(
                n_workers=nw,
                threads_per_worker=1,
                processes=False,        # threads avoid Windows spawn/pickle issues
                dashboard_address=None,
                silence_logs=logging.WARNING,
                memory_limit=0,         # no per-worker memory cap
            )
        )
        self._client = await asyncio.to_thread(
            lambda: Client(self._cluster)
        )
        log.info("Dask cluster ready (%d workers)", nw)

    async def _close_dask(self):
        if self._client:
            await asyncio.to_thread(self._client.close)
            self._client = None
        if self._cluster:
            await asyncio.to_thread(self._cluster.close)
            self._cluster = None

    # ------------------------------------------------------------------
    # Visual node lifecycle  (sync – instant, no cluster interaction)
    # ------------------------------------------------------------------

    def spawn(self, count: int, node_type: str = "simulate") -> List[str]:
        """Add `count` logical nodes. Returns their IDs immediately."""
        new_ids = []
        for _ in range(count):
            self._seq += 1
            nid = f"node-{self._seq:03d}"
            self.node_states[nid] = {
                "node_id": nid,
                "node_type": node_type,
                "state": "idle",
                "task_count": 0,
                "last_rtt": 0.0,
                "error_count": 0,
            }
            new_ids.append(nid)
        return new_ids

    async def clear(self):
        """Remove all visual nodes. Dask cluster keeps running."""
        await self.stop()
        self.node_states.clear()
        self._node_tasks.clear()
        self._seq = 0
        self.stats.reset()

    # ------------------------------------------------------------------
    # Run / Stop
    # ------------------------------------------------------------------

    async def start(self, task_type: str, params: dict):
        if self.is_running or not self.node_states:
            return
        await self._ensure_dask()
        self.task_type = task_type
        self.params = params
        self.is_running = True
        self.stats.reset()
        self.stats.start()
        self._run_task = asyncio.create_task(self._run_loop())

    async def stop(self):
        self.is_running = False
        if self._run_task:
            self._run_task.cancel()
            try:
                await self._run_task
            except asyncio.CancelledError:
                pass
            self._run_task = None

        for t in self._node_tasks.values():
            t.cancel()
        if self._node_tasks:
            await asyncio.gather(*self._node_tasks.values(), return_exceptions=True)
        self._node_tasks.clear()

        for state in self.node_states.values():
            state["state"] = "idle"

        await self.broadcast()

    # ------------------------------------------------------------------
    # Run loop – one asyncio Task per visual node
    # ------------------------------------------------------------------

    async def _run_loop(self):
        """Keep every visual node continuously fed with Dask futures."""
        while self.is_running:
            for nid in list(self.node_states.keys()):
                existing = self._node_tasks.get(nid)
                if existing is None or existing.done():
                    # client.submit() is non-blocking; Dask load-balances
                    # across its fixed thread pool automatically.
                    dask_future = self._client.submit(
                        process_task,
                        self.task_type,
                        self.params.copy(),
                        pure=False,
                    )
                    self._node_tasks[nid] = asyncio.create_task(
                        self._handle(dask_future, nid)
                    )
                    self.node_states[nid]["state"] = "computing"
            await asyncio.sleep(0.04)   # 25 Hz dispatch poll

    async def _handle(self, dask_future, nid: str):
        """Await one Dask future and write the result back to node state."""
        try:
            result = await asyncio.to_thread(dask_future.result)
            self.stats.record(result)
            if nid not in self.node_states:
                return
            ns = self.node_states[nid]
            ns["task_count"] += 1
            ns["last_rtt"] = round(result.get("rtt", 0.0) * 1000, 2)   # ms
            ns["state"] = "done" if result.get("success") else "error"
            if not result.get("success"):
                ns["error_count"] += 1
        except asyncio.CancelledError:
            if nid in self.node_states:
                self.node_states[nid]["state"] = "idle"
        except Exception as exc:
            log.warning("Task error on %s: %s", nid, exc)
            if nid in self.node_states:
                self.node_states[nid]["state"] = "error"
                self.node_states[nid]["error_count"] += 1

    # ------------------------------------------------------------------
    # WebSocket broadcast
    # ------------------------------------------------------------------

    def _make_payload(self) -> str:
        active = sum(1 for s in self.node_states.values() if s["state"] == "computing")
        return json.dumps({
            "type": "cluster_state",
            "nodes": list(self.node_states.values()),
            "stats": self.stats.snapshot(active, len(self.node_states)),
            "is_running": self.is_running,
        })

    async def broadcast(self):
        if not self.clients:
            return
        data = self._make_payload()
        dead = []
        for ws in self.clients:
            try:
                await ws.send_text(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.remove(ws)

    async def start_broadcast_loop(self):
        if self._bcast_task and not self._bcast_task.done():
            return
        self._bcast_task = asyncio.create_task(self._bcast_loop())

    async def _bcast_loop(self):
        while True:
            await self.broadcast()
            await asyncio.sleep(0.1)    # 10 Hz


# Singleton used by main.py
cluster = ClusterManager()
