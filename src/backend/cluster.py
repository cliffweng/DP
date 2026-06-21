"""
ClusterManager – logical node layer over a fixed Dask thread pool.

Visual nodes are pure Python state dicts, decoupled from Dask workers so
you can spawn any count regardless of CPU core limit.

The terminal display uses rich.live.Live – the Rich equivalent of Ink:
a fixed panel that redraws in-place at ~8 Hz. Nothing ever scrolls.
"""
import asyncio
import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional

from fastapi import WebSocket

from .workers import process_task

log = logging.getLogger(__name__)

_SPINNER = "⣾⣽⣻⢿⡿⣟⣯⣷"
_TRAFFIC = ">>>>>>>     "   # scrolls left→right each frame


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
        self._cluster: Any = None
        self._client: Any = None

        self.node_states: Dict[str, dict] = {}
        self._node_tasks: Dict[str, asyncio.Task] = {}
        self._seq = 0

        self.is_running = False
        self.task_type = "simulate"
        self.params: dict = {"max_calc_time": 2.0}

        self.stats = ClusterStats()
        self.clients: List[WebSocket] = []

        self._run_task: Optional[asyncio.Task] = None
        self._bcast_task: Optional[asyncio.Task] = None
        self._console_stop: Optional[threading.Event] = None
        self._console_thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Dask cluster
    # ------------------------------------------------------------------

    async def _ensure_dask(self):
        if self._client is not None:
            return
        import os
        from dask.distributed import LocalCluster, Client

        # Dask's internal loggers ignore silence_logs and write through
        # Python's logging system – silence them all explicitly.
        _DASK_LOGGERS = [
            "distributed", "distributed.worker", "distributed.core",
            "distributed.scheduler", "distributed.nanny", "distributed.client",
            "distributed.protocol", "distributed.batched",
            "tornado", "tornado.application",
            "asyncio",
        ]
        for name in _DASK_LOGGERS:
            logging.getLogger(name).setLevel(logging.ERROR)

        nw = min(os.cpu_count() or 4, 32)
        self._cluster = await asyncio.to_thread(
            lambda: LocalCluster(
                n_workers=nw,
                threads_per_worker=1,
                processes=False,
                dashboard_address=None,
                silence_logs=logging.ERROR,
                memory_limit=0,
            )
        )
        self._client = await asyncio.to_thread(lambda: Client(self._cluster))

    async def _close_dask(self):
        if self._client:
            await asyncio.to_thread(self._client.close)
            self._client = None
        if self._cluster:
            await asyncio.to_thread(self._cluster.close)
            self._cluster = None

    # ------------------------------------------------------------------
    # Visual node lifecycle
    # ------------------------------------------------------------------

    def spawn(self, count: int, node_type: str = "simulate") -> List[str]:
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
    # Run loop
    # ------------------------------------------------------------------

    async def _run_loop(self):
        while self.is_running:
            for nid in list(self.node_states.keys()):
                existing = self._node_tasks.get(nid)
                if existing is None or existing.done():
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
            await asyncio.sleep(0.04)

    async def _handle(self, dask_future, nid: str):
        try:
            result = await asyncio.to_thread(dask_future.result)
            self.stats.record(result)
            if nid not in self.node_states:
                return
            ns = self.node_states[nid]
            ns["task_count"] += 1
            ns["last_rtt"] = round(result.get("rtt", 0.0) * 1000, 2)
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
            await asyncio.sleep(0.1)

    # ------------------------------------------------------------------
    # Rich Live console  (Ink-style fixed-panel terminal display)
    # Runs in its own daemon thread so it never fights the asyncio loop
    # or uvicorn's own stdout/stderr writes.
    # ------------------------------------------------------------------

    def _build_renderable(self, frame: int):
        """Build the Rich renderable for one display frame (called from thread)."""
        from rich.console import Group
        from rich.panel import Panel
        from rich.rule import Rule
        from rich.table import Table
        from rich.text import Text
        from rich import box

        # Snapshot node states (thread-safe read)
        try:
            states = list(self.node_states.values())
        except RuntimeError:
            states = []

        st = self.stats.snapshot(
            sum(1 for s in states if s["state"] == "computing"),
            len(states),
        )

        # ── header ──────────────────────────────────────────────────
        badge = (
            Text("● RUNNING", style="bold yellow")
            if self.is_running
            else Text("○  IDLE", style="dim white")
        )
        header = Text.assemble(
            ("  ", ""),
            badge,
            ("   ", ""),
            (f"{len(states)}", "bold white"), (" nodes", "dim"),
            ("   ", ""),
            (f"{st['throughput']:.1f}", "bold cyan"), ("/s", "dim"),
            ("   avg ", "dim"),
            (f"{st['avg_rtt_ms']:.0f} ms", "white"),
            ("   ", ""),
            (f"{st['total_tasks']:,}", "bold white"), (" tasks", "dim"),
            ("   ", ""),
            (f"{st['elapsed_s']:.0f} s", "dim"),
        )

        # ── node table ──────────────────────────────────────────────
        tbl = Table(
            box=box.SIMPLE, show_header=True,
            header_style="bold dim", pad_edge=False, show_edge=False,
        )
        tbl.add_column("Node",    width=10, style="bold white", no_wrap=True)
        tbl.add_column("Type",    width=4,  style="dim",        no_wrap=True)
        tbl.add_column("Status",  width=15,                     no_wrap=True)
        tbl.add_column("Traffic", width=14,                     no_wrap=True)
        tbl.add_column("Tasks",   width=6,  justify="right")
        tbl.add_column("RTT",     width=8,  justify="right",    style="dim")

        if not states:
            tbl.add_row("", "", Text("No nodes — click Spawn", style="dim italic"), "", "", "")
        else:
            MAX = 28
            for s in states[:MAX]:
                node_st  = s["state"]
                ntype    = s["node_type"][:3].upper()
                tasks    = s["task_count"]
                rtt      = s["last_rtt"]
                errs     = s["error_count"]
                rtt_str  = f"{rtt:.0f} ms" if rtt > 0 else "—"

                if node_st == "computing":
                    spin      = _SPINNER[frame % len(_SPINNER)]
                    off       = frame % len(_TRAFFIC)
                    traf_str  = (_TRAFFIC * 2)[off: off + 13]
                    status_t  = Text(f"{spin} computing",  style="bold yellow")
                    traffic_t = Text(traf_str,             style="yellow")
                elif node_st == "done":
                    status_t  = Text("✓  done",            style="bold green")
                    traffic_t = Text("✓",                  style="green")
                elif node_st == "error":
                    status_t  = Text(f"✗  error ×{errs}",  style="bold red")
                    traffic_t = Text("✗",                  style="red")
                else:
                    status_t  = Text("·  idle",            style="dim")
                    traffic_t = Text("·",                  style="dim")

                tbl.add_row(s["node_id"], ntype, status_t, traffic_t, str(tasks), rtt_str)

            if len(states) > MAX:
                hidden = len(states) - MAX
                busy   = sum(1 for s in states[MAX:] if s["state"] == "computing")
                tbl.add_row("…", "", Text(f"…{hidden} more  ({busy} busy)", style="dim"), "", "", "")

        # ── footer ──────────────────────────────────────────────────
        err_style = "bold red" if st["errors"] else "dim"
        footer = Text.assemble(
            ("  throughput ", "dim"), (f"{st['throughput']:.1f}/s",    "bold white"),
            ("   RTT ",       "dim"), (f"{st['avg_rtt_ms']:.1f} ms",   "white"),
            ("   overhead ",  "dim"), (f"{st['overhead_ms']:.2f} ms",  "white"),
            ("   errors ",    "dim"), (str(st["errors"]),               err_style),
            ("   active ",    "dim"), (f"{st['active_nodes']}/{st['total_nodes']}", "white"),
        )

        return Panel(
            Group(header, Rule(style="dim blue"), tbl, Rule(style="dim blue"), footer),
            title="[bold blue]DP Dashboard[/bold blue]",
            border_style="blue",
            padding=(0, 1),
        )

    # ── thread entry point ───────────────────────────────────────────

    def _console_thread_fn(self, stop: threading.Event):
        """
        Runs in a daemon thread.  rich.live.Live owns the terminal here;
        redirect_stderr=True absorbs uvicorn's log lines and prints them
        ABOVE the panel instead of breaking the display.
        """
        from rich.live import Live
        frame = 0
        try:
            with Live(
                self._build_renderable(0),
                refresh_per_second=8,
                redirect_stderr=True,
            ) as live:
                while not stop.is_set():
                    try:
                        live.update(self._build_renderable(frame))
                    except Exception:
                        pass
                    frame += 1
                    time.sleep(0.12)
        except Exception as exc:
            log.error("Console display error: %s", exc)

    async def start_console_loop(self):
        if self._console_thread and self._console_thread.is_alive():
            return
        self._console_stop   = threading.Event()
        self._console_thread = threading.Thread(
            target=self._console_thread_fn,
            args=(self._console_stop,),
            name="dp-console",
            daemon=True,            # dies automatically when the process exits
        )
        self._console_thread.start()

    async def stop_console(self):
        if self._console_stop:
            self._console_stop.set()
        if self._console_thread:
            await asyncio.to_thread(self._console_thread.join, 2.0)


# Singleton used by main.py
cluster = ClusterManager()
