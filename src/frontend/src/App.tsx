import { useEffect, useRef, useState, useCallback } from "react";
import type { ClusterMessage, ClusterStats, NodeData, TaskMode } from "./types";
import NodeCanvas from "./components/NodeCanvas";
import StatsPanel from "./components/StatsPanel";
import Controls from "./components/Controls";
import type { RunParams } from "./components/Controls";

const EMPTY_STATS: ClusterStats = {
  throughput: 0,
  avg_rtt_ms: 0,
  overhead_ms: 0,
  total_tasks: 0,
  errors: 0,
  elapsed_s: 0,
  active_nodes: 0,
  total_nodes: 0,
};

// In dev the Vite proxy can't reliably tunnel WebSockets (Vite 8 changed WS
// proxy behaviour), so connect directly to the FastAPI backend.
const WS_URL = import.meta.env.DEV
  ? "ws://localhost:8000/ws"
  : `ws://${window.location.host}/ws`;

export default function App() {
  const [nodes, setNodes] = useState<NodeData[]>([]);
  const [stats, setStats] = useState<ClusterStats>(EMPTY_STATS);
  const [isRunning, setIsRunning] = useState(false);
  const [wsConnected, setWsConnected] = useState(false);

  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => setWsConnected(true);

    ws.onmessage = (e) => {
      try {
        const msg: ClusterMessage = JSON.parse(e.data);
        if (msg.type === "cluster_state") {
          setNodes(msg.nodes);
          setStats(msg.stats);
          setIsRunning(msg.is_running);
        }
      } catch {
        // ignore malformed frames
      }
    };

    ws.onclose = () => {
      setWsConnected(false);
      reconnectTimer.current = setTimeout(connect, 2000);
    };

    ws.onerror = () => ws.close();
  }, []);

  useEffect(() => {
    connect();
    return () => {
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
      wsRef.current?.close();
    };
  }, [connect]);

  // ------------------------------------------------------------------
  // API helpers
  // ------------------------------------------------------------------

  const api = async (path: string, body?: object) => {
    try {
      const res = await fetch(`/api${path}`, {
        method: body !== undefined ? "POST" : "GET",
        headers: body !== undefined ? { "Content-Type": "application/json" } : undefined,
        body: body !== undefined ? JSON.stringify(body) : undefined,
      });
      return res.json();
    } catch (err) {
      console.error(`API ${path} failed:`, err);
    }
  };

  const handleSpawn = (count: number, nodeType: string) =>
    api("/spawn", { count, node_type: nodeType });

  const handleRun = (mode: TaskMode, params: RunParams) =>
    api("/run", { task_type: mode, ...params });

  const handleStop = () => api("/stop", {});

  const handleReset = () => api("/reset", {});

  // ------------------------------------------------------------------

  return (
    <div className="flex h-screen bg-gray-950 text-gray-100 overflow-hidden">
      <Controls
        isRunning={isRunning}
        nodeCount={nodes.length}
        onSpawn={handleSpawn}
        onRun={handleRun}
        onStop={handleStop}
        onReset={handleReset}
      />

      <div className="flex flex-col flex-1 min-w-0">
        {/* Top bar */}
        <header className="flex items-center justify-between px-6 py-3 border-b border-gray-800 shrink-0">
          <div className="flex items-center gap-3">
            <span className="text-blue-400 font-mono text-lg font-bold">⬡</span>
            <span className="font-semibold text-gray-200 tracking-wide">Distributed Processing Dashboard</span>
          </div>
          <div className="flex items-center gap-6 text-xs font-mono text-gray-500">
            <span>{nodes.length} nodes</span>
            <span
              className={`font-bold ${
                isRunning ? "text-amber-400 animate-pulse" : "text-gray-600"
              }`}
            >
              {isRunning ? "● RUNNING" : "○ IDLE"}
            </span>
          </div>
        </header>

        {/* Canvas – fills remaining space */}
        <div className="flex-1 min-h-0">
          <NodeCanvas nodes={nodes} isRunning={isRunning} />
        </div>

        <StatsPanel stats={stats} isRunning={isRunning} wsConnected={wsConnected} />
      </div>
    </div>
  );
}
