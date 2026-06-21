import type { ClusterStats } from "../types";

interface Props {
  stats: ClusterStats;
  isRunning: boolean;
  wsConnected: boolean;
}

function Metric({
  label,
  value,
  unit,
  highlight,
}: {
  label: string;
  value: string | number;
  unit?: string;
  highlight?: boolean;
}) {
  return (
    <div className="flex flex-col items-center justify-center px-4 py-2 min-w-[100px]">
      <span className="text-gray-500 text-xs uppercase tracking-widest mb-1">{label}</span>
      <span
        className={`font-mono text-xl font-bold tabular-nums ${
          highlight ? "text-amber-400" : "text-gray-100"
        }`}
      >
        {value}
        {unit && (
          <span className="text-gray-500 text-sm font-normal ml-0.5">{unit}</span>
        )}
      </span>
    </div>
  );
}

export default function StatsPanel({ stats, isRunning, wsConnected }: Props) {
  return (
    <div className="flex items-center justify-between border-t border-gray-800 bg-gray-950/90 backdrop-blur px-2 h-20 shrink-0">
      {/* Left: connection + run indicator */}
      <div className="flex items-center gap-3 pl-2">
        <div className={`w-2 h-2 rounded-full ${wsConnected ? "bg-emerald-400" : "bg-red-500"}`} />
        <span className="text-xs text-gray-500 font-mono">
          {wsConnected ? "CONNECTED" : "OFFLINE"}
        </span>
        {isRunning && (
          <span className="text-xs text-amber-400 font-mono animate-pulse ml-2">● RUNNING</span>
        )}
      </div>

      {/* Metrics */}
      <div className="flex divide-x divide-gray-800">
        <Metric
          label="Throughput"
          value={stats.throughput.toFixed(1)}
          unit="/s"
          highlight={isRunning}
        />
        <Metric
          label="Avg RTT"
          value={stats.avg_rtt_ms < 1 ? stats.avg_rtt_ms.toFixed(3) : stats.avg_rtt_ms.toFixed(1)}
          unit="ms"
        />
        <Metric
          label="Overhead"
          value={stats.overhead_ms.toFixed(2)}
          unit="ms"
        />
        <Metric
          label="Tasks"
          value={stats.total_tasks.toLocaleString()}
        />
        <Metric
          label="Active"
          value={`${stats.active_nodes}/${stats.total_nodes}`}
        />
        <Metric
          label="Errors"
          value={stats.errors}
        />
        <Metric
          label="Elapsed"
          value={stats.elapsed_s.toFixed(0)}
          unit="s"
        />
      </div>

      {/* Right: elapsed ticker */}
      <div className="pr-4 text-right">
        <span className="text-xs text-gray-600 font-mono">DP Dashboard v0.1</span>
      </div>
    </div>
  );
}
