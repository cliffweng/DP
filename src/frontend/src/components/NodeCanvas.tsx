import { useEffect, useRef, useState, useMemo } from "react";
import type { NodeData, NodeState } from "../types";

interface Props {
  nodes: NodeData[];
  isRunning: boolean;
}

interface Pos {
  x: number;
  y: number;
}

function getPositions(count: number, w: number, h: number): Pos[] {
  if (count === 0) return [];
  const cx = w / 2;
  const cy = h / 2;
  const minD = Math.min(w, h);

  if (count <= 10) {
    const r = minD * 0.36;
    return Array.from({ length: count }, (_, i) => {
      const a = (i * 2 * Math.PI) / count - Math.PI / 2;
      return { x: cx + r * Math.cos(a), y: cy + r * Math.sin(a) };
    });
  }
  if (count <= 22) {
    const r1 = minD * 0.20;
    const r2 = minD * 0.38;
    const inner = Math.min(8, Math.floor(count / 2));
    const outer = count - inner;
    const pos: Pos[] = [];
    for (let i = 0; i < inner; i++) {
      const a = (i * 2 * Math.PI) / inner - Math.PI / 2;
      pos.push({ x: cx + r1 * Math.cos(a), y: cy + r1 * Math.sin(a) });
    }
    for (let i = 0; i < outer; i++) {
      const a = (i * 2 * Math.PI) / outer - Math.PI / 2;
      pos.push({ x: cx + r2 * Math.cos(a), y: cy + r2 * Math.sin(a) });
    }
    return pos;
  }
  // Grid fallback for many nodes
  const cols = Math.ceil(Math.sqrt(count * (w / h)));
  const rows = Math.ceil(count / cols);
  const sx = w / (cols + 1);
  const sy = h / (rows + 1);
  return Array.from({ length: count }, (_, i) => ({
    x: sx * ((i % cols) + 1),
    y: sy * (Math.floor(i / cols) + 1),
  }));
}

const NODE_COLORS: Record<NodeState, string> = {
  idle:      "#374151",
  computing: "#f59e0b",
  done:      "#10b981",
  error:     "#ef4444",
};

const NODE_TYPE_ICON: Record<string, string> = {
  simulate:     "SIM",
  bond_price:   "BND",
  short_circuit:"SC",
};

export default function NodeCanvas({ nodes, isRunning }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [dims, setDims] = useState({ w: 800, h: 600 });

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const { width, height } = entries[0].contentRect;
      setDims({ w: width, h: height });
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const positions = useMemo(
    () => getPositions(nodes.length, dims.w, dims.h),
    [nodes.length, dims]
  );

  const cx = dims.w / 2;
  const cy = dims.h / 2;

  return (
    <div ref={containerRef} className="relative w-full h-full overflow-hidden">
      <svg width={dims.w} height={dims.h} className="absolute inset-0">
        <defs>
          <filter id="glow-yellow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="5" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
          <filter id="glow-green" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="4" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
          <filter id="glow-blue" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="6" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
          <radialGradient id="coord-grad" cx="50%" cy="30%" r="70%">
            <stop offset="0%" stopColor="#60a5fa" />
            <stop offset="100%" stopColor="#2563eb" />
          </radialGradient>
        </defs>

        {/* Background grid */}
        <pattern id="grid" width="40" height="40" patternUnits="userSpaceOnUse">
          <path d="M 40 0 L 0 0 0 40" fill="none" stroke="#1f2937" strokeWidth="0.5" />
        </pattern>
        <rect width={dims.w} height={dims.h} fill="url(#grid)" />

        {/* Lines from coordinator to each node */}
        {nodes.map((node, i) => {
          const pos = positions[i];
          if (!pos) return null;
          const computing = node.state === "computing";
          return (
            <line
              key={`line-${node.node_id}`}
              x1={cx} y1={cy}
              x2={pos.x} y2={pos.y}
              stroke={computing ? "#f59e0b" : "#1f2937"}
              strokeWidth={computing ? 1.5 : 1}
              strokeDasharray={computing ? "6 6" : "4 4"}
              opacity={computing ? 0.8 : 0.3}
              className={computing ? "line-flow" : undefined}
            />
          );
        })}

        {/* Nodes */}
        {nodes.map((node, i) => {
          const pos = positions[i];
          if (!pos) return null;
          const state = node.state;
          const color = NODE_COLORS[state];
          const computing = state === "computing";
          const done = state === "done";
          const label = node.node_id.slice(-6);
          const icon = NODE_TYPE_ICON[node.node_type] ?? node.node_type.slice(0, 3).toUpperCase();

          const floatDur = 2.2 + (i * 0.31) % 0.9;
          const floatDelay = (i * 0.45) % 2.0;

          return (
            <g
              key={node.node_id}
              style={{
                animationName: "node-float",
                animationDuration: `${floatDur}s`,
                animationDelay: `${floatDelay}s`,
                animationTimingFunction: "ease-in-out",
                animationIterationCount: "infinite",
                transformOrigin: `${pos.x}px ${pos.y}px`,
              }}
            >
              {/* Pulse rings for computing */}
              {computing && (
                <>
                  <circle
                    cx={pos.x} cy={pos.y} r={22}
                    fill="none" stroke="#f59e0b" strokeWidth={2}
                    className="ring-pulse"
                  />
                  <circle
                    cx={pos.x} cy={pos.y} r={22}
                    fill="none" stroke="#f59e0b" strokeWidth={1.5}
                    className="ring-pulse-delayed"
                  />
                </>
              )}

              {/* Spinning orbit ring for computing */}
              {computing && (
                <circle
                  cx={pos.x} cy={pos.y} r={26}
                  fill="none"
                  stroke="#f59e0b"
                  strokeWidth={1}
                  strokeDasharray="8 16"
                  opacity={0.6}
                  style={{
                    transformOrigin: `${pos.x}px ${pos.y}px`,
                    animation: "spin 2s linear infinite",
                  }}
                />
              )}

              {/* Main circle */}
              <circle
                cx={pos.x} cy={pos.y} r={20}
                fill={color}
                filter={
                  computing ? "url(#glow-yellow)" :
                  done ? "url(#glow-green)" : undefined
                }
                opacity={state === "idle" ? 0.75 : 1}
              />

              {/* Node type label */}
              <text
                x={pos.x} y={pos.y - 2}
                textAnchor="middle"
                fill="white"
                fontSize={8}
                fontWeight="bold"
                fontFamily="monospace"
              >
                {icon}
              </text>

              {/* Short ID */}
              <text
                x={pos.x} y={pos.y + 9}
                textAnchor="middle"
                fill="rgba(255,255,255,0.7)"
                fontSize={7}
                fontFamily="monospace"
              >
                {label}
              </text>

              {/* Task count badge */}
              {node.task_count > 0 && (
                <g>
                  <circle cx={pos.x + 14} cy={pos.y - 14} r={8} fill="#1e293b" stroke="#475569" strokeWidth={1} />
                  <text
                    x={pos.x + 14} y={pos.y - 11}
                    textAnchor="middle"
                    fill="#94a3b8"
                    fontSize={7}
                    fontFamily="monospace"
                  >
                    {node.task_count > 999 ? "999+" : node.task_count}
                  </text>
                </g>
              )}

              {/* RTT below node */}
              {node.last_rtt > 0 && (
                <text
                  x={pos.x} y={pos.y + 34}
                  textAnchor="middle"
                  fill="#6b7280"
                  fontSize={8}
                  fontFamily="monospace"
                >
                  {node.last_rtt.toFixed(1)}ms
                </text>
              )}

              {/* Error indicator */}
              {node.error_count > 0 && (
                <text
                  x={pos.x} y={pos.y + 44}
                  textAnchor="middle"
                  fill="#ef4444"
                  fontSize={7}
                  fontFamily="monospace"
                >
                  ✕{node.error_count}
                </text>
              )}
            </g>
          );
        })}

        {/* Coordinator (center) */}
        {nodes.length > 0 && (
          <g>
            <circle
              cx={cx} cy={cy} r={28}
              fill="none" stroke="#3b82f6" strokeWidth={1}
              strokeDasharray="4 4"
              opacity={0.4}
              style={{
                transformOrigin: `${cx}px ${cy}px`,
                animation: isRunning ? "spin 8s linear infinite reverse" : undefined,
              }}
            />
            <circle
              cx={cx} cy={cy} r={22}
              fill="url(#coord-grad)"
              filter="url(#glow-blue)"
            />
            <text
              x={cx} y={cy - 3}
              textAnchor="middle"
              fill="white"
              fontSize={8}
              fontWeight="bold"
              fontFamily="monospace"
            >
              COORD
            </text>
            <text
              x={cx} y={cy + 8}
              textAnchor="middle"
              fill="rgba(255,255,255,0.6)"
              fontSize={7}
              fontFamily="monospace"
            >
              {nodes.length}N
            </text>
          </g>
        )}

        {/* Empty state */}
        {nodes.length === 0 && (
          <text
            x={cx} y={cy}
            textAnchor="middle"
            fill="#4b5563"
            fontSize={14}
            fontFamily="monospace"
          >
            Spawn nodes to begin
          </text>
        )}
      </svg>
    </div>
  );
}
