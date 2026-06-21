import { useState } from "react";
import type { TaskMode } from "../types";

interface Props {
  isRunning: boolean;
  nodeCount: number;
  onSpawn: (count: number, nodeType: string) => void;
  onRun: (mode: TaskMode, params: RunParams) => void;
  onStop: () => void;
  onReset: () => void;
}

export interface RunParams {
  max_calc_time: number;
  face_value: number;
  coupon_rate: number;
  ytm: number;
  periods: number;
}

const MODE_LABELS: Record<TaskMode, string> = {
  simulate: "Simulate",
  bond_price: "Bond Price",
  short_circuit: "Short-Circuit",
};

const MODE_DESC: Record<TaskMode, string> = {
  simulate: "Sleeps for rand(0, T) seconds — pure latency simulation.",
  bond_price: "DCF bond pricer with market-data latency sim.",
  short_circuit: "Returns immediately — measures pure framework overhead.",
};

function Slider({
  label,
  min,
  max,
  step,
  value,
  unit,
  onChange,
}: {
  label: string;
  min: number;
  max: number;
  step: number;
  value: number;
  unit: string;
  onChange: (v: number) => void;
}) {
  return (
    <div className="flex flex-col gap-1">
      <div className="flex justify-between text-xs text-gray-400">
        <span>{label}</span>
        <span className="font-mono text-gray-300">
          {value.toFixed(step < 1 ? 2 : 0)}{unit}
        </span>
      </div>
      <input
        type="range"
        min={min}
        max={max}
        step={step}
        value={value}
        onChange={(e) => onChange(parseFloat(e.target.value))}
        className="w-full accent-amber-400 h-1"
      />
    </div>
  );
}

export default function Controls({ isRunning, nodeCount, onSpawn, onRun, onStop, onReset }: Props) {
  const [spawnCount, setSpawnCount] = useState(4);
  const [mode, setMode] = useState<TaskMode>("simulate");
  const [maxCalcTime, setMaxCalcTime] = useState(2.0);
  const [faceValue, setFaceValue] = useState(1000);
  const [couponRate, setCouponRate] = useState(0.05);
  const [ytm, setYtm] = useState(0.05);
  const [periods, setPeriods] = useState(10);

  const handleRun = () => {
    onRun(mode, {
      max_calc_time: maxCalcTime,
      face_value: faceValue,
      coupon_rate: couponRate,
      ytm: ytm,
      periods: periods,
    });
  };

  return (
    <aside className="w-64 shrink-0 flex flex-col bg-gray-900 border-r border-gray-800 overflow-y-auto">
      {/* Header */}
      <div className="p-4 border-b border-gray-800">
        <div className="flex items-center gap-2">
          <div className="w-2 h-2 rounded-full bg-blue-500" />
          <span className="text-sm font-bold tracking-wider text-gray-200">DP DASHBOARD</span>
        </div>
        <p className="text-xs text-gray-500 mt-1">Ray · FastAPI · React</p>
      </div>

      <div className="flex flex-col gap-5 p-4 flex-1">

        {/* ── Spawn ── */}
        <section>
          <h2 className="text-xs uppercase tracking-widest text-gray-500 mb-3">Nodes</h2>

          <div className="flex items-center gap-2 mb-3">
            <button
              onClick={() => setSpawnCount(Math.max(1, spawnCount - 1))}
              className="w-7 h-7 rounded bg-gray-800 text-gray-300 hover:bg-gray-700 font-mono text-lg leading-none flex items-center justify-center"
            >−</button>
            <input
              type="number"
              min={1}
              max={32}
              value={spawnCount}
              onChange={(e) => setSpawnCount(Math.max(1, Math.min(32, parseInt(e.target.value) || 1)))}
              className="w-14 text-center bg-gray-800 border border-gray-700 rounded text-gray-200 font-mono text-sm h-7"
            />
            <button
              onClick={() => setSpawnCount(Math.min(32, spawnCount + 1))}
              className="w-7 h-7 rounded bg-gray-800 text-gray-300 hover:bg-gray-700 font-mono text-lg leading-none flex items-center justify-center"
            >+</button>
          </div>

          <button
            onClick={() => onSpawn(spawnCount, mode === "short_circuit" ? "short_circuit" : mode === "bond_price" ? "bond_price" : "simulate")}
            disabled={isRunning}
            className="w-full py-2 rounded bg-blue-700 hover:bg-blue-600 disabled:opacity-40 disabled:cursor-not-allowed text-white text-sm font-semibold transition-colors"
          >
            Spawn {spawnCount} Node{spawnCount !== 1 ? "s" : ""}
          </button>

          {nodeCount > 0 && (
            <p className="text-center text-xs text-gray-500 mt-2">{nodeCount} node{nodeCount !== 1 ? "s" : ""} online</p>
          )}
        </section>

        {/* ── Mode ── */}
        <section>
          <h2 className="text-xs uppercase tracking-widest text-gray-500 mb-3">Task Mode</h2>
          <div className="flex flex-col gap-1.5">
            {(["simulate", "bond_price", "short_circuit"] as TaskMode[]).map((m) => (
              <label
                key={m}
                className={`flex items-center gap-2.5 p-2.5 rounded cursor-pointer border transition-colors ${
                  mode === m
                    ? "border-amber-500 bg-amber-500/10 text-amber-300"
                    : "border-gray-700 bg-gray-800/50 text-gray-400 hover:border-gray-600"
                }`}
              >
                <input
                  type="radio"
                  name="mode"
                  value={m}
                  checked={mode === m}
                  onChange={() => setMode(m)}
                  className="accent-amber-400"
                />
                <span className="text-sm font-medium">{MODE_LABELS[m]}</span>
              </label>
            ))}
          </div>
          <p className="text-xs text-gray-600 mt-2 leading-relaxed">{MODE_DESC[mode]}</p>
        </section>

        {/* ── Task params ── */}
        {mode !== "short_circuit" && (
          <section>
            <h2 className="text-xs uppercase tracking-widest text-gray-500 mb-3">Parameters</h2>
            <div className="flex flex-col gap-3">
              <Slider
                label="Max Calc Time"
                min={0.05}
                max={10}
                step={0.05}
                value={maxCalcTime}
                unit="s"
                onChange={setMaxCalcTime}
              />
              {mode === "bond_price" && (
                <>
                  <Slider label="Face Value" min={100} max={10000} step={100} value={faceValue} unit="$" onChange={setFaceValue} />
                  <Slider label="Coupon Rate" min={0.01} max={0.15} step={0.005} value={couponRate} unit="%" onChange={(v) => setCouponRate(v)} />
                  <Slider label="YTM" min={0.01} max={0.15} step={0.005} value={ytm} unit="%" onChange={setYtm} />
                  <Slider label="Periods" min={1} max={30} step={1} value={periods} unit="yr" onChange={setPeriods} />
                </>
              )}
            </div>
          </section>
        )}

      </div>

      {/* ── Action buttons ── */}
      <div className="p-4 border-t border-gray-800 flex flex-col gap-2">
        {!isRunning ? (
          <button
            onClick={handleRun}
            disabled={nodeCount === 0}
            className="w-full py-3 rounded-lg bg-emerald-600 hover:bg-emerald-500 disabled:opacity-40 disabled:cursor-not-allowed text-white font-bold text-sm tracking-widest transition-colors"
          >
            ▶ RUN
          </button>
        ) : (
          <button
            onClick={onStop}
            className="w-full py-3 rounded-lg bg-red-700 hover:bg-red-600 text-white font-bold text-sm tracking-widest transition-colors animate-pulse"
          >
            ■ STOP
          </button>
        )}
        <button
          onClick={onReset}
          disabled={isRunning}
          className="w-full py-2 rounded-lg bg-gray-800 hover:bg-gray-700 disabled:opacity-40 disabled:cursor-not-allowed text-gray-400 text-xs font-semibold tracking-wider transition-colors"
        >
          RESET CLUSTER
        </button>
      </div>
    </aside>
  );
}
