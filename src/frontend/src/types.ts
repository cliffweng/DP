export type NodeState = "idle" | "computing" | "done" | "error";

export interface NodeData {
  node_id: string;
  node_type: string;
  state: NodeState;
  task_count: number;
  last_rtt: number;   // milliseconds
  error_count: number;
}

export interface ClusterStats {
  throughput: number;       // tasks/s (5-second rolling window)
  avg_rtt_ms: number;
  overhead_ms: number;
  total_tasks: number;
  errors: number;
  elapsed_s: number;
  active_nodes: number;
  total_nodes: number;
}

export interface ClusterMessage {
  type: "cluster_state";
  nodes: NodeData[];
  stats: ClusterStats;
  is_running: boolean;
}

export type TaskMode = "simulate" | "bond_price" | "short_circuit";
