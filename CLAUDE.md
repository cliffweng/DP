# Distributed Processing Dashboard

## Stack
- Backend: Python 3.10+, Ray, FastAPI, uvicorn
- Frontend: React 18, TypeScript, Vite, Tailwind CSS v3

## Running (two terminals)
```sh
# Terminal 1 - backend
pip install -r requirements.txt
python -m uvicorn src.backend.main:app --reload --port 8000

# Terminal 2 - frontend
cd src/frontend
npm install
npm run dev
```
Open http://localhost:5173

## Architecture
- Ray actors (`ProcessingNode`) are the nodes visualized on the dashboard
- `ClusterManager` owns all actors, dispatches tasks, tracks stats
- FastAPI broadcasts cluster state at 10 Hz via WebSocket `/ws`
- React App subscribes to WS and renders NodeCanvas + StatsPanel + Controls

## Adding a new task type
1. Add handler in `src/backend/workers.py` inside `process_task()`
2. Add an option to the `mode` select in `src/frontend/src/components/Controls.tsx`
