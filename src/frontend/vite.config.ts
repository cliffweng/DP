import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      // REST only – WebSocket connects directly to :8000 (see App.tsx)
      "/api": "http://localhost:8000",
    },
  },
});
