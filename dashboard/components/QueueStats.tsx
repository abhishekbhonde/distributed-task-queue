"use client";

import { useEffect, useState } from "react";
import { listQueues, QueueInfo } from "../lib/api";

export default function QueueStats() {
  const [queues, setQueues] = useState<QueueInfo[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchStats() {
      try {
        const data = await listQueues();
        setQueues(data);
        setError(null);
      } catch (err: any) {
        setError(err.message || "Failed to fetch queue stats");
      }
    }

    fetchStats();
    const interval = setInterval(fetchStats, 5000);
    return () => clearInterval(interval);
  }, []);

  if (error) {
    return (
      <div style={{ color: "#f87171", padding: "1rem", background: "rgba(239, 68, 68, 0.1)", borderRadius: "8px", border: "1px solid rgba(239, 68, 68, 0.2)", fontSize: "0.875rem", marginBottom: "1.5rem" }}>
        {error}
      </div>
    );
  }

  return (
    <div className="stats-container">
      {queues.length === 0 ? (
        <div className="stat-card glass-panel">
          <span className="stat-label">Queue: default</span>
          <span className="stat-val">0</span>
        </div>
      ) : (
        queues.map((q) => (
          <div key={q.name} className="stat-card glass-panel">
            <span className="stat-label">{q.name} queue depth</span>
            <span className="stat-val">{q.depth}</span>
          </div>
        ))
      )}
    </div>
  );
}
