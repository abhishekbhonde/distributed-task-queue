"use client";

import QueueStats from "../components/QueueStats";
import SubmitJobForm from "../components/SubmitJobForm";
import JobList from "../components/JobList";
import { useWebSocket } from "../lib/useWebSocket";

export default function Home() {
  const { jobs, connected } = useWebSocket();

  return (
    <div className="container">
      <header className="header">
        <h1>Forge Dashboard</h1>
        <div className="connection-status">
          <span 
            className={`dot ${connected ? "connected" : "disconnected"}`} 
            style={{ marginRight: "0.25rem" }}
          />
          {connected ? "Connected to Backend" : "Disconnected / Retrying"}
        </div>
      </header>

      <QueueStats />

      <main className="dashboard-grid">
        <SubmitJobForm />
        <JobList jobs={jobs} />
      </main>
    </div>
  );
}
