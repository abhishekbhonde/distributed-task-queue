"use client";

import { useState } from "react";
import { Job, retryJob } from "../lib/api";

interface JobListProps {
  jobs: Record<string, Job>;
}

export default function JobList({ jobs }: JobListProps) {
  const [retryingIds, setRetryingIds] = useState<Record<string, boolean>>({});
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  // Convert map to array, sort by UpdatedAt descending, and limit to last 50
  const jobsList = Object.values(jobs)
    .sort((a, b) => new Date(b.UpdatedAt).getTime() - new Date(a.UpdatedAt).getTime())
    .slice(0, 50);

  const handleRetry = async (id: string) => {
    setRetryingIds((prev) => ({ ...prev, [id]: true }));
    setErrorMsg(null);
    try {
      await retryJob(id);
    } catch (err: any) {
      setErrorMsg(err.message || "Failed to retry job");
    } finally {
      setRetryingIds((prev) => ({ ...prev, [id]: false }));
    }
  };

  return (
    <div className="list-card glass-panel">
      <div className="list-header">
        <h2 className="list-title">Recent Jobs</h2>
      </div>

      {errorMsg && (
        <div style={{ color: "#f87171", padding: "0.75rem 1rem", background: "rgba(239, 68, 68, 0.1)", borderRadius: "8px", border: "1px solid rgba(239, 68, 68, 0.2)", fontSize: "0.875rem", marginBottom: "1.5rem" }}>
          {errorMsg}
        </div>
      )}

      <div className="jobs-table-container">
        {jobsList.length === 0 ? (
          <div className="empty-state">
            <svg style={{ width: "48px", height: "48px", stroke: "#3f3f46" }} fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
            </svg>
            <span>No jobs active in this session.</span>
            <span style={{ fontSize: "0.75rem", color: "#52525b" }}>Use the form on the left to submit a job.</span>
          </div>
        ) : (
          <table className="jobs-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Type</th>
                <th>Status</th>
                <th>Priority</th>
                <th>Attempts</th>
                <th>Updated</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {jobsList.map((job) => {
                const isRetrying = !!retryingIds[job.ID];
                return (
                  <tr key={job.ID}>
                    <td>
                      <span className="job-id-link" title={job.ID}>
                        {job.ID.substring(0, 8)}...
                      </span>
                    </td>
                    <td>{job.Type}</td>
                    <td>
                      <span className={`badge ${job.Status}`}>
                        <span className="indicator-dot" />
                        {job.Status === "dead_letter" ? "dead letter" : job.Status}
                      </span>
                    </td>
                    <td>{job.Priority}</td>
                    <td>{job.Attempts}/{job.MaxRetries}</td>
                    <td>{new Date(job.UpdatedAt).toLocaleTimeString()}</td>
                    <td>
                      {job.Status === "dead_letter" && (
                        <button
                          className="btn-secondary"
                          onClick={() => handleRetry(job.ID)}
                          disabled={isRetrying}
                        >
                          {isRetrying ? "Retrying..." : "Retry"}
                        </button>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
