"use client";

import React, { useState } from "react";
import { enqueueJob } from "../lib/api";

export default function SubmitJobForm() {
  const [type, setType] = useState("send_email");
  const [priority, setPriority] = useState(0);
  const [maxRetries, setMaxRetries] = useState(3);
  const [payloadStr, setPayloadStr] = useState('{\n  "to": "user@example.com"\n}');
  const [loading, setLoading] = useState(false);
  const [successMsg, setSuccessMsg] = useState<string | null>(null);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  // Auto-fill sample payload based on selection
  const handleTypeChange = (newType: string) => {
    setType(newType);
    if (newType === "send_email") {
      setPayloadStr('{\n  "to": "user@example.com"\n}');
    } else if (newType === "resize_image") {
      setPayloadStr('{\n  "image_id": "img_987",\n  "width": 800,\n  "height": 600\n}');
    } else if (newType === "generate_report") {
      setPayloadStr('{\n  "report_id": "rep_123",\n  "format": "pdf"\n}');
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setSuccessMsg(null);
    setErrorMsg(null);

    try {
      let payload = {};
      try {
        payload = JSON.parse(payloadStr);
      } catch (err) {
        throw new Error("Invalid payload JSON. Must be a valid JSON object.");
      }

      const res = await enqueueJob(type, payload, priority, maxRetries);
      setSuccessMsg(`Job enqueued successfully! ID: ${res.id}`);
    } catch (err: any) {
      setErrorMsg(err.message || "Failed to submit job");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="form-card glass-panel">
      <h2 className="form-title">Submit New Job</h2>
      <form onSubmit={handleSubmit}>
        <div className="form-group">
          <label htmlFor="job-type">Job Type</label>
          <select
            id="job-type"
            className="form-select"
            value={type}
            onChange={(e) => handleTypeChange(e.target.value)}
          >
            <option value="send_email">send_email</option>
            <option value="resize_image">resize_image</option>
            <option value="generate_report">generate_report</option>
          </select>
        </div>

        <div className="form-group">
          <label htmlFor="priority">Priority (higher runs first)</label>
          <input
            id="priority"
            type="number"
            className="form-input"
            value={priority}
            onChange={(e) => setPriority(parseInt(e.target.value) || 0)}
          />
        </div>

        <div className="form-group">
          <label htmlFor="max-retries">Max Retries</label>
          <input
            id="max-retries"
            type="number"
            className="form-input"
            value={maxRetries}
            onChange={(e) => setMaxRetries(parseInt(e.target.value) || 0)}
          />
        </div>

        <div className="form-group">
          <label htmlFor="payload">Payload (JSON)</label>
          <textarea
            id="payload"
            className="form-input"
            style={{ minHeight: "100px", fontFamily: "monospace", resize: "vertical", lineHeight: "1.4" }}
            value={payloadStr}
            onChange={(e) => setPayloadStr(e.target.value)}
          />
        </div>

        <button type="submit" className="btn btn-primary" style={{ marginTop: "0.5rem" }} disabled={loading}>
          {loading ? "Submitting..." : "Enqueue Job"}
        </button>

        {successMsg && <div className="alert-success">{successMsg}</div>}
        {errorMsg && (
          <div style={{ color: "#f87171", padding: "0.75rem 1rem", background: "rgba(239, 68, 68, 0.1)", borderRadius: "8px", border: "1px solid rgba(239, 68, 68, 0.2)", fontSize: "0.875rem", marginTop: "1rem", wordBreak: "break-all" }}>
            {errorMsg}
          </div>
        )}
      </form>
    </div>
  );
}
