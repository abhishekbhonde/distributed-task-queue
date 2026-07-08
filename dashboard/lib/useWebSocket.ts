import { useEffect, useState, useRef } from "react";
import { Job } from "./api";

export function useWebSocket() {
  const [jobs, setJobs] = useState<Record<string, Job>>({});
  const [connected, setConnected] = useState(false);
  const reconnectTimeoutRef = useRef<NodeJS.Timeout>();
  const socketRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    let reconnectDelay = 1000;

    function connect() {
      const wsUrl = getWsUrl();
      const ws = new WebSocket(wsUrl);
      socketRef.current = ws;

      ws.onopen = () => {
        setConnected(true);
        reconnectDelay = 1000; // Reset backoff delay
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          if (data.event === "initial_state") {
            const initialJobs: Record<string, Job> = {};
            if (Array.isArray(data.jobs)) {
              data.jobs.forEach((j: Job) => {
                initialJobs[j.ID] = j;
              });
            }
            setJobs(initialJobs);
          } else if (data.event === "job_updated" && data.job) {
            setJobs((prev) => ({
              ...prev,
              [data.job.ID]: data.job,
            }));
          }
        } catch (err) {
          console.error("ws: failed to parse message", err);
        }
      };

      ws.onclose = () => {
        setConnected(false);
        socketRef.current = null;
        reconnectTimeoutRef.current = setTimeout(() => {
          reconnectDelay = Math.min(reconnectDelay * 2, 30000);
          connect();
        }, reconnectDelay);
      };

      ws.onerror = (err) => {
        console.error("ws: error:", err);
        ws.close();
      };
    }

    connect();

    return () => {
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      if (socketRef.current) {
        socketRef.current.onclose = null;
        socketRef.current.close();
      }
    };
  }, []);

  return { jobs, connected };
}

function getWsUrl() {
  // Use browser location fallback if running client-side
  const apiUrl = typeof window !== "undefined"
    ? (process.env.NEXT_PUBLIC_API_URL || window.location.origin)
    : "http://localhost:8080";
  
  // Convert http/https to ws/wss
  return apiUrl.replace(/^http/, "ws") + "/ws";
}
