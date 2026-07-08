const BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8080";

export interface Job {
  ID: string;
  Type: string;
  Payload: Record<string, any>;
  Status: "pending" | "running" | "succeeded" | "failed" | "dead_letter";
  Priority: number;
  Attempts: number;
  MaxRetries: number;
  CreatedAt: string;
  UpdatedAt: string;
  LastError: string;
}

export interface QueueInfo {
  name: string;
  depth: number;
}

export async function enqueueJob(
  type: string,
  payload: Record<string, any> = {},
  priority: number = 0,
  maxRetries: number = 3
): Promise<{ id: string; status: string }> {
  const res = await fetch(`${BASE_URL}/api/jobs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      type,
      payload,
      priority,
      max_retries: maxRetries,
    }),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "Unknown error" }));
    throw new Error(err.error || `Failed to enqueue job: ${res.statusText}`);
  }
  return res.json();
}

export async function getJob(id: string): Promise<Job> {
  const res = await fetch(`${BASE_URL}/api/jobs/${id}`);
  if (!res.ok) {
    throw new Error(`Failed to fetch job ${id}: ${res.statusText}`);
  }
  return res.json();
}

export async function listQueues(): Promise<QueueInfo[]> {
  const res = await fetch(`${BASE_URL}/api/queues`);
  if (!res.ok) {
    throw new Error(`Failed to list queues: ${res.statusText}`);
  }
  return res.json();
}

export async function retryJob(id: string): Promise<Job> {
  const res = await fetch(`${BASE_URL}/api/jobs/${id}/retry`, {
    method: "POST",
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: "Unknown error" }));
    throw new Error(err.error || `Failed to retry job ${id}: ${res.statusText}`);
  }
  return res.json();
}
