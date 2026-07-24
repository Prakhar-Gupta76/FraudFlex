import type {
  AlertDetail,
  AlertStatus,
  AlertSummary,
  DashboardSummary,
  ReviewInput,
  RiskCategory,
  TransactionDetail,
  TransactionSummary,
} from "./types";

export const API_BASE =
  import.meta.env.VITE_API_URL?.replace(/\/$/, "") ??
  "http://127.0.0.1:8000";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...init?.headers,
    },
  });
  if (!response.ok) {
    const body = await response.json().catch(() => null);
    throw new Error(body?.detail ?? `Request failed (${response.status})`);
  }
  return response.json() as Promise<T>;
}

export const api = {
  summary: () => request<DashboardSummary>("/dashboard/summary"),
  transactions: (options: {
    search?: string;
    category?: RiskCategory | "";
    customerId?: string;
    limit?: number;
  } = {}) => {
    const query = new URLSearchParams({
      limit: String(options.limit ?? 100),
    });
    if (options.search) query.set("search", options.search);
    if (options.category) query.set("category", options.category);
    if (options.customerId) query.set("customer_id", options.customerId);
    return request<TransactionSummary[]>(`/transactions?${query}`);
  },
  transaction: (id: string) =>
    request<TransactionDetail>(`/transactions/${encodeURIComponent(id)}`),
  alerts: (status?: AlertStatus | "") => {
    const query = new URLSearchParams({ limit: "100" });
    if (status) query.set("status", status);
    return request<AlertSummary[]>(`/alerts?${query}`);
  },
  alert: (id: string) =>
    request<AlertDetail>(`/alerts/${encodeURIComponent(id)}`),
  review: (id: string, input: ReviewInput) =>
    request(`/alerts/${encodeURIComponent(id)}/review`, {
      method: "POST",
      body: JSON.stringify(input),
    }),
};
