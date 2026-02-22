import type {
  RevertResult,
  StudioCommitDetail,
  StudioHistoryEntry,
  StudioSchemaTable,
  StudioTableDataView,
  StudioTablesView,
  TossStatus,
} from "@toss/core";

export interface TableDataQuery {
  page: number;
  pageSize: number;
  sortBy?: string | undefined;
  sortDir?: "asc" | "desc" | undefined;
  filters: Record<string, string>;
}

export interface HistoryQuery {
  limit?: number | undefined;
  page?: number | undefined;
  kind?: "apply" | "revert" | undefined;
  table?: string | undefined;
}

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, init);
  if (!response.ok) {
    let message = `Request failed with status ${response.status}`;
    try {
      const payload = await response.json();
      if (payload !== null && typeof payload === "object" && "message" in payload && typeof payload.message === "string") {
        message = payload.message;
      }
    } catch {
      // ignore parse errors, keep status message
    }
    throw new Error(message);
  }
  return (await response.json()) as T;
}

function toSearchParams(values: Record<string, string | number | undefined>): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) {
    if (value === undefined) {
      continue;
    }
    params.set(key, String(value));
  }
  const encoded = params.toString();
  return encoded.length > 0 ? `?${encoded}` : "";
}

export async function fetchStatus(): Promise<TossStatus> {
  return await fetchJson<TossStatus>("/api/status");
}

export async function fetchTables(): Promise<StudioTablesView> {
  return await fetchJson<StudioTablesView>("/api/tables");
}

export async function fetchTableData(name: string, query: TableDataQuery): Promise<StudioTableDataView> {
  const params = new URLSearchParams();
  params.set("page", String(query.page));
  params.set("pageSize", String(query.pageSize));
  if (query.sortBy) {
    params.set("sortBy", query.sortBy);
    params.set("sortDir", query.sortDir ?? "asc");
  }
  for (const [column, value] of Object.entries(query.filters)) {
    if (value.length === 0) {
      continue;
    }
    params.set(`filters.${column}`, value);
  }
  return await fetchJson<StudioTableDataView>(`/api/tables/${encodeURIComponent(name)}?${params.toString()}`);
}

export async function fetchTableSchema(name: string): Promise<StudioSchemaTable> {
  return await fetchJson<StudioSchemaTable>(`/api/tables/${encodeURIComponent(name)}/schema`);
}

export async function fetchTableHistory(name: string, limit = 50): Promise<StudioHistoryEntry[]> {
  const search = toSearchParams({ limit });
  return await fetchJson<StudioHistoryEntry[]>(`/api/tables/${encodeURIComponent(name)}/history${search}`);
}

export async function fetchHistory(query: HistoryQuery): Promise<StudioHistoryEntry[]> {
  const search = toSearchParams({
    limit: query.limit,
    page: query.page,
    kind: query.kind,
    table: query.table,
  });
  return await fetchJson<StudioHistoryEntry[]>(`/api/history${search}`);
}

export async function fetchCommitDetail(id: string): Promise<StudioCommitDetail> {
  return await fetchJson<StudioCommitDetail>(`/api/history/${encodeURIComponent(id)}`);
}

export async function revertCommitById(id: string): Promise<RevertResult> {
  return await fetchJson<RevertResult>(`/api/revert/${encodeURIComponent(id)}`, {
    method: "POST",
  });
}
