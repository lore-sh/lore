import type {
  RevertConflictResult,
  RevertResult,
  StudioCommitDetail,
  StudioHistoryEntry,
  StudioSchemaTable,
  StudioTableDataView,
  StudioTablesView,
  TossStatus,
} from "@toss/core";
import { hc, type InferRequestType, type InferResponseType } from "hono/client";
import type { StudioApi, StudioApiError } from "../../server/app";

const client = hc<StudioApi>("/");

const statusEndpoint = client.api.status.$get;
const tablesEndpoint = client.api.tables.$get;
const tableRowsQueryEndpoint = client.api.tables[":name"].rows.query.$post;
const tableSchemaEndpoint = client.api.tables[":name"].schema.$get;
const tableHistoryEndpoint = client.api.tables[":name"].history.$get;
const commitsEndpoint = client.api.commits.$get;
const commitDetailEndpoint = client.api.commits[":id"].$get;
const revertCommitEndpoint = client.api.commits[":id"].revert.$post;

type TableRowsQueryRequest = InferRequestType<typeof tableRowsQueryEndpoint>;
type CommitsRequest = InferRequestType<typeof commitsEndpoint>;
type RevertConflictPayload = InferResponseType<typeof revertCommitEndpoint, 409>;

type TableRowsQueryJson = TableRowsQueryRequest["json"];
type CommitQueryRaw = NonNullable<CommitsRequest["query"]>;

export type TableDataQuery = Omit<TableRowsQueryJson, "page" | "pageSize" | "filters"> & {
  page: number;
  pageSize: number;
  filters: Record<string, string>;
};

export interface HistoryQuery {
  limit?: number | undefined;
  page?: number | undefined;
  kind?: CommitQueryRaw["kind"];
  table?: string | undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isStudioApiError(value: unknown): value is StudioApiError {
  if (!isRecord(value)) {
    return false;
  }
  return typeof value.code === "string" && typeof value.message === "string";
}

interface StudioProblemPayload {
  type: string;
  title: string;
  status: number;
  detail: string;
  instance: string;
  code: string;
}

function isStudioProblemPayload(value: unknown): value is StudioProblemPayload {
  if (!isRecord(value)) {
    return false;
  }
  return (
    typeof value.type === "string" &&
    typeof value.title === "string" &&
    typeof value.status === "number" &&
    typeof value.detail === "string" &&
    typeof value.instance === "string" &&
    typeof value.code === "string"
  );
}

export function toErrorFromPayload(payload: unknown, status: number): Error {
  if (isStudioApiError(payload)) {
    const error = new Error(payload.message);
    Object.assign(error, {
      code: payload.code,
      details: payload.details,
      status,
    });
    return error;
  }

  if (isStudioProblemPayload(payload)) {
    const error = new Error(payload.detail);
    Object.assign(error, {
      code: payload.code,
      status: payload.status,
      type: payload.type,
      title: payload.title,
      instance: payload.instance,
    });
    return error;
  }

  const error = new Error(`Request failed with status ${status}`);
  Object.assign(error, { status });
  return error;
}

async function throwApiError(response: Response): Promise<never> {
  let payload: unknown;

  try {
    payload = await response.json();
  } catch {
    const error = new Error(`Request failed with status ${response.status}`);
    Object.assign(error, { status: response.status });
    throw error;
  }

  throw toErrorFromPayload(payload, response.status);
}

function isRevertConflictResult(value: unknown): value is RevertConflictResult {
  if (!isRecord(value)) {
    return false;
  }
  if (value.ok !== false) {
    return false;
  }
  return Array.isArray(value.conflicts);
}

export async function fetchStatus(): Promise<TossStatus> {
  const response = await statusEndpoint();
  if (response.status !== 200) {
    await throwApiError(response);
  }
  return (await response.json()) as TossStatus;
}

export async function fetchTables(): Promise<StudioTablesView> {
  const response = await tablesEndpoint();
  if (response.status !== 200) {
    await throwApiError(response);
  }
  return (await response.json()) as StudioTablesView;
}

export async function fetchTableData(name: string, query: TableDataQuery): Promise<StudioTableDataView> {
  const response = await tableRowsQueryEndpoint({
    param: { name },
    json: query,
  });

  if (response.status !== 200) {
    await throwApiError(response);
  }
  return (await response.json()) as StudioTableDataView;
}

export async function fetchTableSchema(name: string): Promise<StudioSchemaTable> {
  const response = await tableSchemaEndpoint({
    param: { name },
  });

  if (response.status !== 200) {
    await throwApiError(response);
  }
  return (await response.json()) as StudioSchemaTable;
}

export async function fetchTableHistory(name: string, limit = 50, page?: number): Promise<StudioHistoryEntry[]> {
  const query: Record<string, string> = {
    limit: String(limit),
  };

  if (typeof page === "number") {
    query.page = String(page);
  }

  const response = await tableHistoryEndpoint({
    param: { name },
    query,
  });

  if (response.status !== 200) {
    await throwApiError(response);
  }
  return (await response.json()) as StudioHistoryEntry[];
}

export async function fetchHistory(query: HistoryQuery): Promise<StudioHistoryEntry[]> {
  const encoded: Record<string, string> = {};

  if (typeof query.limit === "number") {
    encoded.limit = String(query.limit);
  }
  if (typeof query.page === "number") {
    encoded.page = String(query.page);
  }
  if (typeof query.kind === "string") {
    encoded.kind = query.kind;
  }
  if (typeof query.table === "string" && query.table.trim().length > 0) {
    encoded.table = query.table;
  }

  const response = await commitsEndpoint({
    query: encoded,
  });

  if (response.status !== 200) {
    await throwApiError(response);
  }
  return (await response.json()) as StudioHistoryEntry[];
}

export async function fetchCommitDetail(id: string): Promise<StudioCommitDetail> {
  const response = await commitDetailEndpoint({
    param: { id },
  });

  if (response.status !== 200) {
    await throwApiError(response);
  }
  return (await response.json()) as StudioCommitDetail;
}

export async function revertCommitById(id: string): Promise<RevertResult> {
  const response = await revertCommitEndpoint({
    param: { id },
  });

  if (response.status === 409) {
    const payload: RevertConflictPayload = await response.json();
    if (isRevertConflictResult(payload)) {
      return payload;
    }
    throw toErrorFromPayload(payload, 409);
  }

  if (response.status !== 200) {
    await throwApiError(response);
  }

  return (await response.json()) as RevertResult;
}
