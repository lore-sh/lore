import type {
  Commit,
  describeDb,
  EncodedRow,
  Operation,
  history,
  queryTable,
  revert,
  status,
  tableOverview,
} from "@toss/core";
import { hc, type InferRequestType, type InferResponseType } from "hono/client";
import type { StudioApi, StudioApiError } from "../../server/app";
import type { StudioRow } from "../../server/studio-row";

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
type CommitSummary = ReturnType<typeof history>[number];
type RevertResult = ReturnType<typeof revert>;
type RevertConflicts = Extract<RevertResult, { ok: false }>;
type SchemaTable = ReturnType<typeof describeDb>["tables"][number];
type Status = ReturnType<typeof status>;
type TableOverview = ReturnType<typeof tableOverview>[number];
type TablePage = ReturnType<typeof queryTable>;

type TableRowsQueryJson = TableRowsQueryRequest["json"];
type CommitQueryRaw = NonNullable<CommitsRequest["query"]>;

export type TableData = Omit<TablePage, "rows"> & {
  rows: StudioRow[];
};

export interface TablesPayload {
  dbPath: string;
  generatedAt: string;
  tables: TableOverview[];
}

export interface CommitDetailPayload {
  commit: Commit;
  operations: Operation[];
  effects: {
    rows: Array<{
      tableName: string;
      pk: Record<string, string>;
      opKind: "insert" | "update" | "delete";
      beforeRow: EncodedRow | null;
      afterRow: EncodedRow | null;
      beforeHash: string | null;
      afterHash: string | null;
    }>;
    schemas: Array<{
      tableName: string;
      beforeTable: unknown;
      afterTable: unknown;
    }>;
  };
}

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

function isRevertConflicts(value: unknown): value is RevertConflicts {
  if (!isRecord(value)) {
    return false;
  }
  if (value.ok !== false) {
    return false;
  }
  return Array.isArray(value.conflicts);
}

export async function fetchStatus(): Promise<Status> {
  const response = await statusEndpoint();
  if (response.status !== 200) {
    await throwApiError(response);
  }
  return (await response.json()) as Status;
}

export async function fetchTables(): Promise<TablesPayload> {
  const response = await tablesEndpoint();
  if (response.status !== 200) {
    await throwApiError(response);
  }
  return (await response.json()) as TablesPayload;
}

export async function fetchTableData(name: string, query: TableDataQuery): Promise<TableData> {
  const response = await tableRowsQueryEndpoint({
    param: { name },
    json: query,
  });

  if (response.status !== 200) {
    await throwApiError(response);
  }
  return (await response.json()) as TableData;
}

export async function fetchTableSchema(name: string): Promise<SchemaTable> {
  const response = await tableSchemaEndpoint({
    param: { name },
  });

  if (response.status !== 200) {
    await throwApiError(response);
  }
  return (await response.json()) as SchemaTable;
}

export async function fetchTableHistory(name: string, limit = 50, page?: number): Promise<CommitSummary[]> {
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
  return (await response.json()) as CommitSummary[];
}

export async function fetchHistory(query: HistoryQuery): Promise<CommitSummary[]> {
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
  return (await response.json()) as CommitSummary[];
}

export async function fetchCommitDetail(id: string): Promise<CommitDetailPayload> {
  const response = await commitDetailEndpoint({
    param: { id },
  });

  if (response.status !== 200) {
    await throwApiError(response);
  }
  return (await response.json()) as CommitDetailPayload;
}

export async function revertCommitById(id: string): Promise<RevertResult> {
  const response = await revertCommitEndpoint({
    param: { id },
  });

  if (response.status === 409) {
    const payload: RevertConflictPayload = await response.json();
    if (isRevertConflicts(payload)) {
      return payload;
    }
    throw toErrorFromPayload(payload, 409);
  }

  if (response.status !== 200) {
    await throwApiError(response);
  }

  return (await response.json()) as RevertResult;
}
