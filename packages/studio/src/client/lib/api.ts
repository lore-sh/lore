import { hc, type InferRequestType, type InferResponseType } from "hono/client";
import type { StudioApi } from "../../server/app";

const client = hc<StudioApi>("/");

const tablesEndpoint = client.api.tables.$get;
const tableDataEndpoint = client.api.tables[":name"].$get;
const schemaEndpoint = client.api.schema.$get;
const historyEndpoint = client.api.history.$get;
const commitDetailEndpoint = client.api.history[":id"].$get;
const statusEndpoint = client.api.status.$get;

export type TableDataRequest = InferRequestType<typeof tableDataEndpoint>;
export type HistoryRequest = InferRequestType<typeof historyEndpoint>;
export type CommitDetailRequest = InferRequestType<typeof commitDetailEndpoint>;
export type TablesResponse = InferResponseType<typeof tablesEndpoint, 200>;
export type TableDataResponse = InferResponseType<typeof tableDataEndpoint, 200>;
export type SchemaResponse = InferResponseType<typeof schemaEndpoint, 200>;
export type HistoryResponse = InferResponseType<typeof historyEndpoint, 200>;
export type CommitDetailResponse = InferResponseType<typeof commitDetailEndpoint, 200>;
export type StatusResponse = InferResponseType<typeof statusEndpoint, 200>;

async function throwForError(response: { status: number; json: () => Promise<unknown> }): Promise<never> {
  let message = `Request failed with status ${response.status}`;
  let parseError: string | null = null;
  try {
    const payload = await response.json();
    if (
      payload !== null &&
      typeof payload === "object" &&
      "message" in payload &&
      typeof payload.message === "string" &&
      payload.message.length > 0
    ) {
      message = payload.message;
    }
  } catch (error: unknown) {
    parseError = error instanceof Error ? error.message : String(error);
  }
  if (parseError) {
    message = `${message} (failed to parse error body: ${parseError})`;
  }
  throw new Error(message);
}

export async function fetchTables(): Promise<TablesResponse> {
  const response = await tablesEndpoint();
  if (!response.ok) {
    await throwForError(response);
  }
  return await response.json();
}

export async function fetchTableData(
  param: TableDataRequest["param"],
  query: TableDataRequest["query"],
): Promise<TableDataResponse> {
  const response = await tableDataEndpoint({
    param,
    query,
  });
  if (!response.ok) {
    await throwForError(response);
  }
  return await response.json();
}

export async function fetchSchema(): Promise<SchemaResponse> {
  const response = await schemaEndpoint();
  if (!response.ok) {
    await throwForError(response);
  }
  return await response.json();
}

export async function fetchHistory(query: HistoryRequest["query"]): Promise<HistoryResponse> {
  const response = await historyEndpoint({
    query,
  });
  if (!response.ok) {
    await throwForError(response);
  }
  return await response.json();
}

export async function fetchCommitDetail(param: CommitDetailRequest["param"]): Promise<CommitDetailResponse> {
  const response = await commitDetailEndpoint({
    param,
  });
  if (!response.ok) {
    await throwForError(response);
  }
  return await response.json();
}

export async function fetchStatus(): Promise<StatusResponse> {
  const response = await statusEndpoint();
  if (!response.ok) {
    await throwForError(response);
  }
  return await response.json();
}
