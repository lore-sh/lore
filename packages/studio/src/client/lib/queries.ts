import { queryOptions } from "@tanstack/react-query";
import {
  fetchCommitDetail,
  fetchHistory,
  fetchStatus,
  fetchTableData,
  fetchTableHistory,
  fetchTableSchema,
  fetchTables,
  type HistoryQuery,
  type TableDataQuery,
} from "./api";

export function statusQueryOptions() {
  return queryOptions({
    queryKey: ["status"],
    queryFn: fetchStatus,
    staleTime: 5_000,
  });
}

export function tablesQueryOptions() {
  return queryOptions({
    queryKey: ["tables"],
    queryFn: fetchTables,
    staleTime: 5_000,
  });
}

export function tableDataQueryOptions(name: string, query: TableDataQuery) {
  return queryOptions({
    queryKey: [
      "table-data",
      name,
      query.page,
      query.pageSize,
      query.sortBy ?? "",
      query.sortDir ?? "",
      JSON.stringify(Object.entries(query.filters).sort(([a], [b]) => a.localeCompare(b))),
    ],
    queryFn: () => fetchTableData(name, query),
    staleTime: 2_000,
  });
}

export function tableSchemaQueryOptions(name: string) {
  return queryOptions({
    queryKey: ["table-schema", name],
    queryFn: () => fetchTableSchema(name),
    staleTime: 10_000,
  });
}

export function tableHistoryQueryOptions(name: string, limit: number) {
  return queryOptions({
    queryKey: ["table-history", name, limit],
    queryFn: () => fetchTableHistory(name, limit),
    staleTime: 2_000,
  });
}

export function historyQueryOptions(query: HistoryQuery) {
  return queryOptions({
    queryKey: ["history", query.limit ?? 50, query.page ?? 1, query.kind ?? "all", query.table ?? ""],
    queryFn: () => fetchHistory(query),
    staleTime: 2_000,
  });
}

export function commitDetailQueryOptions(id: string) {
  return queryOptions({
    queryKey: ["history-detail", id],
    queryFn: () => fetchCommitDetail(id),
    staleTime: 30_000,
  });
}
