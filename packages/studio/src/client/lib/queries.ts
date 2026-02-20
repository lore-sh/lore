import { queryOptions } from "@tanstack/react-query";
import { fetchCommitDetail, fetchHistory, fetchSchema, fetchStatus, fetchTableData, fetchTables } from "./api";
import { encodeFilters, normalizedFilters, type TableRouteSearch } from "./table-search";

export function tablesQueryOptions() {
  return queryOptions({
    queryKey: ["tables"],
    queryFn: fetchTables,
    staleTime: 5_000,
  });
}

export function statusQueryOptions() {
  return queryOptions({
    queryKey: ["status"],
    queryFn: fetchStatus,
    staleTime: 5_000,
  });
}

export function schemaQueryOptions() {
  return queryOptions({
    queryKey: ["schema"],
    queryFn: fetchSchema,
    staleTime: 5_000,
  });
}

export function tableDataQueryOptions(name: string, search: TableRouteSearch) {
  const filterEntries = normalizedFilters(search.filters);
  const filterKey = JSON.stringify(filterEntries);
  const query = {
    page: search.page,
    pageSize: search.pageSize,
    sortBy: search.sortBy,
    sortDir: search.sortDir,
    filter: encodeFilters(search.filters),
  };
  return queryOptions({
    queryKey: ["table-data", name, search.page, search.pageSize, search.sortBy ?? "", search.sortDir, filterKey],
    queryFn: () => fetchTableData({ name }, query),
    staleTime: 2_000,
  });
}

export function historyQueryOptions(limit: number) {
  return queryOptions({
    queryKey: ["history", limit],
    queryFn: () => fetchHistory({ limit }),
    staleTime: 2_000,
  });
}

export function commitDetailQueryOptions(id: string) {
  return queryOptions({
    queryKey: ["history-detail", id],
    queryFn: () => fetchCommitDetail({ id }),
    staleTime: 30_000,
  });
}
