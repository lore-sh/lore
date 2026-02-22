import { parsePositiveInt } from "./parse";

export type SortDirection = "asc" | "desc";
export type TableTab = "data" | "schema" | "history";

export interface TableRouteSearch {
  tab: TableTab;
  page: number;
  pageSize: number;
  sortBy?: string | undefined;
  sortDir?: SortDirection | undefined;
  [key: string]: unknown;
}

function normalizeTab(value: unknown): TableTab {
  if (value === "schema") {
    return "schema";
  }
  if (value === "history") {
    return "history";
  }
  return "data";
}

function normalizeSortDir(value: unknown): SortDirection | undefined {
  if (value === "asc" || value === "desc") {
    return value;
  }
  return undefined;
}

export function validateTableSearch(raw: Record<string, unknown>): TableRouteSearch {
  const sortBy = typeof raw.sortBy === "string" && raw.sortBy.trim().length > 0 ? raw.sortBy.trim() : undefined;
  const sortDir = normalizeSortDir(raw.sortDir);
  const search: TableRouteSearch = {
    tab: normalizeTab(raw.tab),
    page: parsePositiveInt(raw.page, 1),
    pageSize: parsePositiveInt(raw.pageSize, 50),
    sortBy,
    sortDir: sortBy ? sortDir ?? "asc" : undefined,
  };

  for (const [key, value] of Object.entries(raw)) {
    if (!key.startsWith("filters.")) {
      continue;
    }
    if (typeof value !== "string") {
      continue;
    }
    const normalized = value.trim();
    if (normalized.length === 0) {
      continue;
    }
    search[key] = normalized;
  }

  return search;
}

export function tableFilterEntries(search: TableRouteSearch): Array<[string, string]> {
  const entries: Array<[string, string]> = [];
  for (const [key, value] of Object.entries(search)) {
    if (!key.startsWith("filters.")) {
      continue;
    }
    if (typeof value !== "string") {
      continue;
    }
    const column = key.slice("filters.".length);
    if (column.length === 0) {
      continue;
    }
    entries.push([column, value]);
  }
  entries.sort(([left], [right]) => left.localeCompare(right));
  return entries;
}

export function tableFilters(search: TableRouteSearch): Record<string, string> {
  return Object.fromEntries(tableFilterEntries(search));
}

export function tableFilterValue(search: TableRouteSearch, column: string): string {
  const key = `filters.${column}`;
  const value = search[key];
  return typeof value === "string" ? value : "";
}

export function updateTableFilter(search: TableRouteSearch, column: string, value: string): TableRouteSearch {
  const key = `filters.${column}`;
  const next: TableRouteSearch = { ...search };
  const normalized = value.trim();
  if (normalized.length === 0) {
    delete next[key];
  } else {
    next[key] = normalized;
  }
  next.page = 1;
  return next;
}

export function nextSortState(search: TableRouteSearch, column: string): {
  sortBy?: string | undefined;
  sortDir?: SortDirection | undefined;
} {
  if (search.sortBy !== column) {
    return { sortBy: column, sortDir: "asc" };
  }
  if (search.sortDir === "asc") {
    return { sortBy: column, sortDir: "desc" };
  }
  return { sortBy: undefined, sortDir: undefined };
}
