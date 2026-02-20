export type SortDirection = "asc" | "desc";

export interface TableRouteSearch {
  page: number;
  pageSize: number;
  sortBy?: string | undefined;
  sortDir: SortDirection;
  filters: Record<string, string>;
}

function parsePositiveInt(value: unknown, fallback: number): number {
  if (typeof value === "number") {
    if (Number.isFinite(value) && value > 0) {
      return Math.floor(value);
    }
    return fallback;
  }
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number.parseInt(value, 10);
    if (Number.isFinite(parsed) && parsed > 0) {
      return parsed;
    }
  }
  return fallback;
}

function parseFilters(input: unknown): Record<string, string> {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return {};
  }
  const normalized: Record<string, string> = {};
  for (const [key, value] of Object.entries(input)) {
    if (typeof value !== "string") {
      continue;
    }
    const name = key.trim();
    const raw = value.trim();
    if (name.length === 0 || raw.length === 0) {
      continue;
    }
    normalized[name] = raw;
  }
  return normalized;
}

export function validateTableSearch(raw: Record<string, unknown>): TableRouteSearch {
  const page = parsePositiveInt(raw.page, 1);
  const pageSize = parsePositiveInt(raw.pageSize, 50);
  const sortBy = typeof raw.sortBy === "string" && raw.sortBy.trim().length > 0 ? raw.sortBy : undefined;
  return {
    page,
    pageSize,
    sortBy,
    sortDir: raw.sortDir === "desc" ? "desc" : "asc",
    filters: parseFilters(raw.filters),
  };
}

export function normalizedFilters(filters: Record<string, string>): Array<[string, string]> {
  return Object.entries(filters).sort(([left], [right]) => left.localeCompare(right));
}

export function encodeFilters(filters: Record<string, string>): string | undefined {
  const entries = normalizedFilters(filters);
  if (entries.length === 0) {
    return undefined;
  }
  return JSON.stringify(Object.fromEntries(entries));
}
