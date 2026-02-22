export type TimelineKind = "all" | "apply" | "revert";

export interface TimelineRouteSearch {
  page: number;
  kind: TimelineKind;
  table?: string | undefined;
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

export function validateTimelineSearch(raw: Record<string, unknown>): TimelineRouteSearch {
  const rawKind = raw.kind;
  const kind: TimelineKind = rawKind === "apply" || rawKind === "revert" ? rawKind : "all";
  const table = typeof raw.table === "string" && raw.table.trim().length > 0 ? raw.table.trim() : undefined;
  return {
    page: parsePositiveInt(raw.page, 1),
    kind,
    table,
  };
}
